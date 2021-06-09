package pgchainbs

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/go-bs-postgres-chainnotated/lib/cidkeyedlru"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/xerrors"
)

// NewPgBlockstore initializes a new PostgreSQL-backed blockstore and ensures
// the database is in the proper deployed-state and is properly locked, all
// in line with the provided PgBlockstoreConfig. See the documentation of this
// struct for more info.
func NewPgBlockstore(ctx context.Context, cfg PgBlockstoreConfig) (chainAnnotatedPgBlockstore *PgBlockstore, err error) {

	shutdownChan := make(chan struct{})
	dbbs := &PgBlockstore{
		prefetchDagLayersOnDbRead: cfg.PrefetchDagLayersOnDbRead,
		preloadRecents:            cfg.CachePreloadRecentBlocks,
		cacheInactiveBeforeRead:   cfg.CacheInactiveBeforeRead,
		isWritable:                cfg.StoreIsWritable,
		parseBlockLinks:           !cfg.DisableBlocklinkParsing,
		lruSizeBytes:              DefaultLruCacheSize,
		firstReadPerformed:        new(int32),
		lastFlushEpoch:            new(int64),
		linearSyncEventCount:      new(int64),
		shutdownSemaphore:         shutdownChan,
		accessLogsRecent:          make(map[int64]struct{}, 16384),
		limiterSetLastAccess:      make(chan struct{}, 1),
		limiterLogsDetailedWrite:  make(chan struct{}, concurrentDetailedLogWriters),
		limiterBlockProcessing:    make(chan struct{}, concurrentBlockProcessors),
	}

	if cfg.InstanceNamespace != "" {
		if !validNamespace.MatchString(cfg.InstanceNamespace) {
			return nil, fmt.Errorf("provided instance-specific chainsync namespace '%s' does not match %s", cfg.InstanceNamespace, validNamespace.String())
		}
		dbbs.instanceNamespace = "fil_instance_" + cfg.InstanceNamespace // everything is hard-coded to a `fil_` prefix
	}

	if cfg.ExtraPreloadNamespace != "" {
		if !validNamespace.MatchString(cfg.ExtraPreloadNamespace) {
			return nil, fmt.Errorf("provided instance-specific extra namespace '%s' does not match %s", cfg.ExtraPreloadNamespace, validNamespace.String())
		}
		dbbs.additionalPreloadNamespace = "fil_instance_" + cfg.ExtraPreloadNamespace
	}

	if cfg.CacheSizeGiB != 0 {
		if cfg.CacheSizeGiB > 256 {
			return nil, fmt.Errorf("requested cache size of %dGiB impossibly large", cfg.CacheSizeGiB)
		}
		dbbs.lruSizeBytes = int64(cfg.CacheSizeGiB << 30)
	}

	var dbConnCfg *pgxpool.Config
	dbConnCfg, err = pgxpool.ParseConfig(cfg.PgxConnectString)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse connection string '%s': %w", cfg.PgxConnectString, err)
	}

	// If one needs to watch the cleartext in tcpdump or somesuch
	// dbConnCfg.ConnConfig.TLSConfig = nil

	if !dbbs.isWritable {
		dbConnCfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "TRUE"
	} else if dbbs.instanceNamespace != "" {
		// when we are writable and will be locking - ensure there are always a few live connections
		dbConnCfg.MinConns = minPgxPoolSize
		if dbConnCfg.MaxConns < minPgxPoolSize {
			dbConnCfg.MaxConns = minPgxPoolSize
		}
	}

	if dbConnCfg.MaxConns > MaxPgxPoolSize {
		dbConnCfg.MaxConns = MaxPgxPoolSize
	}

	var dbPool *pgxpool.Pool
	dbPool, err = pgxpool.ConnectConfig(ctx, dbConnCfg)
	defer func() {
		if err != nil && dbPool != nil {
			dbPool.Close()
		}
	}()
	if err != nil {
		return nil, xerrors.Errorf("failed to connect to '%s': %w", cfg.PgxConnectString, err)
	}
	dbbs.dbPool = dbPool

	var currentPgVersion int32
	err = dbPool.QueryRow(ctx, `SELECT CURRENT_SETTING('server_version_num')::INTEGER`).Scan(&currentPgVersion)
	if err != nil {
		return nil, xerrors.Errorf("retrieving server version failed: %w", err)
	}
	if currentPgVersion < minimumPgVersion {
		return nil, fmt.Errorf(
			"code was tested on PostgreSQL version %d only: change the source if you want to try running on the older version %d",
			minimumPgVersion, currentPgVersion,
		)
	}

	dbSettingsExpected := "Encoding:SQL_ASCII Collate:C Ctype:C"
	var dbSettings string
	if err := dbPool.QueryRow(
		ctx,
		`
		SELECT 'Encoding:' || pg_encoding_to_char(encoding) || ' Collate:' || datcollate || ' Ctype:' || datctype
			FROM pg_database
		WHERE datname = current_database()
		`,
	).Scan(&dbSettings); err != nil {
		return nil, err
	}
	if dbSettings != dbSettingsExpected {
		return nil, fmt.Errorf(
			"unexpected database settings: you must create your database with something like `%s` for reliable and performant binary storage\n Current settings: %s\nExpected settings: %s",
			`CREATE DATABASE {{name}} ENCODING='SQL_ASCII' LC_COLLATE='C' LC_CTYPE='C' TEMPLATE='template0'`,
			dbSettings, dbSettingsExpected,
		)
	}

	// Check if write perms are missing due to external factors ( e.g. replica)
	// and force RO which in turn shuts off various parts of the access pipeline
	if dbbs.isWritable {
		conn, err := dbPool.Acquire(ctx)
		defer func() {
			if conn != nil {
				conn.Release()
			}
		}()
		if err != nil {
			return nil, xerrors.Errorf("writability check connection acquire failed: %w", err)
		}

		if _, tempCreateErr := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TEMPORARY TABLE %s ( pk INTEGER ) ON COMMIT DROP",
			"tmptable_"+randBytesAsHex(),
		)); tempCreateErr != nil {

			// We might have errored for a lot of reasons: double check we are still alive
			// There doesn't seem to be a decent way to enumerate all potential reasons for a failure
			if err = conn.QueryRow(ctx, `SELECT CURRENT_SETTING('server_version_num')::INTEGER`).Scan(new(int32)); err != nil {
				return nil, xerrors.Errorf("connection entered unexpected faulty state during writability check: %w", err)
			}

			log.Warnf("temporary table creation failed even though StoreIsWritable was set: forcing blockstore back to ReadOnly mode: %s", tempCreateErr)
			dbbs.isWritable = false
		}

		conn.Release()
		conn = nil // disarm defer
	}

	dbbs.lru, err = cidkeyedlru.NewCidKeyedLruCache(dbbs.lruSizeBytes)
	if err != nil {
		return nil, xerrors.Errorf("failed to instantiate cache: %w", err)
	}

	if cfg.LogDetailedAccess {
		if dbbs.instanceNamespace == "" {
			return nil, xerrors.New("detailed access logging not possible without an instance-specific chainsync namespace")
		}
		if !dbbs.isWritable {
			return nil, xerrors.New("detailed access logging not possible on a read-only store")
		}
		dbbs.accessLogsDetailed = make(map[accessUnit]int64, 65535)
	}

	neededDDL, err := dbbs.neededDeployDDL(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to determine current deployed schema status: %w", err)
	}

	if neededDDL != nil {

		if !dbbs.isWritable {
			if dbbs.instanceNamespace != "" {
				return nil, fmt.Errorf(
					"unable to continue: connecting in ReadOnly mode requires that the view %s.current_head and all associates dependencies have been already deployed",
					dbbs.instanceNamespace,
				)
			}
			return nil, xerrors.New(
				"unable to continue: connecting in ReadOnly mode requires that the fil_common_base schema has been already deployed",
			)
		}

		if !cfg.AutoUpdateSchema {
			return nil, xerrors.New(
				"unable to continue: currently-deployed schema does not (partially) match the expected state, and AutoUpdateSchema is not enabled",
			)
		}

		if err = dbbs.deploy(ctx, neededDDL); err != nil {
			return nil, err
		}
	}

	// not RO and everything is deployed: let's make sure we can:
	// - EX-lock the namespace or quit
	// - then auto-SH-lock it going forward
	if dbbs.isWritable && dbbs.instanceNamespace != "" {

		tableToAdvisoryLock := dbbs.instanceNamespace + `.datablocks_recent`

		var exLockSuccess bool
		err = dbbs.dbPool.QueryRow(
			ctx,
			`SELECT PG_TRY_ADVISORY_LOCK( $1, TO_REGCLASS( $2 )::INTEGER )`,
			PgLockOidVector,
			tableToAdvisoryLock,
		).Scan(&exLockSuccess)
		if err != nil {
			return nil, xerrors.Errorf(
				"error while attempting exclusive lock over OID of %s: %w",
				tableToAdvisoryLock,
				err,
			)
		}
		if !exLockSuccess {
			return nil, xerrors.Errorf(
				"unable to continue: another connection pool is already holding shared locks over OID of %s",
				tableToAdvisoryLock,
			)
		}

		// every new connection will hold a shared lock, preventing future exclusive ones
		dbConnCfg.AfterConnect = func(ctx context.Context, db *pgx.Conn) error {
			_, err := db.Exec(
				ctx,
				`SELECT PG_ADVISORY_LOCK_SHARED( $1, TO_REGCLASS( $2 )::INTEGER )`,
				PgLockOidVector,
				tableToAdvisoryLock,
			)
			return err
		}

		// we need a new connection pool with the config adjusted, will block waiting for SHlock
		finalPoolReady := make(chan struct{})
		go func() {
			// manually force minconn fresh connections ( healthcheck won't kick in for a while )
			// https://github.com/jackc/pgx/issues/969
			var conns [minPgxPoolSize]*pgxpool.Conn

			defer func() {
				for _, c := range conns {
					if c != nil {
						c.Release()
					}
				}
				close(finalPoolReady)
			}()

			dbbs.dbPool, err = pgxpool.ConnectConfig(ctx, dbConnCfg)
			if err != nil {
				return
			}

			for i := int32(0); i < minPgxPoolSize; i++ {
				conns[i], err = dbbs.dbPool.Acquire(ctx)
				if err != nil {
					return
				}
			}

		}()

		// release the initial (old) EXlock
		dbPool.Close()
		<-finalPoolReady

		dbPool = dbbs.dbPool // a defer captures dbPool higher up
		if err != nil {
			return nil, err
		}
	}

	if cfg.LogCacheStatsOnUSR1 {
		sigChUSR1 := make(chan os.Signal, 1)
		go func(l cidkeyedlru.CidKeyedLRU) {
			// infloop until shutdown
			for {
				select {
				case <-shutdownChan:
					return
				case <-sigChUSR1:
					// l.(*cidkeyedlru.CKLru).Reallocate() // FIXME: TEMPORARY, remove when sizing is fixed
					log.Info(l.StatString())
				}
				// drain accumulated stragglers
				for len(sigChUSR1) > 0 {
					<-sigChUSR1
				}
			}
		}(dbbs.lru)
		signal.Notify(sigChUSR1, syscall.SIGUSR1)
	}

	return dbbs, nil
}
