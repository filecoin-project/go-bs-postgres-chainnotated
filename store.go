package pgchainbs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/go-bs-postgres-chainnotated/lib/cidkeyedlru"
	ipfsblock "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/xerrors"
)

// PgBlockstoreConfig is the struct the user passes to NewPgBlockstore. It is
// not referenced after the initialization.
type PgBlockstoreConfig struct {
	PgxConnectString        string // as understood by pgx, e.g. postgres:///{{dbname}}?host=/var/run/postgresql&user={{uname}}&password={{pass}}
	InstanceNamespace       string // if provided use the given namespace to store recent-access and chain-tracking information
	ExtraPreloadNamespace   string // if provided pre-warm the cache from the recently-accessed blocks listed in our and this 'extra' namespace
	CacheSizeGiB            uint64 // optional non-default value for the approximate maximum size of the LRU write-through cache
	StoreIsWritable         bool   // by default stores are opened in Read-Only mode, set to true to be able to write as well
	CacheInactiveBeforeRead bool   // start with the LRU cache deactivated until the first read takes place: useful for initial bulk loading
	DisableBlocklinkParsing bool   // disable parsing and recording of DAG relations between individual blocks as soon as they are added to the store
	LogDetailedAccess       bool   // if true log individual block Reads and Writes with millisecond precision ( requires InstanceNamespace )
	AutoUpdateSchema        bool   // deploy needed schema changes if any ( noop unless StoreIsWritable )
	LogCacheStatsOnUSR1     bool   // install a USR1 trigger to INFO-log LRU cache stats
}

// PgBlockstore implements github.com/ipfs/go-ipfs-blockstore in a PostgreSQL
// RDBMS coupled with a write-through RAM cache and a number of augmentations
// making it more suitable as a massive-scale blockstore ( e.g. for something
// like the Filecoin blockchain ).
//
// Notable differences from a standard blockstore are:
//  - Everything is keyed by complete CIDs instead of multihash
//  - Block-data is transparently stored zstd-compressed where sensible
//  - There is ability to efficiently record and query DAG-forming block relationships directly in the database
//  - If configured with an instance namespace, keeps a log of recently-accessed blocks, which is then used to bulk-load blocks into the LRU cache on cold-starts
//  - Has a mode tracking every Read/Write with millisecond precision
type PgBlockstore struct {
	isWritable                 bool
	cacheInactiveBeforeRead    bool
	parseBlockLinks            bool
	lruSizeBytes               int64
	instanceNamespace          string
	additionalPreloadNamespace string
	lru                        cidkeyedlru.CidKeyedLRU
	dbPool                     *pgxpool.Pool
	accessLogsMu               sync.Mutex
	accessLogsRecent           map[int64]struct{}
	accessLogsDetailed         map[accessUnit]int64
	limiterLogsDetailedWrite   chan struct{}
	limiterSetLastAccess       chan struct{}
	limiterBlockProcessing     chan struct{}
	shutdownSemaphore          chan struct{}

	// all manipulated concurrently via atomic.Store/Load
	firstReadPerformed   *int32
	lastFlushEpoch       *int64
	linearSyncEventCount *int64
}

// Close releases the database connection and purges the cache. The store can
// not be used again after it has been closed.
func (dbbs *PgBlockstore) Close() error {
	if dbbs == nil {
		return nil
	}
	log.Debugf("shut down of %T(%p) begin", dbbs, dbbs)
	close(dbbs.shutdownSemaphore)
	dbbs.dbPool.Close()
	dbbs.lru.Purge()
	log.Debugf("shut down of %T(%p) complete", dbbs, dbbs)
	return nil
}

// PgxPool returns a reference to the underlying pgx.Pool
func (dbbs *PgBlockstore) PgxPool() *pgxpool.Pool { return dbbs.dbPool }

// InstanceNamespace returns the current configuration,
// "" means no namespace has been set.
func (dbbs *PgBlockstore) InstanceNamespace() string { return dbbs.instanceNamespace }

// IsWritable indicates that the store was configured to be Writable
// and we are connected to a RDBMS in a way permitting writes.
func (dbbs *PgBlockstore) IsWritable() bool { return dbbs.isWritable }

// maybeLogUnexpectedErrorf is a helper to log internal unexpected storage
// errors before sending them up the chain where possible (e.g. the Block
// interface has no provisions for error reporting at all)
func (dbbs *PgBlockstore) maybeLogUnexpectedErrorf(format string, a ...interface{}) {
	select {
	case <-dbbs.shutdownSemaphore:
		// if we are shutting down - nothing unexpected about it
	default:
		log.Errorf("UNEXPECTED: "+format, a...)
	}
}

//
// Start of blockstore.Blockstore implementation
//

// DeleteBlock is NOT IMPLEMENTED by this store - the storage is strictly
// append-only. Always returns an error upon invocation.
func (NIM *PgBlockstore) DeleteBlock(cid.Cid) (err error) {
	err = errors.New("DeleteBlock is not implemented by the annotated blockstore")
	log.Error(err)
	return
}

// HashOnRead is a noop - data is always re-hashed on retrieval
func (dbbs *PgBlockstore) HashOnRead(bool) {}

// Put inserts the given block into the blockstore. When possible batch your
// blocks in a larger transaction via PutMany(), which this function proxies
// to behind the scenes.
func (dbbs *PgBlockstore) Put(b ipfsblock.Block) error { return dbbs.PutMany([]ipfsblock.Block{b}) }

// PutMany inserts the given set of blocks into the blockstore and into the
// write-through LRU memory cache. The entire operation happens within a
// transaction, and either completes in its entirely or not at all.
func (dbbs *PgBlockstore) PutMany(bls []ipfsblock.Block) error { return dbbs.dbStore(bls) }

// Has checks the LRU cache, populates it from the RDBMS if needed, and
// returns a boolean indicating whether the Cid in question was found.
// The work done in this function is identical to GetStoredBlock().
func (dbbs *PgBlockstore) Has(c cid.Cid) (found bool, err error) {
	var sb *StoredBlock
	sb, err = dbbs.dbGet(c, HAS)
	if sb != nil && err == nil {
		found = true
	}
	return
}

// GetSize checks the LRU cache, populates it from the RDBMS if needed, and
// returns the size of the content of this given block.
// The work done in this function is identical to GetStoredBlock().
func (dbbs *PgBlockstore) GetSize(c cid.Cid) (int, error) {
	sb, err := dbbs.dbGet(c, SIZE)

	switch {

	case err != nil:
		return -1, err

	case sb == nil:
		return -1, blockstore.ErrNotFound

	default:
		return int(sb.size), nil
	}
}

// Get is a thin casting-wrapper around GetStoredBlock, allowing *PgBlockstore
// to satisfy the IPFS Blockstore interface.
func (dbbs *PgBlockstore) Get(c cid.Cid) (ipfsblock.Block, error) { return dbbs.GetStoredBlock(c) }

// GetStoredBlock checks the LRU cache, populates it from the RDBMS if needed, and
// returns the block in question if found.
func (dbbs *PgBlockstore) GetStoredBlock(c cid.Cid) (*StoredBlock, error) {

	sb, err := dbbs.dbGet(c, GET)

	switch {

	case err != nil:
		return nil, err

	case sb == nil:
		return nil, blockstore.ErrNotFound

	default:
		// trigger a possible error condition
		if _, err = sb.inflatedContent(false); err != nil {
			return nil, err
		}
		return sb, nil
	}
}

// View is supposed to be a slightly more performant way to access the content
// of a block by avoiding copies in exchange for the user pledging not to
// retain the returned slice. Implemented identically to Get() behind the
// scenes.
func (dbbs *PgBlockstore) View(c cid.Cid, cb func([]byte) error) error {
	sb, err := dbbs.dbGet(c, GET)

	switch {

	case err != nil:
		return err

	case sb == nil:
		return blockstore.ErrNotFound

	default:
		data, err := sb.inflatedContent(false)
		if err != nil {
			return err
		}
		return cb(data)
	}
}

// AllKeysChan returns a channel which will in turn provide every CID currently
// in the RDBMS. Do not use this method under normal circumstances - every call
// would create an MVCC snapshot on the database-side and potentially consume
// enormous amount of resources, until either the channel is exhausted, or the
// initially provided context is cancelled.
func (dbbs *PgBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {

	tx, err := dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	defer func() {
		if tx != nil {
			// no checks
			defer tx.Rollback(context.Background())
		}
	}()
	if err != nil {
		return nil, xerrors.Errorf("transaction start failed: %w", err)
	}

	cursorName := `cursor_` + randBytesAsHex()

	// https://www.postgresql.org/docs/12/sql-declare.html
	_, err = tx.Exec(
		ctx,
		fmt.Sprintf(
			`
			DECLARE %s INSENSITIVE BINARY NO SCROLL CURSOR WITHOUT HOLD
				FOR
			SELECT cid
				FROM fil_common_base.datablocks
			WHERE size IS NOT NULL
			`,
			cursorName,
		),
	)
	if err != nil {
		return nil, xerrors.Errorf("cursor declaration failed: %w", err)
	}

	akc := make(chan cid.Cid, BulkFetchSize*3)

	go dbbs.allKeysFetchWorker(ctx, tx, cursorName, akc)
	tx = nil // disarms the defer, the cursor-declare transaction will be rolled back by the worker

	return akc, nil
}