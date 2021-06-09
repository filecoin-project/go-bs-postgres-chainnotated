package pgchainbs

import (
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-bs-postgres-chainnotated/lib/synccid"
	"github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-multihash"

	"github.com/dustin/go-humanize"
	"github.com/jackc/pgx/v4"
	"golang.org/x/xerrors"
)

// Background initial state prefetch
// Panic()s on early errors
func (dbbs *PgBlockstore) prefetchRecentToCache() {

	if dbbs.instanceNamespace == "" {
		return
	}

	log.Info("kicking off background priming of the write-through LRU cache")

	t0 := time.Now()
	ctx := context.Background()

	preloadFrom := fmt.Sprintf(
		`
			SELECT block_ordinal FROM %s.datablocks_recent
		UNION ALL
			SELECT block_ordinal FROM fil_common_base.datablocks
			WHERE cid IN ( SELECT stateroot_cid FROM fil_common_base.states )
		UNION ALL
			SELECT block_ordinal FROM fil_common_base.datablocks
			WHERE cid IN ( SELECT chainblock_cid FROM fil_common_base.chainblocks )
		`,
		dbbs.instanceNamespace,
	)
	if dbbs.additionalPreloadNamespace != "" {
		preloadFrom += fmt.Sprintf(`
			UNION ALL
				SELECT block_ordinal FROM %s.datablocks_recent
			`,
			dbbs.additionalPreloadNamespace,
		)
	}

	blockOrdinalsQ, err := dbbs.dbPool.Query(ctx, preloadFrom)
	defer func() {
		if blockOrdinalsQ != nil {
			blockOrdinalsQ.Close()
		}
	}()
	if err != nil {
		log.Panicf("error assembling initial priming numlist: %s", err)
	}

	ordinalsUnique := make(map[int64]struct{}, 8<<20)
	var ord int64
	for blockOrdinalsQ.Next() {
		if err := blockOrdinalsQ.Scan(&ord); err != nil {
			log.Panicf("error assembling initial priming numlist: %s", err)
		}
		ordinalsUnique[ord] = struct{}{}
	}

	if err = blockOrdinalsQ.Err(); err != nil {
		log.Panicf("error assembling initial priming numlist: %s", err)
	}

	blockOrdinalsQ.Close()
	blockOrdinalsQ = nil // disarm defer above

	if len(ordinalsUnique) == 0 {
		log.Info("background priming aborted: no recent blockrecords found")
		return
	}

	// We got our initial humongous list to retrieve - no panics from here on out, just log.Error's
	log.Infof(
		"starting %d cache-priming background threads retrieving %s recently-accessed blocks: initial listing took %.03fs",
		backgroundFetchWorkers,
		humanize.Comma(int64(len(ordinalsUnique))),
		float64(time.Since(t0).Milliseconds())/1000,
	)

	ordinalList := make([]int64, 0, len(ordinalsUnique))
	for o := range ordinalsUnique {
		ordinalList = append(ordinalList, o)
	}
	ordinalsUnique = nil // release early/explicitly

	// might as well sort (descending) - potential sequential page reads
	sort.Slice(ordinalList, func(i, j int) bool {
		return ordinalList[j] < ordinalList[i]
	})

	t0 = time.Now()
	totalCount, totalBytes, err := dbbs.PrecacheByOrdinal(ctx, ordinalList)

	tookSecs := float64(time.Since(t0).Milliseconds()) / 1000

	if err != nil {
		log.Warnf(
			"priming of the write-through LRU cache on startup ABORTED after %s blocks totalling %s bytes after %.03fs at %.02fMiB/s: %s",
			humanize.Comma(totalCount), humanize.Comma(totalBytes),
			tookSecs, float64(totalBytes/(1<<20))/tookSecs,
			err,
		)
	} else {
		log.Infof(
			"successfully primed the write-through LRU cache on startup with %s recently-accessed blocks totalling %s bytes: took %.03fs at %.02fMiB/s",
			humanize.Comma(totalCount), humanize.Comma(totalBytes),
			tookSecs, float64(totalBytes/(1<<20))/tookSecs,
		)
	}
}

// PrecacheByOrdinal primes the cache with a given (potentially very large)
// list of block ordinals. At scale (millions of blocks) it is not practical to
// read blocks sequentially, both due to performance and also due to the RDBMS
// tending to MVCC-lock the entire set until we go through it. Instead this
// method breaks down the list into batches and performs parallel relatively
// short reads until everything is retrieved.
func (dbbs *PgBlockstore) PrecacheByOrdinal(ctx context.Context, ordinals []int64) (totalCount, totalBytes int64, err error) {

	var wg sync.WaitGroup
	totCount := new(int64)
	totBytes := new(int64)

	limiter := make(chan struct{}, backgroundFetchWorkers)
	errs := make(chan error, backgroundFetchWorkers)
	for len(ordinals) > 0 && len(errs) == 0 {

		// we have exceeded our cache range - no point continuing
		if atomic.LoadInt64(totBytes) > dbbs.lruSizeBytes {
			break
		}

		limiter <- struct{}{}
		wg.Add(1)

		batchLen := len(ordinals)
		if batchLen > BulkFetchSliceSize {
			batchLen = BulkFetchSliceSize
		}

		batch := ordinals[len(ordinals)-batchLen:]
		ordinals = ordinals[:len(ordinals)-batchLen]

		go func() {
			defer func() {
				<-limiter
				wg.Done()
			}()

			rows, err := dbbs.dbPool.Query(
				ctx,
				fmt.Sprintf(
					`
					SELECT %s
						FROM fil_common_base.datablocks
					WHERE
						block_ordinal = ANY( $1::BIGINT[] )
							AND
						content IS NOT NULL
					`,
					StoredBlocksInflatorSelection,
				),
				batch,
			)

			if err != nil {
				if rows != nil {
					rows.Close()
				}
				errs <- err
				return
			}

			sbs, bytes, err := dbbs.InflateAndCloseDbRows(rows, false)
			if err != nil {
				errs <- err
				return
			}

			atomic.AddInt64(totCount, int64(len(sbs)))
			atomic.AddInt64(totBytes, bytes)
		}()
	}

	wg.Wait()

	if len(errs) > 0 {
		err = <-errs
	} else if *totBytes > dbbs.lruSizeBytes {
		err = fmt.Errorf(
			"aborting precaching due to overflow: retrieved cost %s exceeds the configured max-cache-cost %s",
			humanize.Comma(*totBytes), humanize.Comma(dbbs.lruSizeBytes),
		)
	}

	return *totCount, *totBytes, err
}

// This is the background-worker implementation of AllKeysChan
// No ability to return errors, due to crappy interface, so just log them
// Closes the supplied tx on exit
func (dbbs *PgBlockstore) allKeysFetchWorker(ctx context.Context, cursorTxToBeClosed pgx.Tx, cursorName string, outChan chan<- cid.Cid) {

	// captured by the defer() below: DO NOT ... err :=  ...
	var err error
	var cidRows pgx.Rows
	var c cid.Cid
	cidList := make([]cid.Cid, 0, BulkFetchSliceSize)

	defer func() {
		if err != nil {
			dbbs.maybeLogUnexpectedErrorf("while reading from temporary cursor %s: %s", cursorName, err)
		}

		// there is no error signaling mechanism - just close the channel when "done"
		close(outChan)

		if cidRows != nil {
			cidRows.Close()
		}
		if err = cursorTxToBeClosed.Rollback(context.Background()); err != nil {
			dbbs.maybeLogUnexpectedErrorf("rolling back temporary cursor %s: %s", cursorName, err)
		}
	}()

	for {

		select {
		case <-ctx.Done():
			return
		default:
			// just continue
		}

		cidRows, err = cursorTxToBeClosed.Query(ctx, fmt.Sprintf(
			`FETCH FORWARD %d FROM %s`,
			BulkFetchSliceSize,
			cursorName,
		))
		if err != nil {
			return
		}

		cidList = cidList[:0]
		for cidRows.Next() {
			c, err = cid.Cast(cidRows.RawValues()[0])
			if err != nil {
				return
			}
			cidList = append(cidList, c)
		}
		if err = cidRows.Err(); err != nil {
			return
		}

		// if we did not retrieve anything this round - we are done with the cursor
		if len(cidList) == 0 {
			return
		}

		for i := range cidList {
			// check for shutdown on every iteration, otherwise we might block on the sink forever
			select {
			case <-ctx.Done():
				return
			case outChan <- cidList[i]:
				// keep on trucking
			}
		}
	}
}

// BackfillBlockLinks populates (or updates) the `linked_ordinals` for each
// supplied StoredBlock. One uses this method when DisableBlocklinkParsing has
// been enabled previously for some reason.
func (dbbs *PgBlockstore) BackfillBlockLinks(ctx context.Context, storedBlocks []*StoredBlock) (err error) {
	if len(storedBlocks) == 0 {
		return
	}

	if !dbbs.isWritable {
		return xerrors.New("unable to BackfillBlockLinks() on a read-only store")
	}

	var allReferencedCids synccid.Set

	// asynchronously extract links from each individual block
	var wgParse sync.WaitGroup
	blocksToUpdate := make(map[int64]*sbUnit, len(storedBlocks))
	for _, sb := range storedBlocks {

		sbu := &sbUnit{sb: sb}
		blocksToUpdate[*sb.dbBlockOrdinal] = sbu

		wgParse.Add(1)
		dbbs.limiterBlockProcessing <- struct{}{}

		go func() {
			sbu.linkedCids, sbu.processingError = sbu.sb.extractLinks()
			allReferencedCids.Add(sbu.linkedCids...)

			<-dbbs.limiterBlockProcessing
			wgParse.Done()
		}()
	}

	wgParse.Wait()

	// check nothing errorred before performing expensive ordinal mapping
	for _, sbu := range blocksToUpdate {
		if sbu.processingError != nil {
			return sbu.processingError
		}
	}

	// nil means separate transaction: we want to keep the lock for as little as possible
	ordinalsDict, err := dbbs.getCidOrdinals(ctx, nil, &allReferencedCids)
	if err != nil {
		return err
	}

	tx, err := dbbs.dbPool.Begin(ctx)
	defer func() {
		if tx != nil {
			if err == nil {
				err = tx.Commit(ctx)
			}
			if err != nil {
				dbbs.maybeLogUnexpectedErrorf("error-induced rollback during link backfill: %s", err)
				tx.Rollback(ctx) // no error checks
			}
		}
	}()
	if err != nil {
		return err
	}

	linklessOrdinals := make([]int64, 0, len(blocksToUpdate))
	batch := new(pgx.Batch)
	batch.Queue(ObjectExLockStatement, "fil_common_base.datablocks")
	for _, sbu := range blocksToUpdate {

		// insert all no-link entries in one go at the end
		if len(sbu.linkedCids) == 0 {
			linklessOrdinals = append(linklessOrdinals, *sbu.sb.dbBlockOrdinal)
			continue
		}

		links := make([]int64, len(sbu.linkedCids))
		for i := range links {
			links[i] = ordinalsDict[sbu.linkedCids[i].KeyString()]
		}
		batch.Queue(
			`
			UPDATE fil_common_base.datablocks
				SET linked_ordinals = $1::BIGINT[]
			WHERE block_ordinal = $2
			`,
			links,
			*sbu.sb.dbBlockOrdinal,
		)
	}

	if len(linklessOrdinals) > 0 {
		batch.Queue(
			`
			UPDATE fil_common_base.datablocks
				SET linked_ordinals = ARRAY[]::BIGINT[], subdag_maxdepth = 1
			WHERE block_ordinal = ANY( $1::BIGINT[] )
			`,
			linklessOrdinals,
		)
	}

	if err = tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}

	return nil
}

func (dbbs *PgBlockstore) getCidOrdinals(ctx context.Context, tx pgx.Tx, origCidList *synccid.Set) (_ map[string]int64, err error) {

	// no transaction - make our own
	if tx == nil {
		tx, err = dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		defer func() {
			if tx != nil {
				if err == nil {
					err = tx.Commit(ctx)
				}
				if err != nil {
					dbbs.maybeLogUnexpectedErrorf("error-induced rollback during CID placeholders insertion: %s", err)
					tx.Rollback(ctx) // no error checks
				}
			}
		}()
		if err != nil {
			return nil, xerrors.Errorf("failed to start transaction: %w", err)
		}
	}

	// copy so we can truncate the list as we go
	cidList := origCidList.Clone()

	pgBatch := new(pgx.Batch)
	pgBatch.Queue(ObjectExLockStatement, "fil_common_base.datablocks")

	ordinalsDict := make(map[string]int64, cidList.Len())

	var identityCidBytes [][]byte
	// run through everything in cidList and remove identity and/or cached CIDs
	for _, c := range cidList.Keys() {

		// maybe we already know the ordinal
		if cached, found := dbbs.lru.Peek(c); found {
			sb := cached.(*StoredBlock)
			if sb.dbBlockOrdinal == nil {
				return nil, fmt.Errorf("unexpected nil-ordinal in LRU cached entry %s (0x%X)", c.String(), c.KeyString())
			}

			ordinalsDict[c.KeyString()] = *sb.dbBlockOrdinal
			cidList.Remove(c)
			continue
		}

		// treat identity-hashes separately - they have a special contentless-yet-sized constraint in-db
		p := c.Prefix()
		if p.MhType == multihash.IDENTITY {

			var maxDepth *int32
			var linked []*int64
			if p.Codec == cid.Raw {
				v := int32(1)
				maxDepth = &v
				linked = emptyLinkage
			}

			pgBatch.Queue(
				`
				INSERT INTO fil_common_base.datablocks( size, subdag_maxdepth, cid, linked_ordinals )
					VALUES( $1, $2, $3::BYTEA, $4::BIGINT[] )
				ON CONFLICT ( cid ) DO NOTHING
				`,
				p.MhLength,
				maxDepth,
				c.Bytes(),
				linked,
			)

			identityCidBytes = append(identityCidBytes, c.Bytes())
			cidList.Remove(c)
		}
	}

	nonIdentityCidBytes := cidList.SortedRawKeys()
	pgBatch.Queue(
		`
		INSERT INTO fil_common_base.datablocks ( cid )
			SELECT UNNEST( $1::BYTEA[] )
		ON CONFLICT ( cid ) DO NOTHING
		`,
		nonIdentityCidBytes,
	)

	if err = tx.SendBatch(ctx, pgBatch).Close(); err != nil {
		return nil, err
	}

	// FIXME BUG: https://github.com/jackc/pgx/issues/938#issue-806472537
	// Make the below part of the batch in the future
	// // all but the last one are mere Exec()s
	// for i := 1; i < pgBatch.Len(); i++ {
	// 	if _, err = batchRes.Exec(); err != nil {
	// 		return err
	// 	}
	// }

	// now ask back for everything
	ordinalMapRows, err := tx.Query(
		ctx,
		`
		SELECT cid, block_ordinal
			FROM fil_common_base.datablocks
		WHERE cid = ANY( $1::BYTEA[] ) OR cid = ANY( $2::BYTEA[] )
		`,
		nonIdentityCidBytes, identityCidBytes,
	)
	defer func() {
		if ordinalMapRows != nil {
			ordinalMapRows.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	for ordinalMapRows.Next() {
		var cidBytes []byte
		var ordinal int64
		if err = ordinalMapRows.Scan(&cidBytes, &ordinal); err != nil {
			return nil, err
		}
		ordinalsDict[string(cidBytes)] = ordinal
		cidList.RemoveKeyString(string(cidBytes))
	}
	if err = ordinalMapRows.Err(); err != nil {
		return nil, err
	}

	if len(ordinalsDict) != origCidList.Len() {

		cidList = origCidList.Clone()
		for k := range ordinalsDict {
			cidList.RemoveKeyString(k)
		}
		cidList.ForEach(func(c cid.Cid) error {
			log.Errorf("missing ordinal for %s (0x%X)", c.String(), c.KeyString())
			return nil
		})
		return nil, fmt.Errorf(
			"impossibly(?) ended up with %d ordinals vs the expected %d",
			len(ordinalsDict), origCidList.Len(),
		)
	}

	return ordinalsDict, nil
}

// populate through a temptable to allow ON CONFLICT ... DO ... to work
// not implemented as a dbbs.function() to discourage any state seeping in here
func dbBulkInsert(
	ctx context.Context,
	tx pgx.Tx,
	intoTable string,
	columns []string,
	data [][]interface{},
	onConflictSQL string,
	returningSQL string,
	advisoryExLockTargetOid bool,
) (returningRows pgx.Rows, err error) {

	randName := "tmptable_" + randBytesAsHex()

	cmdBatch := new(pgx.Batch)

	cmdBatch.Queue(fmt.Sprintf(
		`CREATE TEMPORARY TABLE %s ( LIKE %s ) ON COMMIT DROP`,
		randName,
		intoTable,
	))
	cmdBatch.Queue(
		`SELECT column_name FROM information_schema.columns WHERE table_name = $1`,
		randName,
	)

	cmdBatchRes := tx.SendBatch(ctx, cmdBatch)
	defer func() {
		if cmdBatchRes != nil {
			finalErr := cmdBatchRes.Close()
			if err == nil {
				err = finalErr
			}
		}
	}()

	if _, err = cmdBatchRes.Exec(); err != nil {
		return
	}

	extraTempNames := make(map[string]struct{}, len(columns))
	dbTempNames, err := cmdBatchRes.Query()
	if err != nil {
		return
	}
	for dbTempNames.Next() {
		extraTempNames[string(dbTempNames.RawValues()[0])] = struct{}{}
	}
	if err = dbTempNames.Err(); err != nil {
		return
	}

	err = cmdBatchRes.Close()
	cmdBatchRes = nil // disarm defer() above
	if err != nil {
		return
	}

	for _, col := range columns {
		_, found := extraTempNames[col]
		if !found {
			err = xerrors.Errorf("Supplied column '%s' does not seem to exist on target table '%s'", col, intoTable)
			return
		}
		delete(extraTempNames, col)
	}

	// ok, we got to delete a few columns
	if len(extraTempNames) > 0 {
		dropList := make([]string, 0, len(extraTempNames))
		for colName := range extraTempNames {
			dropList = append(dropList, "DROP COLUMN "+colName)
		}

		if _, err = tx.Exec(
			ctx,
			fmt.Sprintf(
				"ALTER TABLE %s %s",
				randName,
				strings.Join(dropList, ", "),
			),
		); err != nil {
			return
		}
	}

	if _, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{randName},
		columns,
		pgx.CopyFromRows(data),
	); err != nil {
		return
	}

	sqlColumnList := strings.Join(columns, ",")

	if len(onConflictSQL) > 0 {
		onConflictSQL = "\n" + onConflictSQL
	}
	if len(returningSQL) > 0 {
		returningSQL = "\n" + returningSQL
	}

	if advisoryExLockTargetOid {
		if _, err = tx.Exec(ctx, ObjectExLockStatement, intoTable); err != nil {
			return
		}
	}
	returningRows, err = tx.Query(
		ctx,
		fmt.Sprintf(
			`
			INSERT INTO %s ( %s )
				SELECT %s FROM %s ORDER BY 1%s%s
			`,
			intoTable, sqlColumnList,
			sqlColumnList, randName,
			onConflictSQL,
			returningSQL,
		),
	)
	if err != nil || len(returningSQL) == 0 {
		if returningRows != nil {
			returningRows.Close()
			returningRows = nil
		}
	}

	return
}

const randBytesCount = 16

func randBytesAsHex() string {
	randBinName := pool.Get(randBytesCount)
	defer pool.Put(randBinName)
	rand.Read(randBinName)
	return fmt.Sprintf("%x", randBinName)
}
