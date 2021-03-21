package pgchainbs

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-bs-postgres-chainnotated/lib/cidkeyedlru"
	"github.com/filecoin-project/go-bs-postgres-chainnotated/lib/synccid"
	ipfsblock "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-multihash"

	"github.com/jackc/pgx/v4"
	"github.com/valyala/gozstd"
	"golang.org/x/xerrors"
)

func (dbbs *PgBlockstore) cacheGet(c cid.Cid) *StoredBlock {
	protoBu, found := dbbs.lru.Get(c)

	switch {
	case !found:
		return nil
	case protoBu == nil:
		log.Panicf("impossibly(?) retrieved nil from the cache for cid %s", c.String())
		return nil
	default:
		return protoBu.(*StoredBlock)
	}
}

func (dbbs *PgBlockstore) cachePut(sbs ...*StoredBlock) {
	// do not write to cache until first read: allows for bulk-loads that would not benefit from filling up the cache
	if dbbs.cacheInactiveBeforeRead && atomic.LoadInt32(dbbs.firstReadPerformed) == 0 {
		return
	}

	if len(sbs) == 0 {
		return
	}

	entries := make([]*cidkeyedlru.LruEntry, len(sbs))
	for i := range sbs {
		entries[i] = &cidkeyedlru.LruEntry{
			Cid:    sbs[i].cid,
			Object: sbs[i],
			Cost: int64(sbs[i].size) + // rough accounting for cid+struct overhead: FIXME - this is WRONG
				3*int64(len(sbs[i].cid.KeyString())),
		}
	}

	dbbs.lru.Populate(entries)
}

// This is what backs blockstore.Get/Size/Has
func (dbbs *PgBlockstore) dbGet(rootCid cid.Cid, prefetchDescendentDagLevels int32, aType accessType) (_ *StoredBlock, err error) {
	defer func() {
		if err != nil {
			dbbs.maybeLogUnexpectedErrorf("error retrieving block %s: %s", rootCid.String(), err)
		}
	}()

	// before we call a read at least once there is no point
	if atomic.CompareAndSwapInt32(dbbs.firstReadPerformed, 0, 1) {
		// first read fired - kick off a background prefetch ( it might elect to Panic() on some early errors )
		if dbbs.preloadRecents {
			go dbbs.prefetchRecentToCache()
		}
	}

	// if we can get it out of the cache: do so
	if sb := dbbs.cacheGet(rootCid); sb != nil {
		dbbs.noteAccess(*sb.dbBlockOrdinal, time.Now(), aType|MOD_PREEXISTING)
		return sb, nil
	}

	// if not already in-cache, decode identity on the spot ( same as blockstore.NewIdStore )
	// DO NOT set the cache - it signifies "cid is known in RDBMS", which is not the case
	if rootCid.Prefix().MhType == multihash.IDENTITY {
		dmh, err := multihash.Decode(rootCid.Hash())
		if err != nil {
			return nil, err
		}

		sb := &StoredBlock{
			cid:          rootCid,
			size:         int32(len(dmh.Digest)),
			blockContent: dmh.Digest,
		}

		return sb, nil
	}

	// relatively hot path, save an alloc
	cidKey := rootCid.KeyString()
	cidBuf := pool.Get(len(cidKey))
	copy(cidBuf, cidKey)

	var rows pgx.Rows

	if prefetchDescendentDagLevels > 0 {
		rows, err = dbbs.dbPool.Query(
			context.TODO(),
			fmt.Sprintf(
				`
				WITH RECURSIVE
					cte_dag( descendent_ordinal, level ) AS (

							SELECT block_ordinal, 0
								FROM fil_common_base.datablocks
							WHERE cid = $1::BYTEA AND size IS NOT NULL

						UNION

							SELECT UNNEST( datablocks.linked_ordinals ), cte_dag.level+1
								FROM fil_common_base.datablocks
								JOIN cte_dag
									ON
										datablocks.block_ordinal = cte_dag.descendent_ordinal
											AND
										cte_dag.level < $2
					)
				SELECT %s
					FROM fil_common_base.datablocks
					JOIN cte_dag
						ON
							datablocks.block_ordinal = cte_dag.descendent_ordinal
								AND
							datablocks.size IS NOT NULL
				ORDER BY cte_dag.level
				`,
				StoredBlocksInflatorSelection,
			),
			cidBuf,
			prefetchDescendentDagLevels,
		)
	} else {
		rows, err = dbbs.dbPool.Query(
			context.TODO(),
			fmt.Sprintf(
				`
				SELECT %s
					FROM fil_common_base.datablocks
				WHERE
					cid = $1::BYTEA
						AND
					size IS NOT NULL
				`,
				StoredBlocksInflatorSelection,
			),
			cidBuf,
		)
	}

	pool.Put(cidBuf)

	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, err
	}

	sbs, _, err := dbbs.InflateDbRows(rows, false)

	switch {
	case err != nil:
		return nil, err
	case len(sbs) == 0:
		return nil, nil
	case prefetchDescendentDagLevels == 0 && len(sbs) > 1:
		log.Panicf("impossibly(?) retrieved %d rows for a single unique cid '%s'", len(sbs), rootCid.String())
		return nil, nil
	default:
		dbbs.noteAccess(*sbs[0].dbBlockOrdinal, time.Now(), aType)
		return sbs[0], nil
	}
}

// InflateDbRows transforms result of
// 	`SELECT %StoredBlocksInflatorSelection% FROM fil_common_base.datablocks ...`
// to a set of StoredBlock's, which in turn are the structures implementing
// github.com/ipfs/go-block-format.Block()
// Before returning, Close()s the supplied pgx.Rows()
func (dbbs *PgBlockstore) InflateDbRows(rows pgx.Rows, skipCaching bool) (blocks []*StoredBlock, bytesCached int64, err error) {
	defer rows.Close()

	seen := cid.NewSet()
	ret := make([]*StoredBlock, 0)

	for rows.Next() {
		var sb StoredBlock
		var size *int32
		var cidBytes, dbContentBytes []byte

		err := rows.Scan(&cidBytes, &sb.dbBlockOrdinal, &size, &sb.dbContentEncoding, &dbContentBytes)
		if err != nil {
			return nil, 0, err
		}

		// the SQL query pulled a placeholder: skip
		if size == nil {
			continue
		}
		sb.size = *size

		if sb.cid, err = cid.Cast(cidBytes); err != nil {
			return nil, 0, err
		}

		if !seen.Visit(sb.cid) {
			continue
		}

		switch {

		case sb.cid.Prefix().MhType == multihash.IDENTITY:
			dmh, _ := multihash.Decode(sb.cid.Hash()) // no error checks - we just made this CID
			sb.blockContent = dmh.Digest

		case sb.dbContentEncoding == nil:
			return nil, 0, fmt.Errorf(
				"unexpectedly unspecified (nil) content encoding type for block_ordinal %d (%s)",
				sb.dbBlockOrdinal,
				sb.cid.String(),
			)

		case *sb.dbContentEncoding == 0:
			// *MUST* make a copy, otherwise the LRU will trap the Pgx backing array
			// https://github.com/jackc/pgx/issues/845#issuecomment-705550012
			sb.blockContent = append(
				make([]byte, 0, len(dbContentBytes)),
				dbContentBytes...,
			)

		case *sb.dbContentEncoding == 1:

			// put the magic-prefix back: the copy serves double-duty avoiding
			// https://github.com/jackc/pgx/issues/845#issuecomment-705550012
			sb.dbDeflatedContent = append(append(
				make([]byte, 0, len(zstdMagic)+len(dbContentBytes)),
				zstdMagic...,
			), dbContentBytes...)

		default:
			return nil, 0, fmt.Errorf(
				"unknown content encoding type '%d' for block_ordinal %d (%s)",
				sb.dbContentEncoding,
				sb.dbBlockOrdinal,
				sb.cid.String(),
			)
		}

		ret = append(ret, &sb)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// we got here with no errors: cache everything
	if !skipCaching {
		for _, sb := range ret {
			bytesCached += int64(sb.size)
		}
		dbbs.cachePut(ret...)
	}

	return ret, bytesCached, nil
}

type sbUnit struct {
	sb                      *StoredBlock
	linkedCids              []cid.Cid
	processingError         error
	pooledCidBytes          []byte
	pooledCompressedContent []byte
}

func (sbu *sbUnit) releaseBuffers() {
	pool.Put(sbu.pooledCidBytes)
	pool.Put(sbu.pooledCompressedContent)
}

// This is what backs blockstore.Put/PutMany
func (dbbs *PgBlockstore) dbStore(blks []ipfsblock.Block) (err error) {

	if len(blks) == 0 {
		return nil
	}

	defer func() {
		if err != nil {
			dbbs.maybeLogUnexpectedErrorf("error while storing %d blocks: %s", len(blks), err)
		}
	}()

	cacheIsActive := !dbbs.cacheInactiveBeforeRead || atomic.LoadInt32(dbbs.firstReadPerformed) != 0

	var wgCompress, wgParse sync.WaitGroup
	var referencedCids synccid.Set

	toInsertCidBytes := make([][]byte, 0, len(blks))
	toInsertBlockUnits := make(map[string]*sbUnit, len(blks))
	defer func() {
		wgCompress.Wait() // in case we bailed early we need to release after the pooled slices are no longer in use
		for _, sbu := range toInsertBlockUnits {
			sbu.releaseBuffers()
		}
	}()

	// use the same PUT-time for all members in same batch
	now := time.Now()

	// determine what do we want to insert, and start any async preprocessing needed
	for i := range blks {
		blkCid := blks[i].Cid()

		// sometimes we are given duplicate CIDs to insert :(
		if _, isDuplicate := toInsertBlockUnits[blkCid.KeyString()]; isDuplicate {
			log.Debugf("unexpected duplicate insert of block %s", blkCid)
			continue
		}

		// if it is found in the cache - it got to have been already fully processed
		if cacheIsActive {
			if cbu := dbbs.cacheGet(blkCid); cbu != nil {
				dbbs.noteAccess(*cbu.dbBlockOrdinal, now, PUT|MOD_PREEXISTING)
				continue
			}
		}

		blockData := blks[i].RawData()

		cidBytes := pool.Get(len(blkCid.KeyString()))
		copy(cidBytes, blkCid.KeyString())
		toInsertCidBytes = append(toInsertCidBytes, cidBytes)

		sbu := sbUnit{
			sb: &StoredBlock{
				cid:  blkCid,
				size: int32(len(blockData)),
			},
			pooledCidBytes: cidBytes,
		}
		toInsertBlockUnits[blkCid.KeyString()] = &sbu

		if AssumeUserNeverMutatesBlockdata {
			sbu.sb.blockContent = blockData
		} else {
			sbu.sb.blockContent = append(
				make([]byte, 0, len(blockData)),
				blockData...,
			)
		}

		// we won't be able to write any of the results anyway - do not preprocess
		if !dbbs.isWritable {
			continue
		}

		if dbbs.parseBlockLinks {
			wgParse.Add(1)
			dbbs.limiterBlockProcessing <- struct{}{}
			go func() {
				sbu.linkedCids, sbu.processingError = sbu.sb.extractLinks()
				referencedCids.Add(sbu.linkedCids...)
				<-dbbs.limiterBlockProcessing
				wgParse.Done()
			}()
		}

		// no content to insert for identity cids: no compression
		if blkCid.Prefix().MhType == multihash.IDENTITY {
			continue
		}

		wgCompress.Add(1)
		dbbs.limiterBlockProcessing <- struct{}{}

		go func() {
			sbu.sb.dbContentEncoding = new(int16)

			// protect from underruns, it's all pooled anyway, eagerness is not a concern
			bufSize := int(sbu.sb.size)
			if bufSize < 128 {
				bufSize = 128
			}
			bufSize += 128

			buf := pool.Get(bufSize)
			// Despite the cgo boundary cross, this is 2x faster than using "github.com/klauspost/compress/zstd"
			compressedData := gozstd.CompressLevel(
				buf[:0],
				blockData,
				zstdCompressLevel,
			)

			if !bytes.Equal(compressedData[0:4], zstdMagic) {
				log.Panicf("zstandard compressor produced stream prefixed by 0x%X instead of the expected 0x%X", compressedData[0:4], zstdMagic)
			}

			// check if compression is worthwhile
			// ( also means compressedData and buf are the same array still )
			if len(compressedData)-4 < len(blockData) {
				// this might seem like a microoptimization, but it gives over 5GiB of direct saving for a 4 month old chain 🙀
				sbu.pooledCompressedContent = compressedData[4:]
				*sbu.sb.dbContentEncoding = 1
			} else {
				// not going to use that - return it right awy
				pool.Put(buf)
			}

			<-dbbs.limiterBlockProcessing
			wgCompress.Done()
		}()
	}

	// everything happened to be in the cache \o/
	if len(toInsertCidBytes) == 0 {
		return nil
	}

	// welp - let's get to work
	//
	ctx := context.TODO()

	// see what's already there: marginally cheaper, and allows us to track PREEXISTING
	// forgo a txn - it's a single query
	var existingFullBlocks pgx.Rows
	existingFullBlocks, err = dbbs.dbPool.Query(
		ctx,
		`
		SELECT cid, block_ordinal
			FROM fil_common_base.datablocks
		WHERE
			cid = ANY ( $1::BYTEA[] )
				AND
			size IS NOT NULL
		`,
		toInsertCidBytes,
	)
	defer func() {
		if existingFullBlocks != nil {
			existingFullBlocks.Close()
		}
	}()
	if err != nil {
		return err
	}

	for existingFullBlocks.Next() {
		var cidBytes []byte
		var blockOrdinal *int64

		if err = existingFullBlocks.Scan(&cidBytes, &blockOrdinal); err != nil {
			return err
		}

		toInsertBlockUnits[string(cidBytes)].sb.dbBlockOrdinal = blockOrdinal
	}

	if err = existingFullBlocks.Err(); err != nil {
		return err
	}

	existingFullBlocks.Close()
	existingFullBlocks = nil // disarm defer

	// wait for the backgrounds to finish ( no further sb modifications / error conditions )
	wgCompress.Wait()
	wgParse.Wait()

	toCache := make([]*StoredBlock, 0, len(toInsertCidBytes))
	for _, cidBytes := range toInsertCidBytes {
		sbu := toInsertBlockUnits[string(cidBytes)]

		if sbu.sb.dbBlockOrdinal != nil {
			// retrieved above - done.
			delete(toInsertBlockUnits, string(cidBytes))
			sbu.releaseBuffers() // we delete the entry, means it will not be caught by the scope defer to clean up
			if cacheIsActive {
				dbbs.noteAccess(*sbu.sb.dbBlockOrdinal, now, PUT|MOD_PREEXISTING)
				toCache = append(toCache, sbu.sb)
			}
		} else if sbu.processingError != nil {
			return sbu.processingError
		}
	}
	if len(toCache) > 0 {
		dbbs.cachePut(toCache...)
	}

	// everything happened to be in-db already!
	if len(toInsertBlockUnits) == 0 {
		return nil
	}

	if !dbbs.isWritable {
		return xerrors.New("unable to write new blocks into a read-only store")
	}

	var tx pgx.Tx
	tx, err = dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadUncommitted}) // ReadUncommitted is unsupported but eh...
	defer func() {
		if err != nil && tx != nil {
			dbbs.maybeLogUnexpectedErrorf("error-induced rollback during block insertion: %s", err)
			tx.Rollback(ctx) // no error checks
		}
	}()
	if err != nil {
		return xerrors.Errorf("failed to start transaction: %w", err)
	}

	// If we are doing linkparses insert all placeholders and pull their ordinals out
	// Not trying to be clever here with upserts etc: logic is too complex as it is
	var ordinalsDict map[string]int64
	if dbbs.parseBlockLinks {
		ordinalsDict, err = dbbs.getCidOrdinals(ctx, tx, &referencedCids)
	}

	// final version of what remains to be inserted
	insertFullBlockEntries := make([][]interface{}, 0, len(toInsertBlockUnits))
	for _, be := range toInsertBlockUnits {

		cidPrefix := be.sb.cid.Prefix()

		var dbContent *[]byte
		if cidPrefix.MhType != multihash.IDENTITY {

			switch *be.sb.dbContentEncoding {

			case 0:
				dbContent = &be.sb.blockContent

			case 1:
				dbContent = &be.pooledCompressedContent

			default:
				log.Panicf("unknown content encoding %d", *be.sb.dbContentEncoding)

			}
		}

		var subdagDepth *int32
		var linkedOrdinals []*int64
		if cidPrefix.Codec == cid.Raw {
			cnt := int32(1)
			subdagDepth = &cnt
			linkedOrdinals = emptyLinkage
		} else if dbbs.parseBlockLinks {
			linkedOrdinals = make([]*int64, len(be.linkedCids))
			for i, c := range be.linkedCids {
				v := ordinalsDict[c.KeyString()]
				linkedOrdinals[i] = &v
			}
			if len(linkedOrdinals) == 0 {
				cnt := int32(1)
				subdagDepth = &cnt
			}
		}

		insertFullBlockEntries = append(insertFullBlockEntries, []interface{}{
			subdagDepth,
			be.sb.size,
			be.sb.dbContentEncoding,
			be.pooledCidBytes,
			linkedOrdinals,
			dbContent,
		})
	}

	conflictStanza := `ON CONFLICT ( cid ) DO UPDATE	SET
		size = EXCLUDED.size,
		content_encoding = EXCLUDED.content_encoding,
		content = EXCLUDED.content
	`
	if dbbs.parseBlockLinks {
		conflictStanza +=
			`,
			subdag_maxdepth = COALESCE( EXCLUDED.subdag_maxdepth, fil_common_base.datablocks.subdag_maxdepth ),
			linked_ordinals = EXCLUDED.linked_ordinals
			`
	}

	var upserts pgx.Rows
	upserts, err = dbBulkInsert(
		ctx, tx,
		"fil_common_base.datablocks",
		[]string{
			"subdag_maxdepth",
			"size",
			"content_encoding",
			"cid",
			"linked_ordinals",
			"content",
		},
		insertFullBlockEntries,
		conflictStanza,
		`RETURNING cid, block_ordinal`,
		true, // do ExLock target table
	)
	if err != nil {
		return err
	}
	// FIXME BUG: https://github.com/jackc/pgx/issues/938#issue-806472537
	// defer upserts.Close()
	for upserts.Next() {
		var cidBytes []byte
		var blockOrdinal *int64
		if err = upserts.Scan(&cidBytes, &blockOrdinal); err != nil {
			return err
		}
		toInsertBlockUnits[string(cidBytes)].sb.dbBlockOrdinal = blockOrdinal
	}

	if err = upserts.Err(); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	// Everything got inserted: sancheck and populate the cache
	toCache = toCache[:0]
	for _, sbu := range toInsertBlockUnits {
		if sbu.sb.dbBlockOrdinal == nil {
			log.Panicf("Failed to retrieve a block_ordinal for newly inserted block %s", sbu.sb.cid.String())
		}
		if cacheIsActive {
			dbbs.noteAccess(*sbu.sb.dbBlockOrdinal, now, PUT)
			toCache = append(toCache, sbu.sb)
		}
	}
	if len(toCache) > 0 {
		dbbs.cachePut(toCache...)
	}

	return nil
}
