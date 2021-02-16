package pgchainbs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/jackc/pgx/v4"
	"golang.org/x/xerrors"
)

type accessType uint8

// Access-type constants for the (optional) detailed-access logs.
const (
	_MASK_TYPE = 0b11

	PUT  = accessType(0)
	GET  = accessType(1)
	HAS  = accessType(2)
	SIZE = accessType(3)

	// When OR-ed with above indicates database R/W access skipped due to cache hit or already-existing entry
	MOD_PREEXISTING = accessType(1 << 6)
)

type accessUnit struct {
	atUnix         time.Time
	dbBlockOrdinal int64
	accessType     accessType
}

func (dbbs *PgBlockstore) noteAccess(blockOrdinal int64, t time.Time, atype accessType) {
	if dbbs.instanceNamespace == "" || atomic.LoadInt32(dbbs.firstReadPerformed) == 0 {
		return
	}

	if blockOrdinal == 0 {
		log.Panicf("impossible(?) invocation of noteAccess() with a zero blockOrdinal")
	}

	dbbs.accessLogsMu.Lock()

	// FIXME: not sure what to track as "recent" exactly: just write down all non-PUT's for now...
	if atype&_MASK_TYPE != PUT {
		dbbs.accessLogsRecent[blockOrdinal] = struct{}{}
	}

	if dbbs.accessLogsDetailed != nil {
		dbbs.accessLogsDetailed[accessUnit{
			atUnix:         t.Truncate(time.Millisecond), // reduce resolution for saner aggregation
			dbBlockOrdinal: blockOrdinal,
			accessType:     atype,
		}]++
	}

	dbbs.accessLogsMu.Unlock()
}

// FlushAccessLogs is a function specific to filecoin when this blockstore
// operates as the backing solution of the chain. It expects an epoch and
// optionally a tipset RDBMS-side ordinal to annotate the recently-accessed
// blocks log and the (optional) detailed-access log.
func (dbbs *PgBlockstore) FlushAccessLogs(contextEpoch abi.ChainEpoch, contextTipsetDbOrdinal *int32) error {

	if dbbs.instanceNamespace == "" {
		return errors.New("Invoking FLushAccessLogs() on a namespace-less annotated blockstore is not possible")
	}

	dbbs.accessLogsMu.Lock()
	defer dbbs.accessLogsMu.Unlock()

	// save the individual access logs asynchronously: if it fails - it fails
	if dbbs.accessLogsDetailed != nil && contextTipsetDbOrdinal != nil {

		detailedLogs := make([][]interface{}, 0, len(dbbs.accessLogsDetailed))
		for au, count := range dbbs.accessLogsDetailed {
			detailedLogs = append(detailedLogs, []interface{}{
				au.atUnix, au.dbBlockOrdinal, au.accessType, count, contextEpoch, contextTipsetDbOrdinal,
			})
		}

		// reset the counters: if the background populator below fails - it fails
		dbbs.accessLogsDetailed = make(map[accessUnit]int64, 16384)

		// launch into background, block if contention rises too far
		dbbs.limiterLogsDetailedWrite <- struct{}{}
		go func() {

			if _, err := dbbs.dbPool.CopyFrom(
				context.Background(),
				pgx.Identifier{dbbs.instanceNamespace, "datablocks_accesslog"},
				[]string{"access_wall_time", "block_ordinal", "access_type", "access_count", "context_epoch", "context_tipset_rdinal"},
				pgx.CopyFromRows(detailedLogs),
			); err != nil {
				dbbs.maybeLogUnexpectedErrorf("failure writing detailed access logs for epoch/tipsetOrdinal %d/%d: %s", contextEpoch, contextTipsetDbOrdinal, err)
			}

			<-dbbs.limiterLogsDetailedWrite
		}()
	}

	// if the previous "last-get" update is done - do the next one ( see `default:` at end )
	// we do it asynchronously in a one-at-a-time, fire-and-forget manner, which is good enough here
	select {
	case dbbs.limiterSetLastAccess <- struct{}{}:

		ordinalsToUpdate := make([]int64, 0, len(dbbs.accessLogsRecent))
		for blockOrdinal := range dbbs.accessLogsRecent {
			ordinalsToUpdate = append(ordinalsToUpdate, blockOrdinal)
		}

		// now that we decided what to update: reset the counter
		// if the update further down fails - it fails
		dbbs.accessLogsRecent = make(map[int64]struct{}, 16384)

		// somehow nothing to do...
		if len(ordinalsToUpdate) == 0 {

			<-dbbs.limiterSetLastAccess

		} else {

			go func() (err error) {
				defer func() { <-dbbs.limiterSetLastAccess }()

				ctx := context.Background()

				tx, err := dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
				defer func() {
					if err == nil {
						err = tx.Commit(ctx)
					}
					if err != nil {
						dbbs.maybeLogUnexpectedErrorf("rollback during recent-access bookkeeping for epoch/tipsetOrdinal %d/%d: %s", contextEpoch, contextTipsetDbOrdinal, err)
						if tx != nil {
							tx.Rollback(ctx)
						}
					}
				}()
				if err != nil {
					return xerrors.Errorf("failed to start transaction: %w", err)
				}

				pgBatch := new(pgx.Batch)

				var linearSyncCount int64
				if atomic.LoadInt64(dbbs.lastFlushEpoch)+1 == int64(contextEpoch) {
					linearSyncCount = atomic.AddInt64(dbbs.linearSyncEventCount, 1)
				}
				atomic.StoreInt64(dbbs.lastFlushEpoch, int64(contextEpoch))

				// we stabilized sufficiently: it is safe to start purging logs
				// FIXME/SANCHECK: what constititutes "stabilized" is somewhat arbitrary
				if linearSyncCount > 30 {
					// Delete everything that is:
					// - beyond trackRecentTipsets in the past
					// - more than MaxFutureEpochsAsRecent in the future
					pgBatch.Queue(
						fmt.Sprintf(`DELETE FROM %s.datablocks_recent WHERE last_access_epoch NOT BETWEEN $1 AND $2`, dbbs.instanceNamespace),
						contextEpoch-TrackRecentTipsetsCount, (contextEpoch + MaxFutureEpochsAsRecent),
					)
				}

				// might as well pre-sort, it costs us nothing
				sort.Slice(ordinalsToUpdate, func(i, j int) bool {
					return ordinalsToUpdate[i] < ordinalsToUpdate[j]
				})

				// add new logentries
				pgBatch.Queue(
					fmt.Sprintf(
						`
						INSERT INTO %s.datablocks_recent( block_ordinal, last_access_epoch )
							SELECT UNNEST( $1::BIGINT[] ), $2
						ON CONFLICT ( block_ordinal ) DO UPDATE
							SET last_access_epoch = EXCLUDED.last_access_epoch
						`,
						dbbs.instanceNamespace,
					),
					ordinalsToUpdate,
					contextEpoch,
				)

				return tx.SendBatch(ctx, pgBatch).Close()
			}()
		}

	default:
		// nothing - wait for the next round
	}

	return nil
}
