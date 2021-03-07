package pgchainbs

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-bs-postgres-chainnotated/lib/synccid"
	"github.com/filecoin-project/go-state-types/abi"
	filbig "github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"golang.org/x/xerrors"
)

// DestructuredFilTipSetData and DestructuredFilTipSetHeaderBlock represent the
// pre-parsed contents of a Filecoin tipset structure. The indirection is
// unavoidable as using lotus-defined types leads to import cycles.
type DestructuredFilTipSetData struct {
	Epoch                    abi.ChainEpoch
	ParentWeight             filbig.Int
	ParentBaseFee            filbig.Int
	ParentStaterootCid       cid.Cid
	ParentMessageReceiptsCid cid.Cid
	TipSetCids               []cid.Cid // also duplicated in []HeaderBlocks when set
	ParentTipSetCids         []cid.Cid
	HeaderBlocks             []DestructuredFilTipSetHeaderBlock
	BeaconRoundAndData       [][]byte
}
type DestructuredFilTipSetHeaderBlock struct {
	MinerActorID             uint64
	HeaderCid                cid.Cid
	MessagesCid              cid.Cid
	TypedSignature           []byte
	ForkSignalVarint         []byte
	TicketProof              []byte
	ElectionWincountAndProof []byte
	WinpostTypesAndProof     [][]byte
}

// GetFilTipSetHead queries the configured instance namespace and retrieves
// the epoch and ordered CIDs of the most-recently Visit()-ed tipset.
func (dbbs *PgBlockstore) GetFilTipSetHead(ctx context.Context) (*DestructuredFilTipSetData, error) {

	if dbbs.InstanceNamespace() == "" {
		return nil, xerrors.New("unable to invoke CurrentFilTipSetKey without a previously configured instance namespace")
	}

	return dbbs.tsdFromQuery(
		ctx,
		fmt.Sprintf(
			`SELECT tipset_cids, epoch, parent_stateroot_cid, parent_state_basefee, parent_state_weight FROM %s.current_head`,
			dbbs.InstanceNamespace(),
		),
	)
}

// FindFilTipSet queries the shared tipset store for the tipset formed from the
// provided chain header block CIDs. If the optional walkBackEpochs is larger
// than 0, return the parent at that distance from the given tipset.
func (dbbs *PgBlockstore) FindFilTipSet(ctx context.Context, tipsetCids []cid.Cid, walkBackEpochs abi.ChainEpoch) (*DestructuredFilTipSetData, error) {

	if walkBackEpochs < 0 {
		return nil, xerrors.Errorf("provided value '%d' for walkBackEpochs can not be negative", walkBackEpochs)
	}

	tskBytes := make([][]byte, len(tipsetCids))
	for i := range tipsetCids {
		tskBytes[i] = tipsetCids[i].Bytes()
	}

	return dbbs.tsdFromQuery(
		ctx,
		`
		WITH RECURSIVE
			tipset_walk AS (
					SELECT t.tipset_ordinal, t.parent_stateroot_cid, 0 AS depth
						FROM fil_common_base.tipsets t
					WHERE t.tipset_cids = $1::BYTEA[]
				UNION ALL
					SELECT parent_tipset.tipset_ordinal, parent_tipset.parent_stateroot_cid, tipset_walk.depth+1 AS depth
						FROM tipset_walk
						JOIN fil_common_base.states parent_state
							ON tipset_walk.parent_stateroot_cid = parent_state.stateroot_cid
						JOIN fil_common_base.tipsets parent_tipset
							ON parent_state.applied_tipset_cids = parent_tipset.tipset_cids
					WHERE tipset_walk.depth < $2
			)
		SELECT t.tipset_cids, t.epoch, t.parent_stateroot_cid, s.basefee, s.weight
			FROM tipset_walk
			JOIN fil_common_base.tipsets t
				ON tipset_walk.depth = $2 AND tipset_walk.tipset_ordinal = t.tipset_ordinal
			JOIN fil_common_base.states s
				ON t.parent_stateroot_cid = s.stateroot_cid
		`,
		tskBytes,
		walkBackEpochs,
	)
}

func (dbbs *PgBlockstore) tsdFromQuery(ctx context.Context, sql string, args ...interface{}) (*DestructuredFilTipSetData, error) {

	dts := new(DestructuredFilTipSetData)

	var tskBytes [][]byte
	var parentStateCidBytes []byte
	var parentBaseFee, parentWeight pgtype.Numeric

	err := dbbs.PgxPool().QueryRow(
		ctx,
		sql,
		args...,
	).Scan(&tskBytes, &dts.Epoch, &parentStateCidBytes, &parentBaseFee, &parentWeight)

	if err != nil {
		return nil, err
	}

	dts.ParentBaseFee = filbig.NewFromGo(parentBaseFee.Int)
	dts.ParentWeight = filbig.NewFromGo(parentWeight.Int)

	dts.ParentStaterootCid, err = cid.Cast(parentStateCidBytes)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse parent stateroot CID '%s': %w", parentStateCidBytes, err)
	}

	dts.TipSetCids = make([]cid.Cid, len(tskBytes))
	for i := range tskBytes {
		dts.TipSetCids[i], err = cid.Cast(tskBytes[i])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse tipset key CID '%s': %w", tskBytes[i], err)
		}
	}

	if len(dts.TipSetCids) == 0 {
		return nil, xerrors.Errorf(
			"impossibly(?) ended up with no CIDs from an otherwise successful query against the RDBMS returning %#v",
			tskBytes,
		)
	}

	return dts, nil
}

// StoreFilTipSetVisit records the timing and potentially adjust orphan lists
// when isHeadChange is set.
func (dbbs *PgBlockstore) StoreFilTipSetVisit(ctx context.Context, tsDbOrdinal *int32, tsEpoch abi.ChainEpoch, visitAt time.Time, isHeadChange bool) (err error) {

	if tsDbOrdinal == nil {
		return xerrors.New("impossibly(?) invoked StoreFilTipSetVisit() with NULL tipset_ordinal")
	}

	if !dbbs.isWritable {
		return xerrors.New("unable to StoreFilTipSetVisit() on a read-only store")
	}

	var tx pgx.Tx
	tx, err = dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	defer func() {
		if tx != nil {
			if err == nil {
				err = tx.Commit(ctx)
			}
			if err != nil {
				dbbs.maybeLogUnexpectedErrorf("error-induced rollback during tipset visit storage: %s", err)
				tx.Rollback(ctx)
			}
		}
	}()
	if err != nil {
		return xerrors.Errorf("failed to start transaction: %w", err)
	}

	pgBatch := new(pgx.Batch)

	pgBatch.Queue(
		fmt.Sprintf(
			// yes, need the "DO NOTHING", I've witnessed millisecond-time collisions 🤯
			`
			INSERT INTO %s.tipsets_visited ( visit_wall_time, tipset_ordinal )
				VALUES ( $1, $2 )
			ON CONFLICT DO NOTHING
			`,
			dbbs.instanceNamespace,
		),
		visitAt, tsDbOrdinal,
	)

	if isHeadChange {

		// mark head
		pgBatch.Queue(
			fmt.Sprintf(
				`UPDATE %s.tipsets_visited SET is_current_head = NULL`,
				dbbs.instanceNamespace,
			),
		)
		pgBatch.Queue(
			fmt.Sprintf(
				`UPDATE %s.tipsets_visited SET is_current_head = TRUE WHERE visit_wall_time = $1`,
				dbbs.instanceNamespace,
			),
			visitAt, // this is an actual UNIQUE column, sufficient for locating the row
		)

		// readjust orphans by:
		//

		// 1. marking everything since CurHead()-MaxOrphanLookback() as orphaned
		pgBatch.Queue(
			fmt.Sprintf(
				`
				INSERT INTO %s.states_orphaned ( state_ordinal )
					SELECT state_ordinal
						FROM fil_common_base.states
					WHERE states.epoch >= $1
				ON CONFLICT DO NOTHING
				`,
				dbbs.instanceNamespace,
			),
			tsEpoch-MaxOrphanLookback,
		)

		// 2. walking from our current node MaxOrphanLookback-steps and *un*-marking
		// ( this is ridiculously efficient )
		pgBatch.Queue(
			fmt.Sprintf(
				`
				WITH RECURSIVE
					live_segment AS (
							SELECT s.state_ordinal, s.applied_tipset_cids, 0 AS depth
								FROM fil_common_base.states s
								JOIN fil_common_base.tipsets t
									ON t.tipset_ordinal = $1 AND s.stateroot_cid = t.parent_stateroot_cid
						UNION ALL
							SELECT parent_state.state_ordinal, parent_state.applied_tipset_cids, live_segment.depth+1 AS depth
								FROM live_segment
								JOIN fil_common_base.tipsets parent_tipset
									ON live_segment.applied_tipset_cids = parent_tipset.tipset_cids
								JOIN fil_common_base.states parent_state
									ON parent_tipset.parent_stateroot_cid = parent_state.stateroot_cid
							WHERE live_segment.depth <= $2
					)
				DELETE FROM %s.states_orphaned WHERE state_ordinal IN ( SELECT state_ordinal FROM live_segment )
				`,
				dbbs.instanceNamespace,
			),
			tsDbOrdinal,
			MaxOrphanLookback,
		)
	}

	return tx.SendBatch(ctx, pgBatch).Close()
}

// StoreFilTipSetData records basic metadata from the provided destructured
// tipset data container. These are implemented using only basic types to
// break the dependency cycles.
func (dbbs *PgBlockstore) StoreFilTipSetData(ctx context.Context, tsd *DestructuredFilTipSetData) (tipsetDbOrdinal *int32, err error) {

	if !dbbs.isWritable {
		return nil, xerrors.New("unable to StoreFilTipSetData() on a read-only store")
	}

	var tx pgx.Tx
	tx, err = dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	defer func() {
		if tx != nil {
			if err == nil {
				err = tx.Commit(ctx)
			}
			if err != nil {
				dbbs.maybeLogUnexpectedErrorf("error-induced rollback during tipset data storage: %s", err)
				tx.Rollback(ctx)
			}
		}
	}()
	if err != nil {
		return nil, xerrors.Errorf("failed to start transaction: %w", err)
	}

	// Without a db-side lock, the UPSERTS will eventually stall
	if _, err = tx.Exec(ctx, ObjectExLockStatement, "fil_common_base.tipsets"); err != nil {
		return nil, xerrors.Errorf("failure to obtain tipset advisory lock: %w", err)
	}

	tipSetKeyRaw := make([][]byte, 0, len(tsd.HeaderBlocks))
	tipSetKeyB32 := make([]string, 0, len(tsd.HeaderBlocks))
	for _, h := range tsd.HeaderBlocks {
		tipSetKeyRaw = append(tipSetKeyRaw, h.HeaderCid.Bytes())
		tipSetKeyB32 = append(tipSetKeyB32, h.HeaderCid.String())
	}

	var preExisting bool
	if err = tx.QueryRow(
		ctx,
		`
		WITH
			new_tipset AS (
				INSERT INTO fil_common_base.tipsets ( epoch, parent_stateroot_cid, tipset_cids, tipset_key )
					VALUES ( $1, $2::BYTEA, $3::BYTEA[], $4::TEXT[] )
				ON CONFLICT DO NOTHING
				RETURNING tipset_ordinal
			)
		SELECT tipset_ordinal, false FROM new_tipset
			UNION
		SELECT tipset_ordinal, true FROM fil_common_base.tipsets WHERE tipset_cids = $3::BYTEA[]
		`,
		tsd.Epoch,
		tsd.ParentStaterootCid.Bytes(),
		tipSetKeyRaw,
		tipSetKeyB32,
	).Scan(&tipsetDbOrdinal, &preExisting); err != nil {
		// unlike other insertion codepaths here we hold an exclusive lock: an error is an error
		return nil, xerrors.Errorf("unexpectedly failed to retrieve just-inserted tipset data: %w", err)
	}

	if preExisting {
		return tipsetDbOrdinal, err
	}

	// accumulator for CIDs that might not yet be in the block table ( partial imports, etc )
	var referencedCids synccid.Set

	referencedCids.Add(tsd.ParentStaterootCid, tsd.ParentMessageReceiptsCid)
	referencedCids.Add(tsd.ParentTipSetCids...)
	for _, h := range tsd.HeaderBlocks {
		referencedCids.Add(h.HeaderCid, h.MessagesCid)
	}

	// insert all (possibly-dangling) cids into the main table if not yet seen
	// in order to satisfy various FK constraints
	_, err = dbbs.getCidOrdinals(ctx, tx, &referencedCids)
	if err != nil {
		return nil, err
	}

	parentTipSetKeyRaw := make([][]byte, 0, len(tsd.ParentTipSetCids))
	for _, c := range tsd.ParentTipSetCids {
		parentTipSetKeyRaw = append(parentTipSetKeyRaw, c.Bytes())
	}

	// both tipset 0 and tipset 1 point to state 0
	// other than that tipet N points to state N-1
	parentStateHeight := tsd.Epoch
	if parentStateHeight != 0 {
		parentStateHeight--
	}

	pgBatch := new(pgx.Batch)

	pgBatch.Queue(
		`
		INSERT INTO fil_common_base.states ( epoch, weight, basefee, stateroot_cid, applied_tipset_cids, applied_messages_receipts_cid, next_beacon_round_and_data )
			VALUES ( $1, $2, $3, $4::BYTEA, $5::BYTEA[], $6::BYTEA, $7::BYTEA[] )
		ON CONFLICT DO NOTHING
		`,
		parentStateHeight,
		tsd.ParentWeight.String(),
		tsd.ParentBaseFee.String(),
		tsd.ParentStaterootCid.Bytes(),
		parentTipSetKeyRaw,
		tsd.ParentMessageReceiptsCid.Bytes(),
		tsd.BeaconRoundAndData,
	)

	for _, hdr := range tsd.HeaderBlocks {

		pgBatch.Queue(
			`
			INSERT INTO fil_common_base.chainblocks (
				miner_actor_id,
				epoch,
				chainblock_cid,
				parent_stateroot_cid,
				messages_cid,
				typed_signature,
				ticket_proof,
				fork_signalling_varint,
				election_wincount_and_proof,
				winpost_types_and_proofs
			) VALUES (
				$1,
				$2,
				$3::BYTEA,
				$4::BYTEA,
				$5::BYTEA,
				$6::BYTEA,
				$7::BYTEA,
				$8::BYTEA,
				$9::BYTEA,
				$10::BYTEA[]
			)
			ON CONFLICT DO NOTHING
			`,
			hdr.MinerActorID,
			tsd.Epoch,
			hdr.HeaderCid.Bytes(),
			tsd.ParentStaterootCid.Bytes(),
			hdr.MessagesCid.Bytes(),
			hdr.TypedSignature,
			hdr.TicketProof,
			hdr.ForkSignalVarint,
			hdr.ElectionWincountAndProof,
			hdr.WinpostTypesAndProof,
		)
	}

	// execute entire batch in one go
	if err := tx.SendBatch(ctx, pgBatch).Close(); err != nil {
		return nil, err
	}

	return tipsetDbOrdinal, nil
}
