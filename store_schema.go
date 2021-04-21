package pgchainbs

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"golang.org/x/xerrors"
)

const (
	// older versions might very well work, but this is what I developed against/tested with
	minimumPgVersion = 13_00_00
)

func (dbbs *PgBlockstore) commonDDL() []string {
	return []string{

		`CREATE SCHEMA IF NOT EXISTS fil_sysinfo`,
		`COMMENT ON SCHEMA fil_sysinfo IS 'Various system info views and functions'`,

		`CREATE SCHEMA IF NOT EXISTS fil_common_base`,
		`COMMENT ON SCHEMA fil_common_base IS 'Append-only shared base chain data-tables and views'`,

		`CREATE SCHEMA IF NOT EXISTS fil_common_derived`,
		`COMMENT ON SCHEMA fil_common_derived IS 'Append-only shared derived chain events data-tables and views'`,

		//
		// basic block storage
		//
		`
		CREATE OR REPLACE
			FUNCTION fil_common_base.array_elements_are_distinct_non_null(ANYARRAY) RETURNS BOOLEAN
			LANGUAGE sql IMMUTABLE PARALLEL SAFE
		AS $$
			SELECT
				$1 IS NOT NULL
					AND
				(SELECT COUNT( DISTINCT( elt ) ) FROM UNNEST($1) AS elt) = COALESCE( ARRAY_UPPER($1, 1), 0 )
		$$
		`,
		`COMMENT ON FUNCTION fil_common_base.array_elements_are_distinct_non_null IS 'Internal function used for various column constraints'`,

		`
		DO $$
			BEGIN
				IF NOT EXISTS (SELECT 42 FROM pg_tables WHERE schemaname = 'fil_common_base' AND tablename = 'datablocks_content_encodings') THEN
					CREATE TABLE fil_common_base.datablocks_content_encodings(
						encoding SMALLINT NOT NULL UNIQUE CONSTRAINT valid_content_encoding CHECK ( encoding >= 0 ),
						metadata JSONB NOT NULL
					);
					COMMENT ON TABLE fil_common_base.datablocks_content_encodings IS 'List of supported block-content storage-methods';
					INSERT INTO fil_common_base.datablocks_content_encodings ( encoding, metadata )
						VALUES
							( 0, '{ "description": "Directly stored raw (incompressible) data" }' ),
							( 1, '{ "description": "RFC8478 zstandard-compressed content with the 4 byte magic prefix 28B52FFD stripped off (no dictionary)" }' )
					;
				END IF;
		END $$
		`,

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS fil_common_base.datablocks(

			block_ordinal BIGINT NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY,
			subdag_maxdepth INTEGER CONSTRAINT valid_subdag_maxdepth CHECK ( subdag_maxdepth > 0 OR subdag_maxdepth IS NULL ),
			size INTEGER CONSTRAINT valid_size CHECK ( size >= 0 OR size IS NULL ),
			content_encoding SMALLINT REFERENCES fil_common_base.datablocks_content_encodings( encoding ),
			cid BYTEA NOT NULL,
			cid_tail BYTEA NOT NULL GENERATED ALWAYS AS ( CASE WHEN ( OCTET_LENGTH(cid) <= %d ) THEN ( cid ) ELSE ( SUBSTRING( cid, OCTET_LENGTH(cid) - %d ) ) ) ) STORED,
			linked_ordinals BIGINT[],
			content BYTEA,

			CONSTRAINT no_semipopulated_placeholder CHECK (
				size IS NOT NULL
					OR
				(
					linked_ordinals IS NULL
						AND
					content IS NULL
				)
			),
			CONSTRAINT no_partial_subdag_info CHECK (	linked_ordinals IS NOT NULL OR subdag_maxdepth IS NULL ),
			CONSTRAINT leaf_subdag_marked CHECK (
				linked_ordinals IS NULL
					OR
				linked_ordinals != ARRAY[]::BIGINT[]
					OR
				( subdag_maxdepth IS NOT NULL AND subdag_maxdepth = 1 )
			),

			CONSTRAINT no_untyped_content CHECK ( ( content IS NULL ) = ( content_encoding IS NULL ) ),
			CONSTRAINT suitable_content_type CHECK (
				size IS NULL
					OR
				( LENGTH( content ) = size AND content_encoding IS NOT NULL AND content_encoding = 0 )
					OR
				( LENGTH( content ) < size AND content_encoding IS NOT NULL AND content_encoding > 0 )
			),

			CONSTRAINT no_identity_content CHECK (
				SUBSTRING( cid FROM 3 FOR 1 ) != '\x00'
					OR
				( content IS NULL AND size IS NOT NULL )
			),

			CONSTRAINT valid_linked_ordinals CHECK (
				linked_ordinals IS NULL
					OR
				-- eventually this one will be replaced with https://www.postgresql-archive.org/GSoC-2017-Foreign-Key-Arrays-tp5962835p6175483.html
				(
					NOT ( 0 = ANY( linked_ordinals ) )
						AND
					fil_common_base.array_elements_are_distinct_non_null( linked_ordinals )
				)
			),

			CONSTRAINT no_rawdata_links CHECK (
				size IS NULL
					OR
				SUBSTRING( cid FROM 1 FOR 2 ) != '\x0155'
					OR
				( linked_ordinals IS NOT NULL AND linked_ordinals = ARRAY[]::BIGINT[] )
			)
		)
		`, CidTailBytes, CidTailBytes-1),

		`COMMENT ON CONSTRAINT valid_linked_ordinals ON fil_common_base.datablocks IS 'Eventually this constraint will be replaced with https://www.postgresql-archive.org/GSoC-2017-Foreign-Key-Arrays-tp5962835p6175483.html'`,

		// this table is *MASSIVE*: limit use of TOAST-storage, disable/discourage compression where it makes no sense
		`ALTER TABLE fil_common_base.datablocks SET ( toast_tuple_target = 8160 )`,
		`ALTER TABLE fil_common_base.datablocks ALTER COLUMN cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.datablocks ALTER COLUMN cid_tail SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.datablocks ALTER COLUMN linked_ordinals SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.datablocks ALTER COLUMN content SET STORAGE EXTERNAL`,
		`CREATE UNIQUE INDEX IF NOT EXISTS datablocks_cid_key_extended ON fil_common_base.datablocks ( cid ) INCLUDE ( block_ordinal, size )`,
		`ALTER TABLE fil_common_base.datablocks ADD UNIQUE USING INDEX datablocks_cid_key_extended`,
		`CREATE INDEX IF NOT EXISTS datablocks_cid_tail_idx ON fil_common_base.datablocks ( cid_tail )`,
		`CREATE INDEX IF NOT EXISTS datablocks_missing_blocks_idx ON fil_common_base.datablocks ( block_ordinal ) WHERE size IS NULL`,
		`CREATE INDEX IF NOT EXISTS datablocks_links_pending_idx ON fil_common_base.datablocks ( block_ordinal ) WHERE linked_ordinals IS NULL AND size IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS datablocks_subdag_maxdepth_idx ON fil_common_base.datablocks ( subdag_maxdepth )`,
		`CREATE INDEX IF NOT EXISTS datablocks_reverse_links_gin_idx ON fil_common_base.datablocks USING GIN ( linked_ordinals )`,

		`
		CREATE OR REPLACE VIEW fil_sysinfo.missing_referenced_blocks AS
			SELECT block_ordinal, cid
				FROM fil_common_base.datablocks
			WHERE
				size IS NULL
				--		AND
				--	SUBSTRING( cid FROM 1 FOR 4 ) != '\x0181e203'	-- CommD/P ( baga... )
				--		AND
				--	SUBSTRING( cid FROM 1 FOR 4 ) != '\x0182e203'	-- CommR   ( bagb... )
		`,

		//
		// basic chain section
		//
		`
		CREATE TABLE IF NOT EXISTS fil_common_base.states(
			state_ordinal INTEGER NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY, -- for more compact relationships
			epoch INTEGER NOT NULL CONSTRAINT valid_epoch CHECK ( epoch >= 0 ),
			weight NUMERIC NOT NULL,
			basefee NUMERIC NOT NULL,
			stateroot_cid BYTEA NOT NULL UNIQUE REFERENCES fil_common_base.datablocks( cid ),
			applied_tipset_cids BYTEA[] NOT NULL, -- i.e. "parent tipset"
			applied_messages_receipts_cid BYTEA NOT NULL REFERENCES fil_common_base.datablocks( cid ),
			next_beacon_round_and_data BYTEA[] NOT NULL,

			-- eventually this one will be replaced with https://www.postgresql-archive.org/GSoC-2017-Foreign-Key-Arrays-tp5962835p6175483.html
			-- FK will have to be against fil_common_base.datablocks: the parent tipset might be missing
			CONSTRAINT valid_parent_tipset CHECK (
				NOT ( '' = ANY( applied_tipset_cids ) )
					AND
				fil_common_base.array_elements_are_distinct_non_null( applied_tipset_cids )
			)
		)
		`,
		`CREATE INDEX IF NOT EXISTS states_epoch_idx ON fil_common_base.states ( epoch )`,
		`CREATE INDEX IF NOT EXISTS states_basefee_idx ON fil_common_base.states ( basefee )`,
		`CREATE INDEX IF NOT EXISTS states_parent_tipset_idx ON fil_common_base.states USING GIN ( applied_tipset_cids )`,
		// limit use of TOAST-storage
		`ALTER TABLE fil_common_base.states SET ( toast_tuple_target = 8160 )`,
		`ALTER TABLE fil_common_base.states ALTER COLUMN weight SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.states ALTER COLUMN basefee SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.states ALTER COLUMN stateroot_cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.states ALTER COLUMN applied_tipset_cids SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.states ALTER COLUMN applied_messages_receipts_cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.states ALTER COLUMN next_beacon_round_and_data SET STORAGE MAIN`,

		`
		CREATE TABLE IF NOT EXISTS fil_common_base.tipsets(
			tipset_ordinal INTEGER NOT NULL UNIQUE GENERATED ALWAYS AS IDENTITY, -- for more compact relationships
			epoch INTEGER NOT NULL CONSTRAINT valid_epoch CHECK ( epoch >= 0 ),
			parent_stateroot_cid BYTEA NOT NULL REFERENCES fil_common_base.states ( stateroot_cid ) DEFERRABLE INITIALLY DEFERRED, -- defer because we insert the tipset first,and based on its existence insert the state
			tipset_cids BYTEA[] NOT NULL UNIQUE,
			tipset_key TEXT[] NOT NULL UNIQUE, -- identical to tipset_cids, as b32 for easy querying

			-- eventually this one will be replaced with https://www.postgresql-archive.org/GSoC-2017-Foreign-Key-Arrays-tp5962835p6175483.html
			-- FK will be against fil_common_base.chainblocks
			CONSTRAINT valid_tipset_cids CHECK (
				NOT ( '' = ANY( tipset_cids ) )
					AND
				fil_common_base.array_elements_are_distinct_non_null( tipset_cids )
			),
			CONSTRAINT valid_tipset_key CHECK (
				NOT ( '' = ANY( tipset_key ) )
					AND
				fil_common_base.array_elements_are_distinct_non_null( tipset_key )
			)
		)
		`,

		`CREATE INDEX IF NOT EXISTS tipsets_epoch_idx ON fil_common_base.tipsets ( epoch )`,
		`CREATE INDEX IF NOT EXISTS tipsets_cids_idx ON fil_common_base.tipsets USING GIN ( tipset_cids )`,
		// limit use of TOAST-storage
		`ALTER TABLE fil_common_base.tipsets SET ( toast_tuple_target = 8160 )`,
		`ALTER TABLE fil_common_base.tipsets ALTER COLUMN parent_stateroot_cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.tipsets ALTER COLUMN tipset_cids SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.tipsets ALTER COLUMN tipset_key SET STORAGE MAIN`,

		`
		CREATE TABLE IF NOT EXISTS fil_common_base.chainblocks(
			miner_actor_id BIGINT NOT NULL,
			epoch INTEGER NOT NULL CONSTRAINT valid_epoch CHECK ( epoch >= 0 ),
			chainblock_cid BYTEA NOT NULL UNIQUE REFERENCES fil_common_base.datablocks (cid),
			parent_stateroot_cid BYTEA NOT NULL REFERENCES fil_common_base.states (stateroot_cid),
			messages_cid BYTEA NOT NULL REFERENCES fil_common_base.datablocks (cid),
			typed_signature BYTEA NOT NULL,
			ticket_proof BYTEA NOT NULL,
			fork_signalling_varint BYTEA NOT NULL,
			election_wincount_and_proof BYTEA NOT NULL,
			winpost_types_and_proofs BYTEA[] NOT NULL
		)
		`,
		`CREATE INDEX IF NOT EXISTS chainblocks_epoch_idx ON fil_common_base.chainblocks ( epoch )`,
		`CREATE INDEX IF NOT EXISTS chainblocks_miner_actid_idx ON fil_common_base.chainblocks ( miner_actor_id )`,
		// limit use of TOAST-storage
		`ALTER TABLE fil_common_base.chainblocks SET ( toast_tuple_target = 8160 )`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN chainblock_cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN parent_stateroot_cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN messages_cid SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN typed_signature SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN ticket_proof SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN fork_signalling_varint SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN election_wincount_and_proof SET STORAGE MAIN`,
		`ALTER TABLE fil_common_base.chainblocks ALTER COLUMN winpost_types_and_proofs SET STORAGE MAIN`,

		`
		CREATE OR REPLACE VIEW fil_sysinfo.dangling_states AS
			SELECT st.stateroot_cid, st.epoch
				FROM fil_common_base.states st
				LEFT JOIN fil_common_base.tipsets pt
					ON st.applied_tipset_cids = pt.tipset_cids
			WHERE st.epoch > 0 AND pt.tipset_cids IS NULL
		`,

		`
		CREATE OR REPLACE
			FUNCTION fil_sysinfo.storage_stats()
				RETURNS TABLE( oid OID, object_path TEXT[], cur_value NUMERIC, unit TEXT, max_value_bits SMALLINT, last_analyzed TIMESTAMP WITH TIME ZONE, data_bytes BIGINT, index_bytes BIGINT )
			LANGUAGE plpgsql STABLE PARALLEL SAFE

		AS $$
		DECLARE

			approx_avg_toast_shards INTEGER := 5; --  assume each oversize-row got broken into that many pieces (all sharing an OID), the lower this number: the more conservative the estimate
			statOIDs OID[]; -- initial list of tables to reference
			inforow RECORD;

		BEGIN

			SELECT ARRAY_AGG( pgtable.oid )
				FROM pg_namespace pgschema
				JOIN pg_class pgtable
					ON pgschema.oid = pgtable.relnamespace AND pgtable.relkind IN ( 'r', 'm', 'p' )
			WHERE
				-- ADJUST HERE to determine which table subset you care about
				pgschema.nspname LIKE 'fil_%'
			INTO statOIDs;

			max_value_bits = 32; -- both page limits and sort-of tuple-limits are max(uint32)

			FOR inforow IN (
				SELECT
						pgn.nspname AS table_schema, pgtab.relname AS table_name, pgtab.oid AS oid,
						pgtoast.relname AS toast_name, pgtoast.oid AS toast_oid,
						pgtab.relpages AS table_pages_count, pgtoast.reltuples::BIGINT AS toast_rows_approx,
						GREATEST( stab.last_analyze, stab.last_autoanalyze ) AS last_analyzed_table,
						GREATEST(
							stoast.last_analyze, stoast.last_autoanalyze, stoast.last_vacuum, stoast.last_autovacuum
						) AS last_analyzed_toast
					FROM pg_class pgtab
					JOIN pg_namespace pgn
						ON pgtab.relnamespace = pgn.oid AND pgtab.oid = ANY( statOIDs )
					LEFT JOIN pg_stat_user_tables stab
						ON pgtab.oid = stab.relid
					LEFT JOIN pg_class pgtoast
						ON pgtab.reltoastrelid = pgtoast.oid
					LEFT JOIN pg_stat_sys_tables stoast
						ON pgtab.reltoastrelid = stoast.relid
				ORDER BY table_schema, table_name, toast_name NULLS FIRST
			)
			LOOP

				oid := inforow.oid;
				object_path := ARRAY[ inforow.table_schema, inforow.table_name ];
				cur_value := inforow.table_pages_count;
				unit := 'pages_used';
				last_analyzed := inforow.last_analyzed_table;
				data_bytes := pg_table_size(inforow.oid) - COALESCE( pg_total_relation_size(inforow.toast_oid), 0 );
				index_bytes := pg_indexes_size(inforow.oid);
				RETURN NEXT;

				IF inforow.toast_oid IS NOT NULL THEN
					oid := inforow.toast_oid;
					object_path := ARRAY[ inforow.table_schema, inforow.table_name, inforow.toast_name ];
					cur_value := inforow.toast_rows_approx / approx_avg_toast_shards;
					unit := 'approx_toast_oids';
					last_analyzed := inforow.last_analyzed_toast;
					data_bytes := pg_table_size(inforow.toast_oid);
					index_bytes := pg_indexes_size(inforow.toast_oid);
					RETURN NEXT;
				END IF;

			END LOOP;

			-- identical for the remainder
			oid := NULL;
			unit := 'max_value';
			last_analyzed := NULL;
			data_bytes := NULL;
			index_bytes := NULL;

			FOR inforow IN (
				SELECT table_schema, table_name, column_name, numeric_precision
					FROM information_schema.columns isc
				WHERE
					numeric_precision IS NOT NULL AND numeric_scale = 0 AND numeric_precision_radix = 2
						AND
					EXISTS(
						SELECT 42
							FROM pg_class t
							JOIN pg_namespace n
								ON t.relnamespace = n.oid AND t.oid = ANY( statOIDs )
							JOIN pg_index ix
								ON t.oid = ix.indrelid AND ix.indisvalid AND ix.indislive
							JOIN pg_class ixc
								ON ix.indexrelid = ixc.oid
							JOIN pg_am iam
								ON ixc.relam = iam.oid
							JOIN pg_attribute a
								ON t.oid = a.attrelid
						WHERE
							n.nspname = isc.table_schema AND t.relname = isc.table_name AND a.attname = isc.column_name
								AND
							-- only consider single-key predicate-less ordered indices that can serve a MAX quickly
							a.attnum = ANY(ix.indkey) AND iam.amname IN ( 'btree' ) AND ix.indnkeyatts = 1 AND ix.indpred IS NULL
					)
				ORDER BY table_schema, table_name, column_name
			)
			LOOP
				EXECUTE CONCAT(
					'SELECT MAX(', QUOTE_IDENT(inforow.column_name), ')
						FROM ', QUOTE_IDENT(inforow.table_schema), '.', QUOTE_IDENT(inforow.table_name)
				) INTO cur_value;
				object_path := ARRAY[ inforow.table_schema, inforow.table_name, inforow.column_name ];
				max_value_bits = inforow.numeric_precision-1; -- subtract 1 due to signedness

				RETURN NEXT;

			END LOOP;

		END $$
		`,

		`
		CREATE OR REPLACE VIEW fil_sysinfo.storage_stats AS
			SELECT
				oid,
				object_path,
				cur_value,
				unit,
				'2 ^ '||max_value_bits||' - 1' AS sys_limit,
				( cur_value::NUMERIC * 100 / ( 2::NUMERIC ^ max_value_bits - 1 ) )::NUMERIC(5,2) AS pct_used,
				pg_size_pretty(data_bytes) AS data_size,
				pg_size_pretty(index_bytes) AS index_size
			FROM fil_sysinfo.storage_stats()
		WHERE object_path[ARRAY_LOWER(object_path, 1)] NOT LIKE '%_accesslog'
		`,

		`
		DO $$
			BEGIN
				IF NOT EXISTS (SELECT 42 FROM pg_tables WHERE schemaname = 'fil_sysinfo' AND tablename = 'schema_metadata') THEN
					CREATE TABLE fil_sysinfo.schema_metadata(
						singleton_row BOOL NOT NULL UNIQUE CONSTRAINT single_row_in_table CHECK ( singleton_row IS TRUE ),
						metadata JSONB NOT NULL
					);
					INSERT INTO fil_sysinfo.schema_metadata ( singleton_row, metadata ) VALUES ( true, '{ "schemaVersionMajor": 1, "schemaVersionMinor": 0 }' );
				END IF;
		END $$
		`,
	}
}

func (dbbs *PgBlockstore) namespacedDDL() []string {

	accesslogSchema := dbbs.instanceNamespace + "_accesslog"

	ddl := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, dbbs.instanceNamespace),
		fmt.Sprintf(`SET LOCAL search_path TO %s`, dbbs.instanceNamespace),

		`
		CREATE TABLE IF NOT EXISTS datablocks_recent(
			block_ordinal BIGINT NOT NULL UNIQUE, -- no FK, this table is high velocity, and inconsistencies are inconsequential
			last_access_epoch INTEGER NOT NULL CONSTRAINT valid_last_access CHECK ( last_access_epoch >= 0 )
		)
		`,
		`CREATE INDEX IF NOT EXISTS datablocks_recent_last_access_epoch_idx ON datablocks_recent ( last_access_epoch )`,

		`
		CREATE TABLE IF NOT EXISTS tipsets_visited(
			visit_wall_time TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
			tipset_ordinal INTEGER NOT NULL REFERENCES fil_common_base.tipsets ( tipset_ordinal ),
			is_current_head BOOL UNIQUE CONSTRAINT singleton_current CHECK ( is_current_head IS NULL OR is_current_head IS TRUE )
		)
		`,

		// FIXME - this may need to be adjusted to do actual weight analysis
		// instead of relying on is_current_head (which is noisy)
		// We *do* have sufficient data within the database to make this decision
		// without involving lotus, but it will likely be rather expensive
		//
		// Instead a cop-out: always return the tipset of the *previous* epoch
		`
		CREATE OR REPLACE VIEW current_head AS
			SELECT
					pt.tipset_key, pt.tipset_cids, pt.epoch, pt.parent_stateroot_cid,
					ps.basefee AS parent_state_basefee, ps.weight AS parent_state_weight
				FROM tipsets_visited tv
				JOIN fil_common_base.tipsets t
					ON tv.is_current_head AND tv.tipset_ordinal = t.tipset_ordinal
				JOIN fil_common_base.states s
					ON t.parent_stateroot_cid = s.stateroot_cid
				JOIN fil_common_base.tipsets pt
					ON s.applied_tipset_cids = pt.tipset_cids
				JOIN fil_common_base.states ps
					ON pt.parent_stateroot_cid = ps.stateroot_cid
		`,

		`
		CREATE TABLE IF NOT EXISTS states_orphaned(
			state_ordinal INTEGER NOT NULL UNIQUE REFERENCES fil_common_base.states( state_ordinal )
		)
		`,

		//
		// ctime/atime tracking
		//
		`
		CREATE TABLE IF NOT EXISTS datablocks_accesslog(
			block_ordinal BIGINT NOT NULL CONSTRAINT valid_block_ordinal CHECK ( block_ordinal > 0 ), -- No FK to datablocks here - we might end up storing the log *before* the blocks have been concurrently committed
			access_type SMALLINT NOT NULL,
			access_count BIGINT NOT NULL CONSTRAINT access_count_value CHECK ( access_count > 0 ),
			access_wall_time TIMESTAMP WITH TIME ZONE NOT NULL,
			context_epoch INTEGER NOT NULL CONSTRAINT valid_epoch CHECK ( context_epoch >= 0 ),
			context_tipset_ordinal INTEGER REFERENCES fil_common_base.tipsets (tipset_ordinal)
		) PARTITION BY RANGE ( context_epoch )
		`,
		`CREATE INDEX IF NOT EXISTS datablocks_accesslog_wall_time_idx ON datablocks_accesslog USING BRIN ( access_wall_time ) WITH ( pages_per_range = 1 )`,
		`CREATE INDEX IF NOT EXISTS datablocks_accesslog_context_epoch_idx ON datablocks_accesslog USING BRIN ( context_epoch ) WITH ( pages_per_range = 1 )`,

		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, accesslogSchema),
	}

	// Partition the accesslogs by a week each
	partitionStep := int(EpochsInHour * 24 * 7)

	// Provision 100 weeks to start
	for w := 1; w < 100; w++ {

		ddl = append(ddl, fmt.Sprintf(
			`
			CREATE TABLE IF NOT EXISTS %s.week_%03d
				PARTITION OF %s.datablocks_accesslog
				FOR VALUES FROM (%d) TO (%d)
			`,
			accesslogSchema,
			w,
			dbbs.instanceNamespace,
			(w-1)*partitionStep, w*partitionStep,
		))
	}

	return ddl
}

func (dbbs *PgBlockstore) neededDeployDDL(ctx context.Context) ([]string, error) {

	var missingCommon, missingNamespaced bool

	err := dbbs.dbPool.QueryRow(ctx, `SELECT 42 FROM pg_tables WHERE schemaname = 'fil_sysinfo' AND tablename = 'schema_metadata'`).Scan(new(int))

	switch {

	// no version table: nothing is deployed
	case err == pgx.ErrNoRows:
		missingCommon = true
		missingNamespaced = true

	case err != nil:
		return nil, err

	default:

		// version table is present: pull the value
		var version int64
		err := dbbs.dbPool.QueryRow(ctx, `SELECT metadata->'schemaVersionMajor' FROM fil_sysinfo.schema_metadata`).Scan(&version)

		switch {

		case err == pgx.ErrNoRows:
			// singleton row is not present, assume nothing is deployed
			missingCommon = true
			missingNamespaced = true

		case err != nil:
			return nil, err

		case version != 1:
			return nil, fmt.Errorf("TODO: unexpected existing schema version '%d', only '1' is currently understood", version)

		case dbbs.instanceNamespace == "":
			// nothing further to do

		default:
			// version found, no errors, and it *is* as expected
			// check if the namespaced part is present
			err = dbbs.dbPool.QueryRow(
				ctx,
				`SELECT 42 FROM pg_views WHERE schemaname = $1 AND viewname = 'current_head'`,
				dbbs.instanceNamespace,
			).Scan(new(int))

			switch {

			case err == pgx.ErrNoRows:
				missingNamespaced = true

			case err != nil:
				return nil, err
			}
		}
	}

	ddl := make([]string, 0, 128)
	if missingCommon {
		ddl = append(ddl, dbbs.commonDDL()...)
	}
	if missingNamespaced {
		ddl = append(ddl, dbbs.namespacedDDL()...)
	}

	if len(ddl) == 0 {
		return nil, nil
	}

	return ddl, nil
}

func (dbbs *PgBlockstore) deploy(ctx context.Context, ddl []string) (err error) {

	tx, err := dbbs.dbPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	defer func() {
		if err != nil && tx != nil {
			tx.Rollback(ctx)
		}
	}()
	if err != nil {
		return xerrors.Errorf("failed to start transaction: %w", err)
	}

	for _, statement := range ddl {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return xerrors.Errorf("deploy DDL execution failed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return xerrors.Errorf("failed commit of deploy transaction: %w", err)
	}

	return nil
}
