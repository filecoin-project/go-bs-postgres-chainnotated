package pgchainbs

import (
	"regexp"
	"runtime"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

// Filecoin Mainnet constants redefined here to avoid import cycles.
const (
	EpochDurationSeconds = 30
	EpochsInHour         = abi.ChainEpoch(60 * 60 / EpochDurationSeconds)
	ChainFinality        = abi.ChainEpoch(900)
)

// IgnoreLinksToCodecs is a map of codecs which are considered to "not exist"
// during in-RDBMS link storage.
var IgnoreLinksToCodecs = map[uint64]struct{}{
	cid.FilCommitmentUnsealed: {},
	cid.FilCommitmentSealed:   {},
}

var (
	MaxPgxPoolSize                  = int32(32)             // the maximum size of the connection pool (default scales dynamically based on NumCPU)
	TrackRecentTipsetsCount         = 25 * EpochsInHour     // determines how many Filecoin epochs in the past do we consider a block to be "recently-accessed"
	MaxFutureEpochsAsRecent         = 3 * 24 * EpochsInHour // When backtracking over long distances, prune "recent" entries that end up being that far into the future
	MaxOrphanLookback               = 1 + ChainFinality     // when keeping track of orphaned states, how far back do we want to walk
	AssumeUserNeverMutatesBlockdata = true                  // for performance mimic the behavior of github.com/ipfs/go-block-format regarding data ownership and copying
)

const (
	DefaultLruCacheSize           = int64(32 << 30)                                                      // the default LRU cache size in bytes, overridable in PgBlockstoreConfig
	BulkFetchSliceSize            = 32768                                                                // how many blocks to fetch at once from various RDBMS sets
	ObjectExLockStatement         = `SELECT PG_ADVISORY_XACT_LOCK( 140000, TO_REGCLASS( $1 )::INTEGER )` // complete exclusive-lock-by-named-object statement
	PgLockOidVector               = 140000                                                               // the high-32bit value of 140000 is arbitrarily chosen
	StoredBlocksInflatorSelection = `cid, block_ordinal, size, content_encoding, content`                // ordered set of columns expected by InflateDbRows
)

const (
	concurrentDetailedLogWriters = 8

	// For github.com/valyala/gozstd
	zstdCompressLevel = 11
)

var (
	log            = logging.Logger("chainnotated-blockstore")
	validNamespace = regexp.MustCompile(`^[a-z][a-z_0-9]*$`)

	emptyLinkage = make([]*int64, 0)

	// https://tools.ietf.org/html/rfc8478#section-3.1.1
	zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

	concurrentBlockProcessors = runtime.NumCPU()           // how many workers to compress/parse/etc on write
	backgroundFetchWorkers    = 1 + (runtime.NumCPU() / 4) // cranking this higher results in too much contention
)
