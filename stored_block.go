package pgchainbs

import (
	"bytes"
	"fmt"
	"sync"

	ipfsblock "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cborgen "github.com/whyrusleeping/cbor-gen"

	"github.com/valyala/gozstd"
	"golang.org/x/xerrors"
)

// StoredBlock implements "github.com/ipfs/go-block-format".Block()
// along with a number of blockstore-specific functionalities. There is no
// constructor: you get a StoredBlock object via either
// *PgBlockstore.GetStoredBlock or *PgBlockstore.InflateAndCloseDbRows
type StoredBlock struct {
	size              int32
	cidValidated      bool
	cid               cid.Cid
	dbBlockOrdinal    *int64
	dbContentEncoding *int16
	dbDeflatedContent []byte
	blockContent      []byte
	mu                sync.Mutex
	errHolder         error
}

// assert we can Block()
var _ ipfsblock.Block = &StoredBlock{}

func (sb *StoredBlock) Cid() cid.Cid   { return sb.cid }
func (sb *StoredBlock) String() string { return fmt.Sprintf("[Block %s]", sb.Cid().String()) }
func (sb *StoredBlock) Loggable() map[string]interface{} {
	return map[string]interface{}{"block": sb.Cid().String()}
}

// Error returns a stored error-condition if any.
// A block in such a condition will always return nil for RawData().
func (sb *StoredBlock) Error() error { sb.mu.Lock(); defer sb.mu.Unlock(); return sb.errHolder }

// DbBlockOrdinal returns the value of the RDBMS-side autoincrement-ID. For
// obvious reasons this ID is valid exclusively within the current RDBMS
// instance. Use CIDs whenever possible (unless sheer scale dictates otherwise)
func (sb *StoredBlock) DbBlockOrdinal() *int64 { return sb.dbBlockOrdinal }

// RawData returns the block data payload. When AssumeUserNeverMutatesBlockdata
// is true (the default) the data-slice is returned as-is, without copying
func (sb *StoredBlock) RawData() (data []byte) {

	var err error
	data, err = sb.inflatedContent(false)
	if err != nil {
		return nil
	}

	if !AssumeUserNeverMutatesBlockdata {
		data = append(
			make([]byte, 0, len(data)),
			data...,
		)
	}

	return
}

// This is the central access point for the content, which is unpacked here if needed
func (sb *StoredBlock) inflatedContent(skipCidValidation bool) ([]byte, error) {
	sb.mu.Lock()
	defer func() {
		if sb.errHolder != nil {
			log.Errorf("UNEXPECTED failure deriving data for block %s: %s", sb.cid.String(), sb.errHolder)
		}
		sb.mu.Unlock()
	}()

	if sb.errHolder == nil && sb.blockContent == nil {

		if *sb.dbContentEncoding != 1 {
			sb.errHolder = fmt.Errorf("unexpected dbContentEncoding '%d' during hydration", *sb.dbContentEncoding)
		} else if sb.blockContent, sb.errHolder = gozstd.Decompress(
			make([]byte, 0, sb.size),
			sb.dbDeflatedContent,
		); sb.errHolder != nil {
			sb.errHolder = xerrors.Errorf("zstd decompression failed: %w", sb.errHolder)
		}

		sb.dbDeflatedContent = nil
	}

	if !skipCidValidation && !sb.cidValidated && sb.errHolder == nil {
		var recalcCid cid.Cid
		if recalcCid, sb.errHolder = sb.cid.Prefix().Sum(sb.blockContent); sb.errHolder == nil {
			if recalcCid.Equals(sb.cid) {
				sb.cidValidated = true
			} else {
				sb.errHolder = blockstore.ErrHashMismatch
			}
		}
	}

	if sb.errHolder != nil {
		sb.blockContent = nil
	}

	return sb.blockContent, sb.errHolder
}

// extractLinks returns a list of unique CIDs linked from the block (if any)
// The list is always in first-seen order
func (sb *StoredBlock) extractLinks() ([]cid.Cid, error) {

	codec := sb.cid.Prefix().Codec

	switch codec {

	case cid.Raw:
		return nil, nil

	case cid.DagCBOR:

		data, err := sb.inflatedContent(false)
		if err != nil {
			return nil, err
		}

		/* 64 below comes from:
			SELECT 8*(-1+WIDTH_BUCKET( CARDINALITY( linked_ordinals ), 0, 128, 16 )) AS no_fewer_than_N_links, COUNT(*)
				FROM fil_common_base.datablocks
			WHERE linked_ordinals IS NOT NULL
			GROUP BY no_fewer_than_N_links
		 	ORDER BY no_fewer_than_N_links
		*/
		ret := make([]cid.Cid, 0, 64)
		seen := cid.NewSet()

		err = cborgen.ScanForLinks(bytes.NewReader(data), func(c cid.Cid) {
			if _, isSkippable := IgnoreLinksToCodecs[c.Prefix().Codec]; !isSkippable && seen.Visit(c) {
				ret = append(ret, c)
			}
		})
		if err != nil {
			return nil, err
		}
		return ret, nil

	default:
		return nil, fmt.Errorf("not-yet-supported chain-codec 0x%X (%d)", codec, codec)
	}
}
