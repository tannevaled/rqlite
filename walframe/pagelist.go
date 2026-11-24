package walframe

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// Tx is the logical content of one committed SQLite transaction: page
// numbers and page images, the last being the commit frame, plus the size of
// the database in pages after the commit. It is what dqlite ships.
type Tx struct {
	Pgnos  []uint32
	Pages  [][]byte
	DBSize uint32
}

// HashSize is the size of the base hash and of the entry checksum.
const HashSize = sha256.Size

// PageList is the payload of one Raft entry: the transactions one request
// committed on the Leader, and the BASE they were computed on: the number of
// page lists the Leader's database had applied (BaseSeq) and the hash chain
// over their IDs (BaseHash). The FSM refuses, on every node alike, a list whose
// base is not the node's current state (see Engine.Commit).
type PageList struct {
	PageSize uint32
	BaseSeq  uint64
	BaseHash [HashSize]byte
	Txs      []Tx
}

const (
	pageListMagic = "WFL2"
	idSize        = 16

	// CmdPages is the command type of a PageList entry.
	cmdPages byte = 1

	pageListHeaderSize = 4 + 4 + 8 + HashSize + 4
)

// ErrBadPageList is returned for any malformed encoding.
var ErrBadPageList = errors.New("walframe: malformed page list")

// MarshalPageList encodes pl. Layout, all integers big endian:
//
//	"WFL2" | pageSize u32 | baseSeq u64 | baseHash [32] | nTx u32 | nTx * ( dbSize u32 | nPages u32 | nPages * ( pgno u32 | page ) )
func MarshalPageList(pl *PageList) ([]byte, error) {
	if !validPageSize(pl.PageSize) {
		return nil, fmt.Errorf("%w: page size %d", ErrBadPageList, pl.PageSize)
	}
	if len(pl.Txs) == 0 {
		return nil, fmt.Errorf("%w: no transaction", ErrBadPageList)
	}
	for _, tx := range pl.Txs {
		if len(tx.Pgnos) != len(tx.Pages) || len(tx.Pgnos) == 0 {
			return nil, fmt.Errorf("%w: transaction with %d page numbers and %d pages", ErrBadPageList, len(tx.Pgnos), len(tx.Pages))
		}
		for _, pg := range tx.Pages {
			if len(pg) != int(pl.PageSize) {
				return nil, fmt.Errorf("%w: page of %d bytes", ErrBadPageList, len(pg))
			}
		}
	}
	return encodePageList(pl), nil
}

// encodedSize is the exact size of encodePageList(pl).
func encodedSize(pl *PageList) int {
	n := pageListHeaderSize
	for _, tx := range pl.Txs {
		n += 8 + len(tx.Pgnos)*(4+int(pl.PageSize))
	}
	return n
}

// encodePageList is MarshalPageList for a list the caller built itself (the
// engine's, from frames it just scanned): no validation.
func encodePageList(pl *PageList) []byte {
	b := make([]byte, 0, encodedSize(pl))
	b = append(b, pageListMagic...)
	b = binary.BigEndian.AppendUint32(b, pl.PageSize)
	b = binary.BigEndian.AppendUint64(b, pl.BaseSeq)
	b = append(b, pl.BaseHash[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(pl.Txs)))
	for _, tx := range pl.Txs {
		b = binary.BigEndian.AppendUint32(b, tx.DBSize)
		b = binary.BigEndian.AppendUint32(b, uint32(len(tx.Pgnos)))
		for i, pg := range tx.Pages {
			b = binary.BigEndian.AppendUint32(b, tx.Pgnos[i])
			b = append(b, pg...)
		}
	}
	return b
}

// UnmarshalPageList decodes b. It never allocates more than len(b) bytes for
// pages, and rejects trailing bytes, zero page numbers, zero database sizes,
// lists with no transaction and counts the input cannot hold.
func UnmarshalPageList(b []byte) (*PageList, error) {
	bad := func(why string) (*PageList, error) { return nil, fmt.Errorf("%w: %s", ErrBadPageList, why) }
	if len(b) < pageListHeaderSize || string(b[:4]) != pageListMagic {
		return bad("header")
	}
	ps := binary.BigEndian.Uint32(b[4:])
	if !validPageSize(ps) {
		return bad("page size")
	}
	pl := &PageList{PageSize: ps, BaseSeq: binary.BigEndian.Uint64(b[8:])} // grows only as transactions are read in full
	copy(pl.BaseHash[:], b[16:])
	nTx := binary.BigEndian.Uint32(b[16+HashSize:])
	b = b[pageListHeaderSize:]
	if nTx == 0 {
		return bad("no transaction")
	}
	for i := uint32(0); i < nTx; i++ {
		if len(b) < 8 {
			return bad("truncated transaction")
		}
		dbSize := binary.BigEndian.Uint32(b)
		nPg := binary.BigEndian.Uint32(b[4:])
		b = b[8:]
		if dbSize == 0 || nPg == 0 || uint64(nPg)*(4+uint64(ps)) > uint64(len(b)) {
			return bad("transaction")
		}
		tx := Tx{DBSize: dbSize, Pgnos: make([]uint32, nPg), Pages: make([][]byte, nPg)}
		for j := range tx.Pgnos {
			pg := binary.BigEndian.Uint32(b)
			if pg == 0 {
				return bad("page number")
			}
			tx.Pgnos[j] = pg
			tx.Pages[j] = b[4 : 4+ps : 4+ps] // aliases the input, which the caller keeps
			b = b[4+ps:]
		}
		pl.Txs = append(pl.Txs, tx)
	}
	if len(b) != 0 {
		return bad("trailing bytes")
	}
	return pl, nil
}

// A command is a Raft log entry: a type byte, the 16-byte ID the Leader gave
// the request (so the Leader's own FSM recognises its entry and does not
// apply it a second time), a PageList, and a SHA-256 of everything before it.
// The checksum is verified before anything is decoded and travels with the
// entry end to end: the follower never re-checksums damaged data.
func marshalCommand(id [idSize]byte, pl *PageList) []byte {
	b := make([]byte, 0, 1+idSize+encodedSize(pl)+HashSize)
	b = append(b, cmdPages)
	b = append(b, id[:]...)
	b = append(b, encodePageList(pl)...)
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

func unmarshalCommand(b []byte) (id [idSize]byte, pl *PageList, err error) {
	if len(b) < 1+idSize+HashSize || b[0] != cmdPages {
		return id, nil, fmt.Errorf("%w: command", ErrBadPageList)
	}
	body, trailer := b[:len(b)-HashSize], b[len(b)-HashSize:]
	if sum := sha256.Sum256(body); string(sum[:]) != string(trailer) {
		return id, nil, fmt.Errorf("%w: checksum mismatch", ErrBadPageList)
	}
	copy(id[:], body[1:])
	pl, err = UnmarshalPageList(body[1+idSize:])
	return id, pl, err
}

// chainHash is the base hash after the page list entry id: sha256(prev || id).
func chainHash(prev [HashSize]byte, id [idSize]byte) [HashSize]byte {
	h := sha256.New()
	h.Write(prev[:])
	h.Write(id[:])
	var out [HashSize]byte
	h.Sum(out[:0])
	return out
}
