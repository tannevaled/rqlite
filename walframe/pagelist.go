package walframe

import (
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

// PageList is the payload of one Raft entry: the transactions one request
// committed on the Leader.
type PageList struct {
	PageSize uint32
	Txs      []Tx
}

const (
	pageListMagic = "WFL1"
	idSize        = 16

	// CmdPages is the command type of a PageList entry.
	cmdPages byte = 1
)

// ErrBadPageList is returned for any malformed encoding.
var ErrBadPageList = errors.New("walframe: malformed page list")

// MarshalPageList encodes pl. Layout, all integers big endian:
//
//	"WFL1" | pageSize u32 | nTx u32 | nTx * ( dbSize u32 | nPages u32 | nPages * ( pgno u32 | page ) )
func MarshalPageList(pl *PageList) ([]byte, error) {
	if !validPageSize(pl.PageSize) {
		return nil, fmt.Errorf("%w: page size %d", ErrBadPageList, pl.PageSize)
	}
	n := 12
	for _, tx := range pl.Txs {
		n += 8 + len(tx.Pgnos)*(4+int(pl.PageSize))
	}
	b := make([]byte, 0, n)
	b = append(b, pageListMagic...)
	b = binary.BigEndian.AppendUint32(b, pl.PageSize)
	b = binary.BigEndian.AppendUint32(b, uint32(len(pl.Txs)))
	for _, tx := range pl.Txs {
		if len(tx.Pgnos) != len(tx.Pages) || len(tx.Pgnos) == 0 {
			return nil, fmt.Errorf("%w: transaction with %d page numbers and %d pages", ErrBadPageList, len(tx.Pgnos), len(tx.Pages))
		}
		b = binary.BigEndian.AppendUint32(b, tx.DBSize)
		b = binary.BigEndian.AppendUint32(b, uint32(len(tx.Pgnos)))
		for i, pg := range tx.Pages {
			if len(pg) != int(pl.PageSize) {
				return nil, fmt.Errorf("%w: page of %d bytes", ErrBadPageList, len(pg))
			}
			b = binary.BigEndian.AppendUint32(b, tx.Pgnos[i])
			b = append(b, pg...)
		}
	}
	return b, nil
}

// UnmarshalPageList decodes b. It never allocates more than len(b) bytes for
// pages, and rejects trailing bytes, zero page numbers, zero database sizes
// and counts the input cannot hold.
func UnmarshalPageList(b []byte) (*PageList, error) {
	bad := func(why string) (*PageList, error) { return nil, fmt.Errorf("%w: %s", ErrBadPageList, why) }
	if len(b) < 12 || string(b[:4]) != pageListMagic {
		return bad("header")
	}
	ps := binary.BigEndian.Uint32(b[4:])
	if !validPageSize(ps) {
		return bad("page size")
	}
	nTx := binary.BigEndian.Uint32(b[8:])
	b = b[12:]
	if uint64(nTx)*(8+4+uint64(ps)) > uint64(len(b)) {
		return bad("transaction count")
	}
	pl := &PageList{PageSize: ps, Txs: make([]Tx, 0, nTx)}
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
			tx.Pages[j] = append([]byte(nil), b[4:4+ps]...)
			b = b[4+ps:]
		}
		pl.Txs = append(pl.Txs, tx)
	}
	if len(b) != 0 {
		return bad("trailing bytes")
	}
	return pl, nil
}

// command is a Raft log entry: a type byte, the 16-byte ID the Leader gave
// the request (so the Leader's own FSM recognises its entry and does not
// apply it a second time), and a PageList.
func marshalCommand(id [idSize]byte, pl *PageList) ([]byte, error) {
	p, err := MarshalPageList(pl)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, 1+idSize+len(p))
	b = append(b, cmdPages)
	b = append(b, id[:]...)
	return append(b, p...), nil
}

func unmarshalCommand(b []byte) (id [idSize]byte, pl *PageList, err error) {
	if len(b) < 1+idSize || b[0] != cmdPages {
		return id, nil, fmt.Errorf("%w: command", ErrBadPageList)
	}
	copy(id[:], b[1:])
	pl, err = UnmarshalPageList(b[1+idSize:])
	return id, pl, err
}
