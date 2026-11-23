// Package walframe is a PROTOTYPE of Raft replication of SQLite WAL frames
// (physical pages, dqlite style) over the pure-Go modernc.org/sqlite driver.
// See doc.go for the design and its limits. It is not for production.
package walframe

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Sizes and magic numbers of the WAL format, https://sqlite.org/fileformat2.html#walformat.
const (
	walHeaderSize   = 32
	frameHeaderSize = 24
	walMagicLE      = 0x377f0682
	walMagicBE      = 0x377f0683
)

type walHeader struct {
	Magic, Version, PageSize, CkptSeq uint32
	Salt1, Salt2, Cksum1, Cksum2      uint32
}

type frame struct {
	Offset int64  // offset of the frame header
	Pgno   uint32 // page number
	Commit uint32 // database size in pages after commit, 0 if not a commit frame
	C1, C2 uint32 // cumulative checksum after this frame
}

// walScan is SQLite's recovery view of a WAL image: the frames a connection
// opening this file would accept, up to the LAST COMMIT FRAME.
type walScan struct {
	Header    walHeader
	Valid     []frame
	CommitEnd int64 // byte offset just past the last valid commit frame
}

func cksum(bigEnd bool, b []byte, s1, s2 uint32) (uint32, uint32) {
	order := binary.ByteOrder(binary.LittleEndian)
	if bigEnd {
		order = binary.BigEndian
	}
	for i := 0; i+8 <= len(b); i += 8 {
		s1 += order.Uint32(b[i:]) + s2
		s2 += order.Uint32(b[i+4:]) + s1
	}
	return s1, s2
}

var errNoHeader = errors.New("wal: no valid header")

// validPageSize reports whether n is a page size SQLite accepts.
func validPageSize(n uint32) bool {
	return n >= 512 && n <= 65536 && n&(n-1) == 0
}

// scanWAL parses a WAL image the way sqlite3's walIndexRecover does.
func scanWAL(b []byte) (walScan, error) {
	var s walScan
	if len(b) < walHeaderSize {
		return s, errNoHeader
	}
	be := binary.BigEndian
	h := walHeader{
		Magic: be.Uint32(b[0:]), Version: be.Uint32(b[4:]), PageSize: be.Uint32(b[8:]),
		CkptSeq: be.Uint32(b[12:]), Salt1: be.Uint32(b[16:]), Salt2: be.Uint32(b[20:]),
		Cksum1: be.Uint32(b[24:]), Cksum2: be.Uint32(b[28:]),
	}
	if h.Magic != walMagicLE && h.Magic != walMagicBE {
		return s, errNoHeader
	}
	if !validPageSize(h.PageSize) {
		return s, fmt.Errorf("wal: bad page size %d", h.PageSize)
	}
	bigEnd := h.Magic&1 == 1
	c1, c2 := cksum(bigEnd, b[:24], 0, 0)
	if c1 != h.Cksum1 || c2 != h.Cksum2 {
		return s, errors.New("wal: header checksum mismatch")
	}
	s.Header = h
	s.CommitEnd = walHeaderSize
	ps := int64(h.PageSize)
	fsz := frameHeaderSize + ps
	var pending []frame
	for off := int64(walHeaderSize); off+fsz <= int64(len(b)); off += fsz {
		fh := b[off : off+frameHeaderSize]
		if be.Uint32(fh[8:]) != h.Salt1 || be.Uint32(fh[12:]) != h.Salt2 {
			break
		}
		c1, c2 = cksum(bigEnd, fh[:8], c1, c2)
		c1, c2 = cksum(bigEnd, b[off+frameHeaderSize:off+fsz], c1, c2)
		if c1 != be.Uint32(fh[16:]) || c2 != be.Uint32(fh[20:]) {
			break
		}
		pending = append(pending, frame{Offset: off, Pgno: be.Uint32(fh[0:]), Commit: be.Uint32(fh[4:]), C1: c1, C2: c2})
		if pending[len(pending)-1].Commit != 0 {
			s.Valid = append(s.Valid, pending...)
			pending = pending[:0]
			s.CommitEnd = off + fsz
		}
	}
	return s, nil
}

// extractTxs returns the committed transactions found in wal between byte
// offsets [from, to), to being a commit boundary.
func extractTxs(wal []byte, s walScan, from, to int64) []Tx {
	ps := int64(s.Header.PageSize)
	var out []Tx
	var cur Tx
	for _, f := range s.Valid {
		if f.Offset < from || f.Offset >= to {
			continue
		}
		pg := make([]byte, ps)
		copy(pg, wal[f.Offset+frameHeaderSize:f.Offset+frameHeaderSize+ps])
		cur.Pgnos = append(cur.Pgnos, f.Pgno)
		cur.Pages = append(cur.Pages, pg)
		if f.Commit != 0 {
			cur.DBSize = f.Commit
			out = append(out, cur)
			cur = Tx{}
		}
	}
	return out
}

// appendTxs re-frames txs at the end of the valid part of a WAL image (dqlite's
// vfsWalAppend): own salts, own checksum chain. A missing or empty WAL gets a
// fresh header with the given salts.
func appendTxs(wal []byte, pageSize uint32, txs []Tx, salt [2]uint32) []byte {
	be := binary.BigEndian
	s, err := scanWAL(wal)
	if err != nil {
		hdr := make([]byte, walHeaderSize)
		be.PutUint32(hdr[0:], walMagicBE)
		be.PutUint32(hdr[4:], 3007000)
		be.PutUint32(hdr[8:], pageSize)
		be.PutUint32(hdr[16:], salt[0])
		be.PutUint32(hdr[20:], salt[1])
		c1, c2 := cksum(true, hdr[:24], 0, 0)
		be.PutUint32(hdr[24:], c1)
		be.PutUint32(hdr[28:], c2)
		wal = hdr
		s, _ = scanWAL(wal)
	}
	wal = wal[:s.CommitEnd]
	bigEnd := s.Header.Magic&1 == 1
	c1, c2 := s.Header.Cksum1, s.Header.Cksum2
	if n := len(s.Valid); n > 0 {
		c1, c2 = s.Valid[n-1].C1, s.Valid[n-1].C2
	}
	for _, tx := range txs {
		for i, pg := range tx.Pages {
			fh := make([]byte, frameHeaderSize)
			be.PutUint32(fh[0:], tx.Pgnos[i])
			if i == len(tx.Pages)-1 {
				be.PutUint32(fh[4:], tx.DBSize)
			}
			be.PutUint32(fh[8:], s.Header.Salt1)
			be.PutUint32(fh[12:], s.Header.Salt2)
			c1, c2 = cksum(bigEnd, fh[:8], c1, c2)
			c1, c2 = cksum(bigEnd, pg, c1, c2)
			be.PutUint32(fh[16:], c1)
			be.PutUint32(fh[20:], c2)
			wal = append(wal, fh...)
			wal = append(wal, pg...)
		}
	}
	return wal
}
