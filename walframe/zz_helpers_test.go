package walframe

import (
	"crypto/sha256"
	"encoding/binary"
)

// based stamps pl with the engine's current base, as a Leader would.
func based(e *Engine, pl *PageList) *PageList {
	pl.BaseSeq, pl.BaseHash = e.seq, e.head
	return pl
}

// mkSnap builds a snapshot with a valid checksum around arbitrary content.
func mkSnap(seq uint64, ps, dbPages uint32, dbLen uint64, d, w []byte) []byte {
	b := []byte(snapshotMagic)
	b = binary.BigEndian.AppendUint64(b, seq)
	head := make([]byte, HashSize)
	if seq > 0 {
		head[0] = 1 // a database at a sequence has a head; zero is refused
	}
	b = append(b, head...)
	b = binary.BigEndian.AppendUint32(b, ps)
	b = binary.BigEndian.AppendUint32(b, dbPages)
	b = binary.BigEndian.AppendUint64(b, dbLen)
	b = append(b, d...)
	b = append(b, w...)
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

// asErr turns an FSM response into an error (nil when it is not one).
func asErr(v any) error {
	err, _ := v.(error)
	return err
}
