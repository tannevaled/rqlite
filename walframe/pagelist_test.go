package walframe

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func seedLists() map[string]*PageList {
	pg := func(b byte) []byte { return bytes.Repeat([]byte{b}, 512) }
	return map[string]*PageList{
		"one-tx": {PageSize: 512, Txs: []Tx{{Pgnos: []uint32{1}, Pages: [][]byte{pg(1)}, DBSize: 1}}},
		"two-tx": {PageSize: 512, Txs: []Tx{
			{Pgnos: []uint32{1, 2}, Pages: [][]byte{pg(1), pg(2)}, DBSize: 2},
			{Pgnos: []uint32{2}, Pages: [][]byte{pg(3)}, DBSize: 2}}},
		"empty": {PageSize: 4096},
	}
}

func FuzzUnmarshalPageList(f *testing.F) {
	for _, pl := range seedLists() {
		b, err := MarshalPageList(pl)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		f.Add(b[:len(b)/2])
		f.Add(append(append([]byte{}, b...), 0))
	}
	f.Add([]byte{})
	f.Add([]byte("WFL1"))
	f.Fuzz(func(t *testing.T, b []byte) {
		pl, err := UnmarshalPageList(b)
		if err != nil {
			if !errors.Is(err, ErrBadPageList) {
				t.Fatalf("error not ErrBadPageList: %v", err)
			}
			return
		}
		// Canonical: what decodes re-encodes to the same bytes.
		out, err := MarshalPageList(pl)
		if err != nil {
			t.Fatalf("decoded list does not re-encode: %v", err)
		}
		if !bytes.Equal(out, b) {
			t.Fatalf("round trip differs: %d vs %d bytes", len(out), len(b))
		}
	})
}

func TestPageListRoundTripAndRefusals(t *testing.T) {
	for name, pl := range seedLists() {
		b, err := MarshalPageList(pl)
		if err != nil {
			t.Fatal(name, err)
		}
		got, err := UnmarshalPageList(b)
		if err != nil {
			t.Fatal(name, err)
		}
		if got.PageSize != pl.PageSize || len(got.Txs) != len(pl.Txs) {
			t.Fatalf("%s: round trip mismatch", name)
		}
		if _, err := UnmarshalPageList(append(b, 1)); !errors.Is(err, ErrBadPageList) {
			t.Errorf("%s: trailing byte accepted", name)
		}
		for i := 0; i < len(b); i++ {
			if _, err := UnmarshalPageList(b[:i]); !errors.Is(err, ErrBadPageList) {
				t.Errorf("%s: truncation to %d accepted (%v)", name, i, err)
				break
			}
		}
	}
	// A huge count in a tiny buffer must be refused, not allocated.
	b := []byte("WFL1\x00\x00\x02\x00\xff\xff\xff\xff")
	if _, err := UnmarshalPageList(b); !errors.Is(err, ErrBadPageList) {
		t.Errorf("huge tx count accepted: %v", err)
	}
}

// TestWriteSeedCorpus regenerates testdata/fuzz/FuzzUnmarshalPageList. It runs
// only with WALFRAME_WRITE_CORPUS=1.
func TestWriteSeedCorpus(t *testing.T) {
	if os.Getenv("WALFRAME_WRITE_CORPUS") != "1" {
		t.Skip("set WALFRAME_WRITE_CORPUS=1 to regenerate the seed corpus")
	}
	dir := filepath.Join("testdata", "fuzz", "FuzzUnmarshalPageList")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	seeds := map[string][]byte{
		"empty-input": {},
		"magic-only":  []byte("WFL1"),
		"huge-count":  []byte("WFL1\x00\x00\x02\x00\xff\xff\xff\xff"),
		"zero-pgno":   []byte("WFL1\x00\x00\x02\x00\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00"),
	}
	for name, pl := range seedLists() {
		b, err := MarshalPageList(pl)
		if err != nil {
			t.Fatal(err)
		}
		seeds[name] = b
		seeds[name+"-truncated"] = b[:len(b)/2]
		seeds[name+"-trailing"] = append(append([]byte{}, b...), 0)
	}
	for name, b := range seeds {
		body := fmt.Sprintf("go test fuzz v1\n[]byte(%s)\n", strconv.Quote(string(b)))
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
