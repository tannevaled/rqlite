package walframe

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// A rolled-back spill that is under the read cap is cut by the rolled-back
// path, not by the entry bound: the bound is raised so that the cap does not
// refuse it first.
func TestRolledBackSpillUnderTheCapIsTruncated(t *testing.T) {
	defer nopanic(t)
	a, _ := pair(t, 0)
	a.SetMaxEntryBytes(1 << 30)
	sql := "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<16384) INSERT INTO t SELECT randomblob(1000) FROM c"
	_, pl, err := runPL(a, "BEGIN", sql)
	if pl != nil || errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("pl=%v err=%v", pl != nil, err)
	}
	if n, _ := (osOps{}).Size(a.walPath); n > a.tl.end {
		t.Errorf("WAL still %d bytes after a rolled-back spill (committed end %d)", n, a.tl.end)
	}
}

func runPL(e *Engine, sqls ...string) ([idSize]byte, *PageList, error) {
	_, pl, id, err := e.Run(bg, req(sqls...), false)
	return id, pl, err
}

// The entry bound itself, one step past it: a request whose region is under
// the read cap but whose entry would exceed the bound is refused by the
// estimate, and those just under it are shipped.
func TestEntryBoundJustPastIsRefusedByTheEstimate(t *testing.T) {
	defer nopanic(t)
	a, _ := pair(t, 1<<40)
	const bound = 256 << 10
	a.SetMaxEntryBytes(bound)
	refused := false
	for rows := 100; rows <= 400 && !refused; rows += 4 {
		sql := fmt.Sprintf("WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<%d) INSERT INTO t SELECT randomblob(1000) FROM c", rows)
		id, pl, err := runPL(a, sql)
		if err == nil {
			if pl == nil {
				t.Fatal("no page list")
			}
			est := int64(0)
			for _, tx := range pl.Txs {
				est += int64(len(tx.Pages)) * (int64(pl.PageSize) + 4)
			}
			if est > bound {
				t.Fatalf("%d rows: an entry of %d bytes was accepted, the bound is %d", rows, est, bound)
			}
			if uerr := a.Undo(id); uerr != nil {
				t.Fatal(uerr)
			}
			continue
		}
		if !errors.Is(err, ErrEntryTooLarge) || !strings.Contains(err.Error(), "the bound is") || strings.Contains(err.Error(), "more than") {
			t.Fatalf("%d rows: %v (want the estimate's refusal)", rows, err)
		}
		refused = true
	}
	if !refused {
		t.Fatal("no request crossed the bound")
	}
}
