package walframe

import (
	"errors"
	"strings"
	"testing"
)

// F-R5-1: an index on an fts5 shadow table is refused by the guard, so the
// 'rebuild' that overflows the stack is never reached. The test never issues
// the rebuild when the index was accepted: it fails instead of crashing.
func TestIndexOnAShadowTableIsRefused(t *testing.T) {
	defer nopanic(t)
	e := testEngine(t, 0)
	run := func(sql string) string {
		res, pl, id, err := e.Run(bg, req(sql), false)
		if err != nil {
			return err.Error()
		}
		if pl != nil {
			if err := e.Commit(id, pl); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range res {
			if x := r.GetE(); x != nil && x.Error != "" {
				return x.Error
			}
		}
		return ""
	}
	if msg := run("CREATE VIRTUAL TABLE ft USING fts5(x)"); msg != "" {
		t.Fatalf("create fts5: %s", msg)
	}
	for _, sql := range []string{
		"CREATE INDEX rogue ON ft_data(block)",
		"CREATE INDEX rogue2 ON ft_idx(segid)",
		"CREATE INDEX rogue3 ON ft_docsize(sz)",
		"CREATE INDEX rogue4 ON ft_content(c0)",
		"CREATE INDEX rogue5 ON ft_config(k)",
		"CREATE TRIGGER trg AFTER INSERT ON ft_content BEGIN SELECT 1; END",
	} {
		msg := run(sql)
		if msg == "" {
			t.Fatalf("%s was accepted; the rebuild below would overflow the stack", sql)
		}
		if !strings.Contains(strings.ToLower(msg), "authoriz") && !strings.Contains(msg, "shadow") && !strings.Contains(strings.ToLower(msg), "denied") {
			t.Errorf("%s: refused for another reason: %s", sql, msg)
		}
	}
	// Nothing was created, so the rebuild is an ordinary one.
	if msg := run("INSERT INTO ft(ft) VALUES('rebuild')"); msg != "" {
		t.Errorf("rebuild: %s", msg)
	}
	// An ordinary table can still be indexed.
	if msg := run("CREATE TABLE t(a)"); msg != "" {
		t.Fatal(msg)
	}
	if msg := run("CREATE INDEX ti ON t(a)"); msg != "" {
		t.Errorf("index on an ordinary table: %s", msg)
	}
}

// readSizes records the biggest slice ReadRange returned.
type readSizes struct {
	osOps
	max int64
}

func (r *readSizes) ReadRange(p string, off, n int64) ([]byte, error) {
	b, err := r.osOps.ReadRange(p, off, n)
	if int64(len(b)) > r.max {
		r.max = int64(len(b))
	}
	return b, err
}

// F-R5-2: a request that wrote far more than the entry bound is refused
// without its whole WAL region being read into memory.
func TestOversizedWriteIsRefusedWithoutReadingItAll(t *testing.T) {
	defer nopanic(t)
	a, _ := pair(t, 1<<40)
	a.SetMaxEntryBytes(1 << 20)
	rs := &readSizes{}
	a.ops = rs
	// about 24 MB of pages, 24 times the bound
	sql := "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c WHERE i<24000) INSERT INTO t SELECT randomblob(1000) FROM c"
	_, pl, _, err := a.Run(bg, req(sql), false)
	if !errors.Is(err, ErrEntryTooLarge) || pl != nil {
		t.Fatalf("Run: pl=%v err=%v", pl != nil, err)
	}
	if want := regionLimit(1<<20, int64(a.tl.pageSize)); rs.max > want {
		t.Errorf("read %d bytes of WAL at once, the limit is %d", rs.max, want)
	}
	if rs.max > 4<<20 {
		t.Errorf("read %d bytes at once for a 1 MiB bound", rs.max)
	}
	// The engine goes on.
	runOK(t, a, "INSERT INTO t VALUES(1)")
}
