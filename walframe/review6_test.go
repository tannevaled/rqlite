package walframe

import (
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
		if !strings.Contains(strings.ToLower(msg), "authoriz") && !strings.Contains(msg, "shadow") &&!strings.Contains(strings.ToLower(msg), "denied") {
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
