package db

import (
	"testing"

	sqlite3 "modernc.org/sqlite/lib"
)

// F-R5-1: an index or trigger on a virtual table's shadow table is refused, in
// both guarded write modes, while the same on an ordinary table is not.
func TestGuardDeniesIndexOnShadowTable(t *testing.T) {
	for _, mode := range []int32{guardWrite, guardVacuum} {
		for _, c := range []struct {
			op       int32
			z1, z2   string
			wantDeny bool
		}{
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_data", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "FT_IDX", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_docsize", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_content", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_config", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_segments", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_segdir", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "ft_stat", true},
			{sqlite3.SQLITE_CREATE_INDEX, "rogue", "rt_node", true},
			{sqlite3.SQLITE_CREATE_TRIGGER, "trg", "ft_content", true},
			{sqlite3.SQLITE_REINDEX, "rogue_ft_data", "", true},
			{sqlite3.SQLITE_CREATE_INDEX, "sqlite_autoindex_ft_idx_1", "ft_idx", false},
			{sqlite3.SQLITE_CREATE_INDEX, "i", "t", false},
			{sqlite3.SQLITE_CREATE_INDEX, "i", "data", false},
			{sqlite3.SQLITE_CREATE_TRIGGER, "trg", "t", false},
			{sqlite3.SQLITE_REINDEX, "i", "", false},
		} {
			if got := guardDeniesMode(mode, c.op, c.z1, c.z2, "main"); got != c.wantDeny {
				t.Errorf("mode %d op %d %q %q: denied %v, want %v", mode, c.op, c.z1, c.z2, got, c.wantDeny)
			}
		}
	}
}
