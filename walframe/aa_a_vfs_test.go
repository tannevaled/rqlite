package walframe

import (
	"os"
	"path/filepath"
	"testing"
)

// This file sorts first: a VFS whose Access misreports a missing file makes
// SQLite retry forever, so the engine tests after it would hang instead of
// failing. Run the suite with -failfast.
func TestFileVFSAccess(t *testing.T) {
	d := t.TempDir()
	v := &fileVFS{dir: d}
	ok, err := v.Access("absent.db", 0)
	if ok || err != nil {
		t.Errorf("missing file: %v, %v", ok, err)
	}
	os.WriteFile(filepath.Join(d, "there.db"), nil, 0o644)
	ok, err = v.Access("there.db", 0)
	if !ok || err != nil {
		t.Errorf("existing file: %v, %v", ok, err)
	}
}
