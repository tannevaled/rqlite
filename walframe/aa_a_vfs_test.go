package walframe

import (
	"os"
	"path/filepath"
	"testing"
)

// closeLocked on an engine with no connection changes nothing. It is first
// because a mutant here panics in every later engine test's cleanup, and the
// assertion must be the first failure seen.
func TestCloseLockedWithoutAConnection(t *testing.T) {
	e := testEngine(t, 1<<30)
	func() {
		defer func() { recover() }()
		e.closeLocked()
	}()
	if e.v.fenced.Load() {
		t.Error("closing nothing fenced the storage")
	}
	e.v.fenced.Store(false)
}

// An unwritable parent makes MkdirAll fail where no file removal does.
func TestNewEngineMkdirFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	d := t.TempDir()
	os.Chmod(d, 0o500)
	defer os.Chmod(d, 0o700)
	if e, err := NewEngine(filepath.Join(d, "sub"), 0, nil); err == nil {
		e.Close()
		t.Error("NewEngine created a directory in an unwritable parent")
	}
}

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
