package walframe

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"

	"modernc.org/sqlite/vfs"
)

var errFenced = errors.New("walframe: storage fenced")

// fileVFS is a vfs.VFS over one directory that can be FENCED: while fenced
// every write, truncate and delete fails, so nothing SQLite does on close (its
// checkpoint, which would make an un-replicated transaction durable in the
// database file) reaches storage. See the design note, probe 5 and 6.
type fileVFS struct {
	dir    string
	fenced atomic.Bool
}

func (v *fileVFS) path(name string) string { return filepath.Join(v.dir, filepath.Base(name)) }

func (v *fileVFS) Open(name string, flags vfs.OpenFlag) (vfs.File, vfs.OpenFlag, error) {
	osFlags := os.O_RDWR
	if flags&vfs.OpenReadOnly != 0 {
		osFlags = os.O_RDONLY
	}
	if flags&vfs.OpenCreate != 0 {
		osFlags |= os.O_CREATE
	}
	f, err := os.OpenFile(v.path(name), osFlags, 0o644)
	if err != nil {
		return nil, 0, err
	}
	return &osFile{v: v, f: f}, flags, nil
}

func (v *fileVFS) Delete(name string, _ bool) error {
	if v.fenced.Load() {
		return errFenced
	}
	return os.Remove(v.path(name))
}

func (v *fileVFS) Access(name string, _ vfs.AccessFlag) (bool, error) {
	_, err := os.Stat(v.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (v *fileVFS) FullPathname(name string) (string, error) { return name, nil }

type osFile struct {
	v *fileVFS
	f *os.File
}

func (f *osFile) ReadAt(p []byte, off int64) (int, error) { return f.f.ReadAt(p, off) }

func (f *osFile) WriteAt(p []byte, off int64) (int, error) {
	if f.v.fenced.Load() {
		return 0, errFenced
	}
	return f.f.WriteAt(p, off)
}

func (f *osFile) Close() error { return f.f.Close() }

func (f *osFile) Truncate(size int64) error {
	if f.v.fenced.Load() {
		return errFenced
	}
	return f.f.Truncate(size)
}

func (f *osFile) Sync(vfs.SyncFlag) error { return f.f.Sync() }

func (f *osFile) Size() (int64, error) {
	st, err := f.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
