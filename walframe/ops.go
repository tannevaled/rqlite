package walframe

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
)

// fileOps is the engine's access to its database file and its WAL. A missing
// file is not an error for the reads, the removal, the truncation and the size:
// it is the empty state. Tests wrap it to inject failures.
type fileOps interface {
	ReadFile(path string) ([]byte, error)                // missing: nil, nil
	ReadRange(path string, off, n int64) ([]byte, error) // n bytes from off (all the rest if n < 0); missing: nil, nil
	WriteFile(path string, b []byte) error
	Remove(path string) error                                     // missing: nil
	Truncate(path string, size int64) error                       // missing: nil
	Size(path string) (int64, error)                              // missing: 0, nil
	WriteAtTruncate(path string, b []byte, off, size int64) error // creates the file; writes b at off, then cuts the file to size
}

type osOps struct{}

func (osOps) ReadFile(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

func (osOps) ReadRange(path string, off, n int64) ([]byte, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if n < 0 {
		n = math.MaxInt64 - off
	}
	b, err := io.ReadAll(io.NewSectionReader(f, off, n))
	if err != nil || len(b) == 0 {
		return nil, err
	}
	return b, nil
}

func (osOps) WriteFile(path string, b []byte) error { return os.WriteFile(path, b, 0o644) }

func (osOps) Remove(path string) error {
	err := os.Remove(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (osOps) Truncate(path string, size int64) error {
	err := os.Truncate(path, size)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (osOps) Size(path string) (int64, error) {
	st, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (osOps) WriteAtTruncate(path string, b []byte, off, size int64) error {
	// When the open fails f is nil, whose methods all return os.ErrInvalid, so
	// the open error leads the joined result and no branch is needed.
	f, oerr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	_, werr := f.WriteAt(b, off)
	terr := f.Truncate(size)
	return errors.Join(oerr, werr, terr, f.Close())
}
