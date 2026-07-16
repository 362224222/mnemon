package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const (
	directoryMode   = 0o700
	privateFileMode = 0o600
	busyTimeoutMS   = 5000
)

var (
	// ErrUnsupportedSchema means the database is not an exact R5 schema v1
	// store. R5 deliberately has no migration path from an earlier Harness.
	ErrUnsupportedSchema = errors.New("unsupported node schema")
	// ErrWriterActive means another mnemond process already owns the Node store.
	ErrWriterActive = errors.New("node store writer already active")
)

// Store owns the only SQLite writer connection for one R5 Node.
type Store struct {
	mu       sync.Mutex
	db       *sql.DB
	path     string
	lockFile *os.File
	closed   bool
}

// Open acquires the Node writer guard, configures SQLite, and creates or
// validates schema v1. A version-zero database is initialized only when it
// contains no application objects.
func Open(ctx context.Context, dbPath string) (_ *Store, err error) {
	if ctx == nil {
		return nil, errors.New("open node store: nil context")
	}
	path, err := privateDatabasePath(dbPath)
	if err != nil {
		return nil, err
	}

	lockFile, err := acquireWriterLock(path + ".writer.lock")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = releaseWriterLock(lockFile)
		}
	}()

	if err = preparePrivateDatabaseFile(path); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open node store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	if err = configureDatabase(ctx, db); err != nil {
		return nil, err
	}
	if err = openSchema(ctx, db); err != nil {
		return nil, err
	}
	if err = protectDatabaseFiles(path); err != nil {
		return nil, err
	}

	return &Store{db: db, path: path, lockFile: lockFile}, nil
}

// Path returns the absolute path of node.db.
func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Close flushes SQLite before releasing the process writer guard. It is safe
// to call Close more than once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	dbErr := s.db.Close()
	lockErr := releaseWriterLock(s.lockFile)
	return errors.Join(dbErr, lockErr)
}

func sqliteDSN(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = q.Encode()
	return u.String()
}

func privateDatabasePath(dbPath string) (string, error) {
	if strings.TrimSpace(dbPath) == "" {
		return "", errors.New("open node store: empty database path")
	}
	path, err := filepath.Abs(filepath.Clean(dbPath))
	if err != nil {
		return "", fmt.Errorf("open node store: absolute database path: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, directoryMode); err != nil {
		return "", fmt.Errorf("open node store: create state directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", fmt.Errorf("open node store: inspect state directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("open node store: state directory is not a real directory")
	}
	if err := os.Chmod(dir, directoryMode); err != nil {
		return "", fmt.Errorf("open node store: protect state directory: %w", err)
	}
	if info, err = os.Stat(dir); err != nil || info.Mode().Perm() != directoryMode {
		if err != nil {
			return "", fmt.Errorf("open node store: verify state directory: %w", err)
		}
		return "", fmt.Errorf("open node store: state directory mode is %04o, want %04o", info.Mode().Perm(), directoryMode)
	}
	return path, nil
}

func preparePrivateDatabaseFile(path string) error {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("open node store: node.db is not a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("open node store: inspect node.db: %w", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, privateFileMode)
	if err != nil {
		return fmt.Errorf("open node store: create node.db: %w", err)
	}
	closeErr := file.Close()
	if err := os.Chmod(path, privateFileMode); err != nil {
		return fmt.Errorf("open node store: protect node.db: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("open node store: close node.db bootstrap handle: %w", closeErr)
	}
	return verifyPrivateRegular(path)
}

func acquireWriterLock(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, errors.New("open node store: writer lock is not a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("open node store: inspect writer lock: %w", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, privateFileMode)
	if err != nil {
		return nil, fmt.Errorf("open node store: create writer lock: %w", err)
	}
	if err := file.Chmod(privateFileMode); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open node store: protect writer lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrWriterActive
		}
		return nil, fmt.Errorf("open node store: acquire writer lock: %w", err)
	}
	if err := verifyPrivateRegular(path); err != nil {
		_ = releaseWriterLock(file)
		return nil, err
	}
	return file, nil
}

func releaseWriterLock(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return errors.Join(unlockErr, file.Close())
}

func protectDatabaseFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Lstat(candidate); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("open node store: inspect SQLite file: %w", err)
		}
		if err := os.Chmod(candidate, privateFileMode); err != nil {
			return fmt.Errorf("open node store: protect SQLite file: %w", err)
		}
		if err := verifyPrivateRegular(candidate); err != nil {
			return err
		}
	}
	return nil
}

func verifyPrivateRegular(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("open node store: inspect private file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("open node store: %s is not a regular file", filepath.Base(path))
	}
	if info.Mode().Perm() != privateFileMode {
		return fmt.Errorf("open node store: %s mode is %04o, want %04o", filepath.Base(path), info.Mode().Perm(), privateFileMode)
	}
	return nil
}
