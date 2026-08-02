package authority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	privateDirectoryMode = 0o700
	privateFileMode      = 0o600
	busyTimeoutMS        = 5000
	storeTimeLayout      = "2006-01-02T15:04:05.000000000Z"
)

// Store owns the only SQLite writer for one R7 local authority. The process
// lock excludes a second Store, while the mutex makes Close and transactions
// have one explicit in-process owner.
type Store struct {
	mu       sync.Mutex
	db       *sql.DB
	path     string
	lockFile *os.File
	closed   bool
	now      func() time.Time
}

// Open acquires the writer guard and initializes only an empty version-zero
// database. It never migrates another schema.
func Open(ctx context.Context, databasePath string) (*Store, error) {
	return open(ctx, databasePath, time.Now)
}

func open(ctx context.Context, databasePath string, now func() time.Time) (_ *Store, err error) {
	if ctx == nil || now == nil {
		return nil, errors.New("open authority store: nil context or clock")
	}
	plan, err := prepareAuthorityPath(databasePath)
	if err != nil {
		return nil, err
	}
	lockFile, err := plan.acquireWriterLock()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = releaseWriterLock(lockFile)
		}
	}()
	if err = plan.prepareDatabaseFile(); err != nil {
		return nil, err
	}
	if err = plan.verifyBeforeSQLite(); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", sqliteDSN(plan.databasePath))
	if err != nil {
		return nil, fmt.Errorf("open authority store: SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	if err = configureAuthoritySQLite(ctx, db); err != nil {
		return nil, err
	}
	if err = openSchema(ctx, db); err != nil {
		return nil, err
	}
	if err = plan.verifyAfterSQLite(); err != nil {
		return nil, err
	}
	return &Store{db: db, path: plan.databasePath, lockFile: lockFile, now: now}, nil
}

func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Close flushes SQLite before releasing the writer guard and is idempotent.
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
	return errors.Join(s.db.Close(), releaseWriterLock(s.lockFile))
}

func (s *Store) requireOpen() error {
	if s == nil || s.db == nil || s.closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) trustedNow() (time.Time, error) {
	if s == nil || s.now == nil {
		return time.Time{}, ErrClosed
	}
	value := s.now().Round(0).UTC()
	if value.IsZero() {
		return time.Time{}, errors.New("authority: clock returned zero time")
	}
	return value, nil
}

func sqliteDSN(path string) string {
	value := url.URL{Scheme: "file", Path: path}
	query := value.Query()
	query.Add("mode", "rw")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(FULL)")
	value.RawQuery = query.Encode()
	return value.String()
}

func formatTime(value time.Time) string { return value.Round(0).UTC().Format(storeTimeLayout) }

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(storeTimeLayout, value)
	if err != nil || formatTime(parsed) != value {
		return time.Time{}, errors.New("authority: stored time is not canonical")
	}
	return parsed, nil
}
