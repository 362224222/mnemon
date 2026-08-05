package selector

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	providerBusyTimeoutMS = 5000
	providerTimeLayout    = "2006-01-02T15:04:05.000000000Z"
)

// Store is a removable R8 provider with its own selector.db and sole writer.
// It has no dependency on the R7 authority, node, peer, or transport packages.
type Store struct {
	mu       sync.Mutex
	db       *sql.DB
	path     string
	lockFile *os.File
	now      func() time.Time
	entropy  io.Reader
	closed   bool
}

func OpenStore(ctx context.Context, databasePath string) (*Store, error) {
	return openStore(ctx, databasePath, time.Now, rand.Reader)
}

func openStore(ctx context.Context, databasePath string, now func() time.Time,
	entropy io.Reader,
) (_ *Store, err error) {
	if ctx == nil || now == nil || entropy == nil {
		return nil, errors.New("open selector store: context, clock, and entropy are required")
	}
	lockFile, err := prepareProviderFiles(databasePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = closeProviderLock(lockFile)
		}
	}()
	db, err := sql.Open("sqlite", providerSQLiteDSN(databasePath))
	if err != nil {
		return nil, fmt.Errorf("open selector store: SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	if err = configureProviderSQLite(ctx, db); err != nil {
		return nil, err
	}
	if err = openProviderSchema(ctx, db); err != nil {
		return nil, err
	}
	return &Store{db: db, path: databasePath, lockFile: lockFile, now: now, entropy: entropy}, nil
}

func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

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
	return errors.Join(s.db.Close(), closeProviderLock(s.lockFile))
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
		return time.Time{}, errors.New("selector store clock returned zero time")
	}
	return value, nil
}

func providerSQLiteDSN(path string) string {
	value := url.URL{Scheme: "file", Path: path}
	query := value.Query()
	query.Add("mode", "rw")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", providerBusyTimeoutMS))
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(FULL)")
	value.RawQuery = query.Encode()
	return value.String()
}

func configureProviderSQLite(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open selector store: connect SQLite: %w", err)
	}
	var journal string
	var synchronous, foreignKeys, timeout int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT journal_mode FROM pragma_journal_mode),
		(SELECT synchronous FROM pragma_synchronous),
		(SELECT foreign_keys FROM pragma_foreign_keys),
		(SELECT timeout FROM pragma_busy_timeout)`).
		Scan(&journal, &synchronous, &foreignKeys, &timeout); err != nil {
		return fmt.Errorf("open selector store: inspect SQLite configuration: %w", err)
	}
	if journal != "wal" || synchronous != 2 || foreignKeys != 1 || timeout != providerBusyTimeoutMS {
		return fmt.Errorf("open selector store: unsafe SQLite configuration")
	}
	return nil
}

func formatProviderTime(value time.Time) string {
	return value.Round(0).UTC().Format(providerTimeLayout)
}

func parseProviderTime(value string) (time.Time, error) {
	parsed, err := time.Parse(providerTimeLayout, value)
	if err != nil || formatProviderTime(parsed) != value {
		return time.Time{}, fmt.Errorf("stored selector time is not canonical: %w", ErrState)
	}
	return parsed, nil
}
