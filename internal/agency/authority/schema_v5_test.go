package authority

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSchemaV6DoesNotMigrateOlderAuthority(t *testing.T) {
	path := testDatabasePath(t)
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version = 5"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(context.Background(), path)
	if opened != nil || !errors.Is(err, ErrUnsupportedSchema) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("Open(schema v5) = (%v, %v), want ErrUnsupportedSchema", opened, err)
	}
}
