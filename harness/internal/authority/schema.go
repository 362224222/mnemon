package authority

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"slices"
	"strings"
)

const (
	SchemaVersion       = 2
	schemaApplicationID = 0x4d4e5237 // MNR7
)

//go:embed schema.sql
var currentSchema string

func configureAuthoritySQLite(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open authority store: connect SQLite: %w", err)
	}
	var journalMode string
	var synchronous, foreignKeys, busyTimeout int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT journal_mode FROM pragma_journal_mode),
		(SELECT synchronous FROM pragma_synchronous),
		(SELECT foreign_keys FROM pragma_foreign_keys),
		(SELECT timeout FROM pragma_busy_timeout)`).
		Scan(&journalMode, &synchronous, &foreignKeys, &busyTimeout); err != nil {
		return fmt.Errorf("open authority store: inspect SQLite configuration: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") || synchronous != 2 || foreignKeys != 1 ||
		busyTimeout != busyTimeoutMS {
		return fmt.Errorf("open authority store: unsafe SQLite configuration: journal=%q synchronous=%d foreign_keys=%d busy_timeout=%d",
			journalMode, synchronous, foreignKeys, busyTimeout)
	}
	return nil
}

func openSchema(ctx context.Context, db *sql.DB) error {
	var applicationID, version int
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return fmt.Errorf("open authority store: read application ID: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("open authority store: read schema version: %w", err)
	}
	switch {
	case applicationID == 0 && version == 0:
		var objects int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&objects); err != nil {
			return fmt.Errorf("open authority store: inspect empty schema: %w", err)
		}
		if objects != 0 {
			return fmt.Errorf("%w: version-zero database is not empty", ErrUnsupportedSchema)
		}
		if err := initializeSchema(ctx, db); err != nil {
			return err
		}
	case applicationID == schemaApplicationID && version == SchemaVersion:
	default:
		return fmt.Errorf("%w: got application_id=%d version=%d, want application_id=%d version=%d",
			ErrUnsupportedSchema, applicationID, version, schemaApplicationID, SchemaVersion)
	}
	return validateDatabase(ctx, db)
}

func initializeSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("open authority store: begin schema: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, currentSchema); err != nil {
		return fmt.Errorf("open authority store: create schema v%d: %w", SchemaVersion, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("open authority store: commit schema v%d: %w", SchemaVersion, err)
	}
	return nil
}

func validateDatabase(ctx context.Context, db *sql.DB) error {
	if err := validateSchemaShape(ctx, db); err != nil {
		return err
	}
	var quickCheck string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&quickCheck); err != nil {
		return fmt.Errorf("open authority store: quick check: %w", err)
	}
	if quickCheck != "ok" {
		return fmt.Errorf("open authority store: quick check failed: %s", quickCheck)
	}
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("open authority store: foreign key check: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("open authority store: durable foreign key violation")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("open authority store: foreign key check: %w", err)
	}
	var count, singleton int
	var minimumSequence int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(singleton), 0),
		COALESCE(MIN(origin_sequence), -1) FROM authority_clock`).
		Scan(&count, &singleton, &minimumSequence); err != nil {
		return fmt.Errorf("open authority store: validate authority clock: %w", err)
	}
	if count != 1 || singleton != 1 || minimumSequence < 0 {
		return fmt.Errorf("%w: authority clock singleton is invalid", ErrUnsupportedSchema)
	}
	return nil
}

type schemaDefinition struct {
	objectType string
	name       string
	table      string
	sql        string
}

func validateSchemaShape(ctx context.Context, db *sql.DB) error {
	var applicationID, version int
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return fmt.Errorf("open authority store: validate application ID: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("open authority store: validate schema version: %w", err)
	}
	if applicationID != schemaApplicationID || version != SchemaVersion {
		return fmt.Errorf("%w: schema identity changed", ErrUnsupportedSchema)
	}
	want, err := expectedSchemaDefinitions(ctx)
	if err != nil {
		return err
	}
	got, err := readSchemaDefinitions(ctx, db)
	if err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: durable schema does not exactly match schema v%d",
			ErrUnsupportedSchema, SchemaVersion)
	}
	return nil
}

func expectedSchemaDefinitions(ctx context.Context) ([]schemaDefinition, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open authority store: construct schema oracle: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	if _, err := db.ExecContext(ctx, currentSchema); err != nil {
		return nil, fmt.Errorf("open authority store: construct schema oracle: %w", err)
	}
	return readSchemaDefinitions(ctx, db)
}

func readSchemaDefinitions(ctx context.Context, db *sql.DB) ([]schemaDefinition, error) {
	rows, err := db.QueryContext(ctx, `SELECT type, name, tbl_name, sql FROM sqlite_schema
		WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL ORDER BY type, name`)
	if err != nil {
		return nil, fmt.Errorf("open authority store: inspect canonical schema: %w", err)
	}
	defer rows.Close()
	var definitions []schemaDefinition
	for rows.Next() {
		var definition schemaDefinition
		if err := rows.Scan(&definition.objectType, &definition.name, &definition.table,
			&definition.sql); err != nil {
			return nil, fmt.Errorf("open authority store: scan canonical schema: %w", err)
		}
		definitions = append(definitions, definition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("open authority store: inspect canonical schema: %w", err)
	}
	return definitions, nil
}
