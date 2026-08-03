package selector

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"slices"
)

const (
	providerSchemaVersion       = 2
	providerSchemaApplicationID = 0x4d4e5238 // MNR8
)

//go:embed provider_schema.sql
var providerSchema string

type providerSchemaDefinition struct {
	objectType string
	name       string
	table      string
	sql        string
}

func openProviderSchema(ctx context.Context, db *sql.DB) error {
	var applicationID, version int
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return fmt.Errorf("open selector store: read application ID: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("open selector store: read schema version: %w", err)
	}
	switch {
	case applicationID == 0 && version == 0:
		var objects int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&objects); err != nil {
			return fmt.Errorf("open selector store: inspect empty schema: %w", err)
		}
		if objects != 0 {
			return fmt.Errorf("open selector store: version-zero database is not empty: %w", ErrState)
		}
		if err := initializeProviderSchema(ctx, db); err != nil {
			return err
		}
	case applicationID == providerSchemaApplicationID && version == providerSchemaVersion:
	default:
		return fmt.Errorf("open selector store: unsupported schema identity %d/%d: %w",
			applicationID, version, ErrState)
	}
	return validateProviderDatabase(ctx, db)
}

func initializeProviderSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("open selector store: begin schema: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, providerSchema); err != nil {
		return fmt.Errorf("open selector store: create schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("open selector store: commit schema: %w", err)
	}
	return nil
}

func validateProviderDatabase(ctx context.Context, db *sql.DB) error {
	want, err := providerSchemaOracle(ctx)
	if err != nil {
		return err
	}
	got, err := readProviderSchema(ctx, db)
	if err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("open selector store: durable schema changed: %w", ErrState)
	}
	var quickCheck string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&quickCheck); err != nil {
		return fmt.Errorf("open selector store: quick check: %w", err)
	}
	if quickCheck != "ok" {
		return fmt.Errorf("open selector store: quick check failed: %s: %w", quickCheck, ErrState)
	}
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("open selector store: foreign key check: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("open selector store: durable foreign key violation: %w", ErrState)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return validateProviderRowBounds(ctx, db)
}

func validateProviderRowBounds(ctx context.Context, db *sql.DB) error {
	var selections, active, pending, settled int
	if err := db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM selections),
		(SELECT COUNT(*) FROM selections WHERE phase != 'observed'),
		(SELECT COUNT(*) FROM pending_rounds),
		(SELECT COUNT(*) FROM settled_rounds)`).
		Scan(&selections, &active, &pending, &settled); err != nil {
		return fmt.Errorf("open selector store: inspect durable bounds: %w", err)
	}
	if selections > MaxStoredSelections || active > MaxActiveSelections ||
		pending > MaxPendingRounds || settled > MaxStoredRoundSettlements {
		return fmt.Errorf("open selector store: durable bounds exceeded: %w", ErrState)
	}
	return nil
}

func providerSchemaOracle(ctx context.Context) ([]providerSchemaDefinition, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open selector store: construct schema oracle: %w", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, providerSchema); err != nil {
		return nil, fmt.Errorf("open selector store: construct schema oracle: %w", err)
	}
	return readProviderSchema(ctx, db)
}

func readProviderSchema(ctx context.Context, db *sql.DB) ([]providerSchemaDefinition, error) {
	rows, err := db.QueryContext(ctx, `SELECT type, name, tbl_name, sql FROM sqlite_schema
		WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL ORDER BY type, name`)
	if err != nil {
		return nil, fmt.Errorf("open selector store: inspect schema: %w", err)
	}
	defer rows.Close()
	var definitions []providerSchemaDefinition
	for rows.Next() {
		var definition providerSchemaDefinition
		if err := rows.Scan(&definition.objectType, &definition.name, &definition.table,
			&definition.sql); err != nil {
			return nil, fmt.Errorf("open selector store: scan schema: %w", err)
		}
		definitions = append(definitions, definition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("open selector store: inspect schema: %w", err)
	}
	return definitions, nil
}
