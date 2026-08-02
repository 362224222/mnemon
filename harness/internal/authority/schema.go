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
	SchemaVersion       = 1
	schemaApplicationID = 0x4d4e5237 // MNR7
)

//go:embed schema.sql
var schemaV1 string

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
	if _, err := tx.ExecContext(ctx, schemaV1); err != nil {
		return fmt.Errorf("open authority store: create schema v1: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("open authority store: commit schema v1: %w", err)
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

type schemaColumn struct {
	name       string
	columnType string
	primaryKey int
}

type schemaTable struct {
	name    string
	columns []schemaColumn
}

var requiredSchema = []schemaTable{
	{name: "active_references", columns: []schemaColumn{
		{name: "reference_key", columnType: "TEXT", primaryKey: 1},
		{name: "head_event_id", columnType: "TEXT"}, {name: "state", columnType: "TEXT"},
		{name: "artifact_digest", columnType: "TEXT"},
	}},
	{name: "attachments", columns: []schemaColumn{
		{name: "attachment_id", columnType: "TEXT", primaryKey: 1},
		{name: "principal_id", columnType: "TEXT"}, {name: "mode", columnType: "TEXT"},
		{name: "credential_digest", columnType: "TEXT"}, {name: "issued_at", columnType: "TEXT"},
		{name: "expires_at", columnType: "TEXT"},
	}},
	{name: "authority_clock", columns: []schemaColumn{
		{name: "singleton", columnType: "INTEGER", primaryKey: 1},
		{name: "origin_sequence", columnType: "INTEGER"},
	}},
	{name: "event_artifacts", columns: []schemaColumn{
		{name: "event_id", columnType: "TEXT", primaryKey: 1},
		{name: "artifact_digest", columnType: "TEXT", primaryKey: 2},
	}},
	{name: "events", columns: []schemaColumn{
		{name: "event_id", columnType: "TEXT", primaryKey: 1},
		{name: "event_digest", columnType: "TEXT"}, {name: "origin_sequence", columnType: "INTEGER"},
		{name: "source_principal_id", columnType: "TEXT"}, {name: "request_digest", columnType: "TEXT"},
		{name: "accepted_at", columnType: "TEXT"}, {name: "canonical_json", columnType: "BLOB"},
	}},
	{name: "handlings", columns: []schemaColumn{
		{name: "handling_id", columnType: "TEXT", primaryKey: 1},
		{name: "target_principal_id", columnType: "TEXT"}, {name: "head_event_id", columnType: "TEXT"},
		{name: "state", columnType: "TEXT"}, {name: "outcome", columnType: "TEXT"},
		{name: "claim_attachment_id", columnType: "TEXT"}, {name: "claim_fence", columnType: "INTEGER"},
		{name: "claim_until", columnType: "TEXT"}, {name: "created_sequence", columnType: "INTEGER"},
	}},
	{name: "operations", columns: []schemaColumn{
		{name: "actor_principal_id", columnType: "TEXT", primaryKey: 1},
		{name: "operation_key", columnType: "TEXT", primaryKey: 2},
		{name: "request_digest", columnType: "TEXT"}, {name: "outcome", columnType: "TEXT"},
		{name: "event_id", columnType: "TEXT"}, {name: "receipt_digest", columnType: "TEXT"},
		{name: "receipt_json", columnType: "BLOB"}, {name: "recorded_at", columnType: "TEXT"},
	}},
	{name: "principals", columns: []schemaColumn{
		{name: "principal_id", columnType: "TEXT", primaryKey: 1},
		{name: "created_at", columnType: "TEXT"},
	}},
	{name: "reference_lineage", columns: []schemaColumn{
		{name: "event_id", columnType: "TEXT", primaryKey: 1},
		{name: "reference_key", columnType: "TEXT"}, {name: "previous_event_id", columnType: "TEXT"},
		{name: "state", columnType: "TEXT"}, {name: "artifact_digest", columnType: "TEXT"},
	}},
	{name: "verified_artifacts", columns: []schemaColumn{
		{name: "digest", columnType: "TEXT", primaryKey: 1},
		{name: "byte_size", columnType: "INTEGER"}, {name: "verified_at", columnType: "TEXT"},
	}},
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
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return fmt.Errorf("open authority store: list schema tables: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("open authority store: scan schema table: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("open authority store: close schema table list: %w", err)
	}
	wantNames := make([]string, 0, len(requiredSchema))
	for _, table := range requiredSchema {
		wantNames = append(wantNames, table.name)
	}
	if !slices.Equal(names, wantNames) {
		return fmt.Errorf("%w: application table set does not match schema v%d", ErrUnsupportedSchema, SchemaVersion)
	}
	for _, table := range requiredSchema {
		if err := validateTableColumns(ctx, db, table); err != nil {
			return err
		}
	}
	return nil
}

func validateTableColumns(ctx context.Context, db *sql.DB, table schemaTable) error {
	rows, err := db.QueryContext(ctx,
		"SELECT name, type, pk FROM pragma_table_info(?) ORDER BY cid", table.name)
	if err != nil {
		return fmt.Errorf("open authority store: inspect table %s: %w", table.name, err)
	}
	defer rows.Close()
	var columns []schemaColumn
	for rows.Next() {
		var column schemaColumn
		if err := rows.Scan(&column.name, &column.columnType, &column.primaryKey); err != nil {
			return fmt.Errorf("open authority store: scan table %s: %w", table.name, err)
		}
		column.columnType = strings.ToUpper(column.columnType)
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("open authority store: inspect table %s: %w", table.name, err)
	}
	if !slices.Equal(columns, table.columns) {
		return fmt.Errorf("%w: table %s columns do not match schema v%d",
			ErrUnsupportedSchema, table.name, SchemaVersion)
	}
	return nil
}
