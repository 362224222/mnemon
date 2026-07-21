package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// SchemaVersion is the only Node database version accepted by R5.
const SchemaVersion = 1

//go:embed schema.sql
var schemaV1 string

var createObjectPattern = regexp.MustCompile(
	"(?m)^CREATE (?:UNIQUE )?(TABLE|TRIGGER|INDEX) ([A-Za-z_][A-Za-z0-9_]*)",
)

var referenceSchemaObjectCache = sync.OnceValues(func() (map[string]schemaObject, error) {
	return buildReferenceSchemaObjects(context.Background())
})

func configureDatabase(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open node store: connect SQLite: %w", err)
	}

	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("open node store: enable WAL: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("open node store: journal_mode is %q, want WAL", journalMode)
	}
	for _, statement := range []string{
		"PRAGMA synchronous = FULL",
		"PRAGMA foreign_keys = ON",
		fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMS),
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("open node store: configure SQLite: %w", err)
		}
	}

	var synchronous, foreignKeys, busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("open node store: read synchronous: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("open node store: read foreign_keys: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("open node store: read busy_timeout: %w", err)
	}
	if synchronous != 2 || foreignKeys != 1 || busyTimeout != busyTimeoutMS {
		return fmt.Errorf(
			"open node store: unsafe SQLite configuration: synchronous=%d foreign_keys=%d busy_timeout=%d",
			synchronous,
			foreignKeys,
			busyTimeout,
		)
	}
	return nil
}

func openSchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("open node store: read user_version: %w", err)
	}

	switch version {
	case 0:
		empty, err := applicationSchemaEmpty(ctx, db)
		if err != nil {
			return err
		}
		if !empty {
			return unsupportedSchema(version, "version-zero database is not empty")
		}
		if err := initializeSchema(ctx, db); err != nil {
			return err
		}
	case SchemaVersion:
	default:
		return unsupportedSchema(version, "R5 does not migrate node databases")
	}

	return validateExistingSchema(ctx, db)
}

// openExistingSchema is deliberately separate from openSchema: version zero
// is never a bootstrap signal on daemon restart.
func openExistingSchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("open existing node store: read user_version: %w", err)
	}
	if version != SchemaVersion {
		return unsupportedSchema(version, "daemon restart does not initialize or migrate node databases")
	}
	return validateExistingSchema(ctx, db)
}

func validateExistingSchema(ctx context.Context, db *sql.DB) error {
	if err := validateSchemaObjects(ctx, db); err != nil {
		return err
	}
	var quickCheck string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&quickCheck); err != nil {
		return fmt.Errorf("open node store: quick_check: %w", err)
	}
	if quickCheck != "ok" {
		return fmt.Errorf("open node store: SQLite quick_check failed: %s", quickCheck)
	}
	if err := validateForeignKeys(ctx, db); err != nil {
		return err
	}
	return nil
}

func configureExistingDatabase(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("open existing node store: connect SQLite: %w", err)
	}
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return fmt.Errorf("open existing node store: read journal_mode: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("open existing node store: journal_mode is %q, want WAL", journalMode)
	}
	for _, statement := range []string{
		"PRAGMA synchronous = FULL",
		"PRAGMA foreign_keys = ON",
		fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMS),
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("open existing node store: configure SQLite: %w", err)
		}
	}
	var synchronous, foreignKeys, busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("open existing node store: read synchronous: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("open existing node store: read foreign_keys: %w", err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("open existing node store: read busy_timeout: %w", err)
	}
	if synchronous != 2 || foreignKeys != 1 || busyTimeout != busyTimeoutMS {
		return fmt.Errorf("open existing node store: unsafe SQLite configuration: synchronous=%d foreign_keys=%d busy_timeout=%d",
			synchronous, foreignKeys, busyTimeout)
	}
	return nil
}

func validateForeignKeys(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("open node store: foreign_key_check: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		var table string
		var rowID any
		var parent string
		var foreignKeyID int
		if err := rows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			return fmt.Errorf("open node store: scan foreign_key_check: %w", err)
		}
		return fmt.Errorf("open node store: foreign key violation in %s row %v referencing %s (constraint %d)",
			table, rowID, parent, foreignKeyID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("open node store: foreign_key_check: %w", err)
	}
	return nil
}

func applicationSchemaEmpty(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
	).Scan(&count); err != nil {
		return false, fmt.Errorf("open node store: inspect version-zero schema: %w", err)
	}
	return count == 0, nil
}

func initializeSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("open node store: begin schema transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := tx.ExecContext(ctx, schemaDDL()); err != nil {
		return fmt.Errorf("open node store: create schema v1: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("open node store: commit schema v1: %w", err)
	}
	return nil
}

func validateSchemaObjects(ctx context.Context, db *sql.DB) error {
	expected, err := referenceSchemaObjects(ctx)
	if err != nil {
		return fmt.Errorf("open node store: embedded schema manifest: %w", err)
	}

	rows, err := db.QueryContext(
		ctx,
		"SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
	)
	if err != nil {
		return fmt.Errorf("open node store: read schema objects: %w", err)
	}
	defer rows.Close()

	actual := make(map[string]schemaObject, len(expected))
	for rows.Next() {
		var objectType, name string
		var definition sql.NullString
		if err := rows.Scan(&objectType, &name, &definition); err != nil {
			return fmt.Errorf("open node store: scan schema object: %w", err)
		}
		actual[name] = schemaObject{objectType: objectType, definition: definition.String}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("open node store: scan schema objects: %w", err)
	}

	var differences []string
	for name, object := range expected {
		if got, ok := actual[name]; !ok {
			differences = append(differences, "missing "+object.objectType+" "+name)
		} else if got.objectType != object.objectType {
			differences = append(
				differences,
				fmt.Sprintf("%s has type %s, want %s", name, got.objectType, object.objectType),
			)
		} else if got.definition != object.definition {
			differences = append(differences, "definition mismatch for "+object.objectType+" "+name)
		}
	}
	for name, object := range actual {
		if _, ok := expected[name]; !ok {
			differences = append(differences, "unexpected "+object.objectType+" "+name)
		}
	}
	if len(differences) != 0 {
		sort.Strings(differences)
		return unsupportedSchema(SchemaVersion, strings.Join(differences, "; "))
	}
	return nil
}

type schemaObject struct {
	objectType string
	definition string
}

func referenceSchemaObjects(ctx context.Context) (map[string]schemaObject, error) {
	if ctx == nil {
		return nil, fmt.Errorf("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	objects, err := referenceSchemaObjectCache()
	if err != nil {
		return nil, err
	}
	return cloneSchemaObjects(objects), nil
}

func buildReferenceSchemaObjects(ctx context.Context) (map[string]schemaObject, error) {
	declared, err := declaredSchemaObjects()
	if err != nil {
		return nil, err
	}

	reference, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open reference database: %w", err)
	}
	reference.SetMaxOpenConns(1)
	reference.SetMaxIdleConns(1)
	defer reference.Close()
	if _, err := reference.ExecContext(ctx, schemaDDL()); err != nil {
		return nil, fmt.Errorf("create reference database: %w", err)
	}

	rows, err := reference.QueryContext(
		ctx,
		"SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
	)
	if err != nil {
		return nil, fmt.Errorf("read reference schema: %w", err)
	}
	defer rows.Close()

	objects := make(map[string]schemaObject, len(declared))
	for rows.Next() {
		var objectType, name string
		var definition sql.NullString
		if err := rows.Scan(&objectType, &name, &definition); err != nil {
			return nil, fmt.Errorf("scan reference schema: %w", err)
		}
		objects[name] = schemaObject{objectType: objectType, definition: definition.String}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan reference schema: %w", err)
	}
	if len(objects) != len(declared) {
		return nil, fmt.Errorf("declared %d objects but SQLite created %d", len(declared), len(objects))
	}
	for name, objectType := range declared {
		if object, ok := objects[name]; !ok || object.objectType != objectType {
			return nil, fmt.Errorf("declared %s %s was not created", objectType, name)
		}
	}
	return objects, nil
}

func cloneSchemaObjects(objects map[string]schemaObject) map[string]schemaObject {
	clone := make(map[string]schemaObject, len(objects))
	for name, object := range objects {
		clone[name] = object
	}
	return clone
}

func declaredSchemaObjects() (map[string]string, error) {
	matches := createObjectPattern.FindAllStringSubmatch(schemaV1, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no CREATE statements")
	}
	objects := make(map[string]string, len(matches))
	for _, match := range matches {
		objectType := strings.ToLower(match[1])
		name := match[2]
		if previous, exists := objects[name]; exists {
			return nil, fmt.Errorf("duplicate object %s (%s and %s)", name, previous, objectType)
		}
		objects[name] = objectType
	}
	return objects, nil
}

func schemaDDL() string {
	const firstDDL = "CREATE TABLE node"
	offset := strings.Index(schemaV1, firstDDL)
	if offset < 0 {
		panic("store: embedded schema has no " + firstDDL)
	}
	ddl := schemaV1[offset:]
	if strings.Count(ddl, eventTypeValuesPlaceholder) != 1 {
		panic("store: embedded schema must contain exactly one EventType projection")
	}
	return strings.Replace(ddl, eventTypeValuesPlaceholder, eventTypeSQLProjection(), 1)
}

func unsupportedSchema(version int, reason string) error {
	return fmt.Errorf(
		"%w: user_version=%d, expected %d (%s); run mnemon-harness eject/setup",
		ErrUnsupportedSchema,
		version,
		SchemaVersion,
		reason,
	)
}
