package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
)

//go:embed schema.sql
var schemaV1 string

var createObjectPattern = regexp.MustCompile(
	"(?m)^CREATE (?:UNIQUE )?(TABLE|TRIGGER|INDEX) ([A-Za-z_][A-Za-z0-9_]*)",
)

type schemaObject struct {
	objectType string
	definition string
}

type namedSchemaObject struct {
	name   string
	object schemaObject
}

// schemaManifestCache owns one immutable projection of the embedded schema.
// The builder may block and must observe ctx; it always runs outside the lock.
// Failed or cancelled construction is never published and may be retried.
type schemaManifestCache struct {
	mu       sync.Mutex
	objects  []namedSchemaObject
	building chan struct{}
	build    func(context.Context) ([]namedSchemaObject, error)
}

var canonicalSchemaManifest = schemaManifestCache{build: buildReferenceSchemaManifest}

func referenceSchemaObjects(ctx context.Context) ([]namedSchemaObject, error) {
	return canonicalSchemaManifest.load(ctx)
}

func (cache *schemaManifestCache) load(ctx context.Context) ([]namedSchemaObject, error) {
	if cache == nil || ctx == nil {
		return nil, fmt.Errorf("schema manifest cache is unavailable")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cache.mu.Lock()
		if len(cache.objects) > 0 {
			objects := slices.Clone(cache.objects)
			cache.mu.Unlock()
			return objects, nil
		}
		if cache.build == nil {
			cache.mu.Unlock()
			return nil, fmt.Errorf("schema manifest builder is unavailable")
		}
		if cache.building != nil {
			building := cache.building
			cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-building:
				continue
			}
		}
		cache.building = make(chan struct{})
		build, building := cache.build, cache.building
		cache.mu.Unlock()

		objects, err := build(ctx)
		if err == nil && len(objects) == 0 {
			err = fmt.Errorf("schema manifest builder returned no objects")
		}
		cache.mu.Lock()
		if err == nil {
			cache.objects = slices.Clone(objects)
		}
		close(building)
		cache.building = nil
		cached := slices.Clone(cache.objects)
		cache.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return cached, nil
	}
}

func buildReferenceSchemaManifest(ctx context.Context) ([]namedSchemaObject, error) {
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
	rows, err := reference.QueryContext(ctx,
		"SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name")
	if err != nil {
		return nil, fmt.Errorf("read reference schema: %w", err)
	}
	defer rows.Close()
	objects := make([]namedSchemaObject, 0, len(declared))
	created := make(map[string]string, len(declared))
	for rows.Next() {
		var objectType, name string
		var definition sql.NullString
		if err := rows.Scan(&objectType, &name, &definition); err != nil {
			return nil, fmt.Errorf("scan reference schema: %w", err)
		}
		created[name] = objectType
		objects = append(objects, namedSchemaObject{name: name,
			object: schemaObject{objectType: objectType, definition: definition.String}})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan reference schema: %w", err)
	}
	if len(objects) != len(declared) {
		return nil, fmt.Errorf("declared %d objects but SQLite created %d", len(declared), len(objects))
	}
	declaredNames := make([]string, 0, len(declared))
	for name := range declared {
		declaredNames = append(declaredNames, name)
	}
	sort.Strings(declaredNames)
	for _, name := range declaredNames {
		if created[name] != declared[name] {
			return nil, fmt.Errorf("declared %s %s was not created", declared[name], name)
		}
	}
	return objects, nil
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
