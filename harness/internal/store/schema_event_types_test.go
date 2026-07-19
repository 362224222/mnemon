package store

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestSchemaV1EventTypesProjectFromModelDescriptors(t *testing.T) {
	t.Parallel()

	descriptors := model.EventTypeDescriptors()
	if len(descriptors) == 0 {
		t.Fatal("model EventType descriptor projection is empty")
	}
	values := make([]string, len(descriptors))
	seen := make(map[model.EventType]struct{}, len(descriptors))
	for index, descriptor := range descriptors {
		value := descriptor.Type()
		if !value.Valid() {
			t.Fatalf("descriptor %d has invalid EventType %q", index, value)
		}
		if _, duplicate := seen[value]; duplicate {
			t.Fatalf("descriptor projection repeats EventType %q", value)
		}
		seen[value] = struct{}{}
		values[index] = "'" + string(value) + "'"
	}
	want := "event_type         TEXT NOT NULL CHECK (event_type IN (\n    " +
		strings.Join(values, ",\n    ") + "\n  ))"
	if strings.Count(schemaV1, eventTypeValuesPlaceholder) != 1 {
		t.Fatalf("embedded schema EventType placeholder count = %d, want 1",
			strings.Count(schemaV1, eventTypeValuesPlaceholder))
	}
	ddl := schemaDDL()
	if strings.Contains(ddl, eventTypeValuesPlaceholder) || !strings.Contains(ddl, want) {
		t.Fatal("rendered schema does not contain the deterministic model EventType projection")
	}
}
