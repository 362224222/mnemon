package model

import (
	"testing"
)

func TestTagsJSON_Roundtrip(t *testing.T) {
	ins := &Insight{Tags: []string{"go", "memory", "graph"}}
	json := ins.TagsJSON()

	var restored Insight
	restored.ParseTags(json)

	if len(restored.Tags) != 3 {
		t.Fatalf("want 3 tags, got %d", len(restored.Tags))
	}
	for i, want := range []string{"go", "memory", "graph"} {
		if restored.Tags[i] != want {
			t.Errorf("tag[%d]: want %q, got %q", i, want, restored.Tags[i])
		}
	}
}

func TestTagsJSON_Empty(t *testing.T) {
	ins := &Insight{Tags: []string{}}
	json := ins.TagsJSON()

	var restored Insight
	restored.ParseTags(json)
	if len(restored.Tags) != 0 {
		t.Errorf("want empty tags, got %v", restored.Tags)
	}
}

func TestParseTags_NilHandling(t *testing.T) {
	var ins Insight
	ins.ParseTags("null")
	if ins.Tags == nil {
		t.Error("ParseTags should initialize nil to empty slice")
	}
	if len(ins.Tags) != 0 {
		t.Errorf("want 0 tags, got %d", len(ins.Tags))
	}
}

func TestParseTags_InvalidJSON(t *testing.T) {
	var ins Insight
	ins.ParseTags("not json")
	if ins.Tags == nil {
		t.Error("ParseTags should initialize to empty slice on invalid JSON")
	}
}

func TestEntitiesJSON_Roundtrip(t *testing.T) {
	ins := &Insight{Entities: []string{"Go", "SQLite", "MAGMA"}}
	json := ins.EntitiesJSON()

	var restored Insight
	restored.ParseEntities(json)

	if len(restored.Entities) != 3 {
		t.Fatalf("want 3 entities, got %d", len(restored.Entities))
	}
	for i, want := range []string{"Go", "SQLite", "MAGMA"} {
		if restored.Entities[i] != want {
			t.Errorf("entity[%d]: want %q, got %q", i, want, restored.Entities[i])
		}
	}
}

func TestParseEntities_NilHandling(t *testing.T) {
	var ins Insight
	ins.ParseEntities("null")
	if ins.Entities == nil {
		t.Error("ParseEntities should initialize nil to empty slice")
	}
}

func TestValidCategories(t *testing.T) {
	expected := []Category{
		CategoryPreference, CategoryDecision, CategoryFact,
		CategoryInsight, CategoryContext, CategoryGeneral,
	}
	for _, c := range expected {
		if !ValidCategories[c] {
			t.Errorf("category %q should be valid", c)
		}
	}
	if ValidCategories["bogus"] {
		t.Error("bogus category should not be valid")
	}
}
