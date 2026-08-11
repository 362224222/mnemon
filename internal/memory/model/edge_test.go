package model

import (
	"testing"
)

func TestMetadataJSON_Roundtrip(t *testing.T) {
	e := &Edge{Metadata: map[string]string{"sub_type": "backbone", "direction": "precedes"}}
	json := e.MetadataJSON()

	var restored Edge
	restored.ParseMetadata(json)

	if len(restored.Metadata) != 2 {
		t.Fatalf("want 2 metadata entries, got %d", len(restored.Metadata))
	}
	if restored.Metadata["sub_type"] != "backbone" {
		t.Errorf("sub_type: want backbone, got %q", restored.Metadata["sub_type"])
	}
	if restored.Metadata["direction"] != "precedes" {
		t.Errorf("direction: want precedes, got %q", restored.Metadata["direction"])
	}
}

func TestMetadataJSON_Empty(t *testing.T) {
	e := &Edge{Metadata: map[string]string{}}
	json := e.MetadataJSON()

	var restored Edge
	restored.ParseMetadata(json)
	if len(restored.Metadata) != 0 {
		t.Errorf("want empty metadata, got %v", restored.Metadata)
	}
}

func TestParseMetadata_NilHandling(t *testing.T) {
	var e Edge
	e.ParseMetadata("null")
	if e.Metadata == nil {
		t.Error("ParseMetadata should initialize nil to empty map")
	}
}

func TestParseMetadata_InvalidJSON(t *testing.T) {
	var e Edge
	e.ParseMetadata("not json")
	if e.Metadata == nil {
		t.Error("ParseMetadata should initialize to empty map on invalid JSON")
	}
}

func TestValidEdgeTypes(t *testing.T) {
	expected := []EdgeType{EdgeTemporal, EdgeSemantic, EdgeCausal, EdgeEntity}
	for _, et := range expected {
		if !ValidEdgeTypes[et] {
			t.Errorf("edge type %q should be valid", et)
		}
	}
	if ValidEdgeTypes["narrative"] {
		t.Error("narrative edge type should not be valid")
	}
}
