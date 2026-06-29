package multica

import "testing"

func TestMergeIssueMetadataUsesListedValuesAsAuthoritative(t *testing.T) {
	base := map[string]any{
		MulticaMetadataKind:      "embedded",
		MulticaMetadataSessionID: "embedded-session",
		"local":                  7,
	}
	listed := map[string]string{
		MulticaMetadataKind:          MulticaHubKindAssignmentMailbox,
		MulticaMetadataCorrelationID: "multica:issue:child-1",
		"":                           "ignored",
	}
	got := MergeIssueMetadata(base, listed)
	if got[MulticaMetadataKind] != MulticaHubKindAssignmentMailbox {
		t.Fatalf("listed metadata should override embedded value: %+v", got)
	}
	if got[MulticaMetadataSessionID] != "embedded-session" || got["local"] != 7 {
		t.Fatalf("base metadata not preserved: %+v", got)
	}
	if got[MulticaMetadataCorrelationID] != "multica:issue:child-1" {
		t.Fatalf("listed metadata not merged: %+v", got)
	}
	if _, ok := got[""]; ok {
		t.Fatalf("empty listed key should be ignored: %+v", got)
	}
	if base[MulticaMetadataKind] != "embedded" {
		t.Fatalf("base metadata mutated: %+v", base)
	}
}
