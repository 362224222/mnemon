package multica

import "testing"

func TestMergeIssueMetadataUsesListedValuesAsAuthoritative(t *testing.T) {
	base := map[string]any{
		MulticaMetadataSurfaceRole: "input",
		MulticaMetadataEventRef:    "embedded-event",
		"local":                    7,
	}
	listed := map[string]string{
		MulticaMetadataSurfaceRole: string(SurfaceRoleDisplay),
		MulticaMetadataResourceRef: "progress_digest/prog-1",
		"":                         "ignored",
	}
	got := MergeIssueMetadata(base, listed)
	if got[MulticaMetadataSurfaceRole] != string(SurfaceRoleDisplay) {
		t.Fatalf("listed metadata should override embedded value: %+v", got)
	}
	if got[MulticaMetadataEventRef] != "embedded-event" || got["local"] != 7 {
		t.Fatalf("base metadata not preserved: %+v", got)
	}
	if got[MulticaMetadataResourceRef] != "progress_digest/prog-1" {
		t.Fatalf("listed metadata not merged: %+v", got)
	}
	if _, ok := got[""]; ok {
		t.Fatalf("empty listed key should be ignored: %+v", got)
	}
	if base[MulticaMetadataSurfaceRole] != "input" {
		t.Fatalf("base metadata mutated: %+v", base)
	}
}
