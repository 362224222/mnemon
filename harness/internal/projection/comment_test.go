package projection

import (
	"strings"
	"testing"
)

func TestFormatCommentCarriesStableMarkers(t *testing.T) {
	got := FormatComment(CommentMaterial{
		Title:    "assignment finished",
		Body:     "Worker reported passing evidence.",
		EventIDs: []string{"ev-1", "ev-1", "ev-2"},
	})
	for _, want := range []string{
		"Mnemon update: assignment finished",
		"Worker reported passing evidence.",
		"mnemon:event=ev-1",
		"mnemon:event=ev-2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("projection comment missing %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "mnemon:event=ev-1") != 1 {
		t.Fatalf("projection comment should dedupe markers:\n%s", got)
	}
}
