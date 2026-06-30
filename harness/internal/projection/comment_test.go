package projection

import (
	"strings"
	"testing"
)

func TestFormatCommentCarriesStableMarkers(t *testing.T) {
	got := FormatComment(CommentMaterial{
		Title:        "assignment finished",
		Body:         "Worker reported passing evidence.",
		EventIDs:     []string{"ev-1", "ev-1", "ev-2"},
		EventType:    "progress_digest.accepted",
		SessionID:    "session-1",
		AssignmentID: "asg-1",
	})
	for _, want := range []string{
		"Mnemon update: assignment finished",
		"Worker reported passing evidence.",
		"mnemon:event=ev-1",
		"mnemon:event=ev-2",
		"mnemon:type=progress_digest.accepted",
		"mnemon:session=session-1",
		"mnemon:assignment=asg-1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("projection comment missing %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "mnemon:event=ev-1") != 1 {
		t.Fatalf("projection comment should dedupe markers:\n%s", got)
	}
}
