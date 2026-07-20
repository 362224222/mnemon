package store

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPublicationEvidenceDispositionRejectsOpenOrContradictoryState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		status, diagnostic string
	}{
		{status: "scanned"},
		{status: "pending", diagnostic: "unexpected"},
		{status: "blocked"},
	} {
		if err := insertPublicationEvidenceDisposition(context.Background(), nil, model.Event{},
			test.status, test.diagnostic); err == nil {
			t.Fatalf("disposition (%q,%q) was accepted", test.status, test.diagnostic)
		}
	}
}
