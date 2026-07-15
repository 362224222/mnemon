package event

import (
	"errors"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestValidateContentPreservesNaturalTextAndBoundsIt(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", " review\n\tresult "} {
		if err := validateContent("content", value, false); err != nil {
			t.Fatalf("validateContent(%q) error = %v", value, err)
		}
	}
	for _, test := range []struct {
		name  string
		value string
		cause error
	}{
		{"required", "", model.ErrInvalid},
		{"invalid UTF-8", string([]byte{0xff}), model.ErrInvalid},
		{"control", "bad\x00text", model.ErrInvalid},
		{"limit", strings.Repeat("x", model.MaxContentBytes+1), model.ErrLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateContent("content", test.value, true)
			if !errors.Is(err, ErrInvalidCandidate) || !errors.Is(err, test.cause) {
				t.Fatalf("validateContent() error = %v, want candidate + %v", err, test.cause)
			}
		})
	}
}

func TestValidateTokenRejectsAuthorityLikeFreeText(t *testing.T) {
	t.Parallel()

	if err := validateToken("decision", "work-conflict"); err != nil {
		t.Fatalf("validateToken() error = %v", err)
	}
	for _, value := range []string{"", "two words", "line\nbreak"} {
		if err := validateToken("decision", value); !errors.Is(err, ErrInvalidCandidate) {
			t.Fatalf("validateToken(%q) error = %v", value, err)
		}
	}
}
