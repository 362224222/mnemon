package agency

import (
	"bytes"
	"errors"
	"testing"
)

func TestAgentArtifactTextKeepsOneSmallSafeProjection(t *testing.T) {
	valid := []byte("# Guide\n\nplain UTF-8 文本\twith structure\r\n")
	if err := ValidateAgentArtifactText(valid); err != nil {
		t.Fatalf("valid Agent Artifact text = %v", err)
	}
	for name, content := range map[string][]byte{
		"invalid UTF-8":      {0xff},
		"terminal escape":    []byte("before\x1b[31mafter"),
		"formatting control": []byte("before\u202eafter"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateAgentArtifactText(content); !errors.Is(err, ErrInvalid) {
				t.Fatalf("ValidateAgentArtifactText() = %v, want ErrInvalid", err)
			}
		})
	}
	tooLarge := bytes.Repeat([]byte{'a'}, MaxAgentArtifactReadBytes+1)
	if err := ValidateAgentArtifactText(tooLarge); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized Agent Artifact text = %v, want ErrLimit", err)
	}
}
