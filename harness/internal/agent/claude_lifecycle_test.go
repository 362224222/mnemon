package agent

import (
	"errors"
	"testing"
)

func TestClaudeCompletionStatusDistinguishesProtocolAndCleanup(t *testing.T) {
	failure := errors.New("failure")
	tests := []struct {
		completed bool
		cleanup   error
		run       error
		want      string
	}{
		{completed: true, want: "completed"},
		{completed: true, cleanup: failure, want: "cleanup_failed"},
		{completed: true, run: failure, want: "failed"},
		{run: failure, want: "failed"},
	}
	for _, test := range tests {
		if got := claudeCompletionStatus(test.completed, test.cleanup, test.run); got != test.want {
			t.Fatalf("claudeCompletionStatus(%t, %v, %v) = %q, want %q",
				test.completed, test.cleanup, test.run, got, test.want)
		}
	}
}
