package store

import (
	"context"
	"errors"
	"testing"
)

func TestAgentDeadRecoveryRejectsIncompleteSetupAuthority(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	if _, err := st.RecoverDeadAgentHandlings(context.Background(), AgentDeadRecoverySpec{}); !errors.Is(err, ErrAgentDeadRecoveryInput) {
		t.Fatalf("RecoverDeadAgentHandlings() error = %v", err)
	}
}
