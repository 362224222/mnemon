package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestWorkDeadlineScopeRejectsUnknownChannelWithoutPublishingAuthority(t *testing.T) {
	st := openTestStore(t)
	insertNode(t, st.db)
	insertProfile(t, st.db)
	channel, _ := model.ParseChannelID("channel-deadline-missing")
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := prepareWorkDeadlineScopeTx(context.Background(), tx, channel, 1); err == nil || errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("prepareWorkDeadlineScopeTx() error = %v", err)
	}
}
