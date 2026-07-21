package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorkDeadlineAuthorityRejectsNonControllerBatchBeforeDurableRead(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := validateWorkDeadlineAdmissionAuthority(context.Background(), tx,
		LocalAcceptanceSpec{}, time.Now().UTC()); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("validateWorkDeadlineAdmissionAuthority() error = %v", err)
	}
}
