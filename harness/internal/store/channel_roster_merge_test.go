package store

import (
	"context"
	"errors"
	"testing"
)

func TestMergeChannelRosterRejectsIncompleteAuthority(t *testing.T) {
	t.Parallel()
	var nilStore *Store
	if _, err := nilStore.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{}); !errors.Is(err,
		ErrChannelRosterInput) {
		t.Fatalf("nil Store merge error = %v", err)
	}
	st := openTestStore(t)
	if _, err := st.MergeChannelRoster(nil, MergeChannelRosterSpec{}); !errors.Is(err,
		ErrChannelRosterInput) {
		t.Fatalf("nil context merge error = %v", err)
	}
	if _, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{}); !errors.Is(err,
		ErrChannelRosterInput) {
		t.Fatalf("empty merge error = %v", err)
	}
}
