package node

import (
	"context"
	"errors"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryAuthoritySurvivesLostIdentityAndCredentialButStillRequiresWriter(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	expected := daemonFixtureAuthorityResponse(t, fixture)
	credentialPath := filepath.Join(fixture.nodeState, "profiles",
		model.TeamworkProfileID().String()+".token")
	for _, path := range []string{filepath.Join(fixture.nodeState, identityKeyName), credentialPath} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := InspectAuthority(context.Background(), fixture.workspace); err == nil {
		t.Fatal("normal authority inspection accepted lost identity and credential")
	}
	snapshot, err := InspectRecoveryAuthority(context.Background(), fixture.workspace)
	if err != nil || snapshot.PeerID != fixture.identity.PeerID() {
		t.Fatalf("InspectRecoveryAuthority() = (%#v, %v)", snapshot, err)
	}
	digest, err := AuthorityDigest(expected)
	if err != nil {
		t.Fatal(err)
	}
	confirmed, err := ConfirmRecoveryOfflineAuthority(context.Background(), fixture.workspace, digest)
	if err != nil || confirmed != expected {
		t.Fatalf("ConfirmRecoveryOfflineAuthority() = (%#v, %v)", confirmed, err)
	}
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := ConfirmRecoveryOfflineAuthority(context.Background(), fixture.workspace,
		digest); !errors.Is(err, ErrOfflineAuthorityActive) {
		t.Fatalf("writer-active recovery confirmation = %v", err)
	}
}
