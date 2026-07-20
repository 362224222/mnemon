package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type testProfileCredentials struct{}

func (testProfileCredentials) Ensure(nodeState string) (model.Digest, bool, error) {
	return localapi.EnsureProfileCredential(nodeState)
}

func (testProfileCredentials) Verify(nodeState string, expected model.Digest) error {
	return localapi.VerifyProfileCredential(nodeState, expected)
}

func TestInspectAuthorityReadsActiveAndDisabledExistingState(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		enabled := enabled
		t.Run(map[bool]string{false: "disabled", true: "active"}[enabled], func(t *testing.T) {
			fixture := newDaemonFixture(t, enabled)
			snapshot, err := InspectAuthority(context.Background(), fixture.workspace,
				testProfileCredentials{})
			if err != nil || snapshot.Enabled != enabled || snapshot.Host != fixture.profile.Host() ||
				snapshot.Runtime != fixture.profile.Runtime() ||
				snapshot.AssetRevision != fixture.profile.ActiveAssetRevision() ||
				!snapshot.UpdatedAt.Equal(fixture.profile.UpdatedAt()) ||
				snapshot.PeerID != fixture.identity.PeerID() ||
				snapshot.ActiveAssetRevision != fixture.revision {
				t.Fatalf("InspectAuthority() = (%#v, %v)", snapshot, err)
			}
			st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
			if err != nil {
				t.Fatalf("inspection retained Store writer: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInspectAuthorityRejectsActiveWriterExplicitly(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = InspectAuthority(context.Background(), fixture.workspace, testProfileCredentials{})
	if !errors.Is(err, ErrAuthorityInspection) || !errors.Is(err, store.ErrWriterActive) {
		t.Fatalf("active writer inspection error = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectAuthority(context.Background(), fixture.workspace,
		testProfileCredentials{}); err != nil {
		t.Fatalf("inspection after writer release: %v", err)
	}
}

func TestInspectAuthorityRejectsIdentityAndCredentialDriftAndReleasesStore(t *testing.T) {
	t.Run("credential", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		writeDaemonToken(t, fixture.nodeState, bytes.Repeat([]byte{0x29}, 32), true)
		if _, err := InspectAuthority(context.Background(), fixture.workspace,
			testProfileCredentials{}); !errors.Is(err, ErrAuthorityInspection) {
			t.Fatalf("credential drift error = %v", err)
		}
		st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatalf("failed inspection retained Store writer: %v", err)
		}
		_ = st.Close()
	})
	t.Run("identity", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		if err := os.Remove(filepath.Join(fixture.nodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		replacement, err := EnsureIdentity(fixture.nodeState)
		if err != nil || replacement.PeerID() == fixture.identity.PeerID() {
			t.Fatalf("replacement identity = (%v, %v)", replacement, err)
		}
		if _, err := InspectAuthority(context.Background(), fixture.workspace,
			testProfileCredentials{}); !errors.Is(err, ErrAuthorityInspection) {
			t.Fatalf("identity drift error = %v", err)
		}
	})
}

func TestInspectAuthorityNeverCreatesOrRepairsStateAndHonorsContext(t *testing.T) {
	workspace := newDaemonWorkspace(t)
	if _, err := InspectAuthority(context.Background(), workspace,
		testProfileCredentials{}); !errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("missing authority error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".mnemon")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection created workspace state: %v", err)
	}

	fixture := newDaemonFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectAuthority(ctx, fixture.workspace,
		testProfileCredentials{}); !errors.Is(err, context.Canceled) ||
		!errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("cancelled inspection error = %v", err)
	}
	if _, err := InspectAuthority(context.Background(), ".",
		testProfileCredentials{}); !errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("relative inspection error = %v", err)
	}
	if snapshot, err := InspectAuthority(context.Background(), fixture.workspace,
		testProfileCredentials{}); err != nil ||
		snapshot.PeerID.IsZero() {
		t.Fatalf("post-cancellation inspection = (%#v, %v)", snapshot, err)
	}
}

func TestAuthorityRejectsNoncanonicalTimeRepresentation(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	authority, err := InspectAuthority(context.Background(), fixture.workspace,
		testProfileCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.Validate(); err != nil {
		t.Fatalf("canonical authority validation = %v", err)
	}
	authority.UpdatedAt = authority.UpdatedAt.In(time.FixedZone("noncanonical-utc", 0))
	if err := authority.Validate(); err == nil {
		t.Fatal("Authority accepted a semantically equal but noncanonical time representation")
	}
	if _, err := authority.Digest(); err == nil {
		t.Fatal("Authority digest accepted a noncanonical time representation")
	}
}

func TestAuthorityDigestBindsCanonicalLifecycleGeneration(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	base, err := InspectAuthority(context.Background(), fixture.workspace,
		testProfileCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	want, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if repeated, err := base.Digest(); err != nil || repeated != want {
		t.Fatalf("repeated Digest() = (%s, %v), want %s", repeated, err, want)
	}
	otherPeer, err := model.ParsePeerID("peer-authority-other")
	if err != nil {
		t.Fatal(err)
	}
	otherRevision := model.Sum([]byte("other-authority-assets")).String()
	mutations := []struct {
		name   string
		mutate func(*Authority)
	}{
		{name: "Host and Runtime", mutate: func(value *Authority) {
			value.Host, value.Runtime = model.HostClaudeCode, model.RuntimeClaudeCLI
		}},
		{name: "Enabled", mutate: func(value *Authority) { value.Enabled = !value.Enabled }},
		{name: "asset revisions", mutate: func(value *Authority) {
			value.AssetRevision, value.ActiveAssetRevision = otherRevision, otherRevision
		}},
		{name: "UpdatedAt", mutate: func(value *Authority) {
			value.UpdatedAt = value.UpdatedAt.Add(time.Nanosecond)
		}},
		{name: "PeerID", mutate: func(value *Authority) { value.PeerID = otherPeer }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.mutate(&changed)
			got, err := changed.Digest()
			if err != nil || got == want {
				t.Fatalf("mutated Digest() = (%s, %v), base %s", got, err, want)
			}
		})
	}
	record := authorityDigestRecord{ActiveAssetRevision: base.ActiveAssetRevision,
		AssetRevision: base.AssetRevision, Domain: "mnemon.node.authority.other",
		Enabled: base.Enabled, Host: string(base.Host), PeerID: base.PeerID.String(),
		Runtime: string(base.Runtime), UpdatedAt: base.UpdatedAt.Format(time.RFC3339Nano)}
	raw, err := model.CanonicalMarshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if model.Sum(raw) == want {
		t.Fatal("Authority digest is not separated from another domain")
	}
}

func TestAuthorityValidateRejectsEveryInvalidInvariant(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	base, err := InspectAuthority(context.Background(), fixture.workspace,
		testProfileCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*Authority)
	}{
		{name: "Host", mutate: func(value *Authority) { value.Host = "unknown" }},
		{name: "Runtime", mutate: func(value *Authority) { value.Runtime = model.RuntimeClaudeCLI }},
		{name: "PeerID", mutate: func(value *Authority) { value.PeerID = model.PeerID{} }},
		{name: "Profile revision", mutate: func(value *Authority) { value.AssetRevision = "bad" }},
		{name: "Node revision", mutate: func(value *Authority) { value.ActiveAssetRevision = "bad" }},
		{name: "revision mismatch", mutate: func(value *Authority) {
			value.ActiveAssetRevision = model.Sum([]byte("mismatch")).String()
		}},
		{name: "zero time", mutate: func(value *Authority) { value.UpdatedAt = time.Time{} }},
		{name: "noncanonical time", mutate: func(value *Authority) {
			value.UpdatedAt = value.UpdatedAt.In(time.FixedZone("alternate-utc", 0))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := base
			test.mutate(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("Validate() accepted invalid authority")
			}
			if _, err := invalid.Digest(); err == nil {
				t.Fatal("Digest() accepted invalid authority")
			}
		})
	}
}

func TestTypedNilCredentialPortsFailClosedBeforeInvocation(t *testing.T) {
	var credentials *panicProfileCredentials
	workspace := newDaemonWorkspace(t)
	revision := model.Sum([]byte("typed-nil-credential-assets")).String()
	if result, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: revision, Credentials: credentials}); !errors.Is(err, ErrProvision) || result != (ProvisionResult{}) {
		t.Fatalf("Provision(typed nil) = (%#v, %v)", result, err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".mnemon")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("typed-nil Provision created state: %v", err)
	}
	fixture := newDaemonFixture(t, true)
	if _, err := InspectAuthority(context.Background(), fixture.workspace, credentials); !errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("InspectAuthority(typed nil) = %v", err)
	}
	if _, err := NewDaemonPreflight(DaemonPreflightOptions{Workspace: fixture.workspace,
		NodeState: fixture.nodeState, AssetRevision: fixture.revision,
		Install: fixture.install, Credentials: credentials}); !errors.Is(err, ErrDaemonPreflight) {
		t.Fatalf("NewDaemonPreflight(typed nil) = %v", err)
	}
	if _, err := Activate(context.Background(), ActivateOptions{Credentials: credentials}); !errors.Is(err, ErrActivate) {
		t.Fatalf("Activate(typed nil) = %v", err)
	}
	if _, err := Deactivate(context.Background(), DeactivateOptions{Credentials: credentials}); !errors.Is(err, ErrDeactivate) {
		t.Fatalf("Deactivate(typed nil) = %v", err)
	}
	if _, err := ConfirmOfflineAuthority(context.Background(), fixture.workspace,
		model.Sum([]byte("expected")), credentials, func(context.Context, string) (bool, error) {
			return false, nil
		}); !errors.Is(err, ErrOfflineAuthority) {
		t.Fatalf("ConfirmOfflineAuthority(typed nil) = %v", err)
	}
	if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install, Credentials: credentials,
		Control: newTestControlTransportFactory()}); daemon != nil ||
		!errors.Is(err, ErrDaemonAuthority) {
		t.Fatalf("OpenDaemon(typed nil) = (%v, %v)", daemon, err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

type panicProfileCredentials struct{}

func (*panicProfileCredentials) Ensure(string) (model.Digest, bool, error) {
	panic("typed-nil credential provisioner must be rejected before invocation")
}

func (*panicProfileCredentials) Verify(string, model.Digest) error {
	panic("typed-nil credential verifier must be rejected before invocation")
}
