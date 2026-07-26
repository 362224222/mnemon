package node

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestDeactivateWithdrawsExactAuthorityAndReplays(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	active, err := Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle,
		model.HostCodex))
	if err != nil {
		t.Fatal(err)
	}
	at := active.Profile.UpdatedAt().Add(time.Second)
	options := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, ExpectedUpdatedAt: active.Profile.UpdatedAt(),
		Clock: controllerTestClock{at}}

	first, err := Deactivate(context.Background(), options)
	if err != nil || !first.Changed || first.Profile.Enabled() ||
		first.Profile.Host() != model.HostCodex ||
		first.Node.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
		t.Fatalf("Deactivate() = (%#v, %v)", first, err)
	}
	options.Clock = controllerTestClock{at.Add(time.Hour)}
	options.ExpectedUpdatedAt = first.Profile.UpdatedAt()
	second, err := Deactivate(context.Background(), options)
	if err != nil || second.Changed || second.Profile.Enabled() ||
		!second.Profile.UpdatedAt().Equal(first.Profile.UpdatedAt()) ||
		!second.Node.UpdatedAt().Equal(first.Node.UpdatedAt()) {
		t.Fatalf("replayed Deactivate() = (%#v, %v)", second, err)
	}
}

func TestDeactivateAllowsDriftedProjectionButRejectsAuthorityDrift(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	active, err := Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle,
		model.HostCodex))
	if err != nil {
		t.Fatal(err)
	}
	guide := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md")
	if err := os.Remove(guide); err != nil {
		t.Fatal(err)
	}
	options := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: active.Profile.UpdatedAt(),
		Clock:             controllerTestClock{active.Profile.UpdatedAt().Add(time.Second)}}
	if result, err := Deactivate(context.Background(), options); err != nil || !result.Changed || result.Profile.Enabled() {
		t.Fatalf("projection-drift Deactivate() = (%#v, %v)", result, err)
	}

	workspace, provisioned, bundle = activeTestProvision(t)
	active, err = Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle,
		model.HostCodex))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*DeactivateOptions){
		"Host":     func(value *DeactivateOptions) { value.Host = model.HostKind("claude-code") },
		"revision": func(value *DeactivateOptions) { value.AssetRevision = model.Sum([]byte("other-assets")).String() },
		"generation": func(value *DeactivateOptions) {
			value.ExpectedUpdatedAt = value.ExpectedUpdatedAt.Add(-time.Nanosecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			options := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
				AssetRevision:     bundle.Manifest().AssetRevision,
				ExpectedUpdatedAt: active.Profile.UpdatedAt(),
				Clock:             controllerTestClock{active.Profile.UpdatedAt().Add(time.Second)}}
			mutate(&options)
			if _, err := Deactivate(context.Background(), options); !errors.Is(err, ErrDeactivate) {
				t.Fatalf("Deactivate() error = %v", err)
			}
		})
	}
}

func TestDeactivateRejectsIdentityAndCredentialDrift(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		drift func(*testing.T, string)
	}{
		{name: "identity", drift: func(t *testing.T, nodeState string) {
			if err := os.Remove(filepath.Join(nodeState, identityKeyName)); err != nil {
				t.Fatal(err)
			}
			if _, err := EnsureIdentity(nodeState); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "credential", drift: func(t *testing.T, nodeState string) {
			raw := append([]byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x43}, 32))), '\n')
			path := filepath.Join(nodeState, "profiles", model.TeamworkProfileID().String()+".token")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			workspace, provisioned, bundle := activeTestProvision(t)
			active, err := Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle,
				model.HostCodex))
			if err != nil {
				t.Fatal(err)
			}
			test.drift(t, provisioned.NodeState)
			options := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
				AssetRevision:     bundle.Manifest().AssetRevision,
				ExpectedUpdatedAt: active.Profile.UpdatedAt(),
				Clock:             controllerTestClock{active.Profile.UpdatedAt().Add(time.Second)}}
			if _, err := Deactivate(context.Background(), options); !errors.Is(err, ErrDeactivate) {
				t.Fatalf("Deactivate() error = %v", err)
			}
		})
	}
}

func TestDeactivateRejectsInvalidExpectedGeneration(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	active, err := Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle,
		model.HostCodex))
	if err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]time.Time{
		"zero":              {},
		"before Unix epoch": time.Unix(-1, 0),
	} {
		t.Run(name, func(t *testing.T) {
			options := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
				AssetRevision: bundle.Manifest().AssetRevision, ExpectedUpdatedAt: expected,
				Clock: controllerTestClock{active.Profile.UpdatedAt().Add(time.Second)}}
			if _, err := Deactivate(context.Background(), options); !errors.Is(err, ErrDeactivate) {
				t.Fatalf("Deactivate() error = %v", err)
			}
		})
	}
	equalClock := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, ExpectedUpdatedAt: active.Profile.UpdatedAt(),
		Clock: controllerTestClock{active.Profile.UpdatedAt()}}
	if _, err := Deactivate(context.Background(), equalClock); !errors.Is(err, ErrDeactivate) {
		t.Fatalf("equal-clock Deactivate() error = %v", err)
	}
}

func TestDeactivateRejectsReactivatedAuthorityABA(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	active, err := Activate(context.Background(), activeTestOptions(workspace, provisioned, bundle,
		model.HostCodex))
	if err != nil {
		t.Fatal(err)
	}
	deactivated, err := Deactivate(context.Background(), DeactivateOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: active.Profile.UpdatedAt(),
		Clock:             controllerTestClock{active.Profile.UpdatedAt().Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	reactivate := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
	reactivate.ExpectedUpdatedAt = deactivated.Profile.UpdatedAt()
	reactivate.Clock = controllerTestClock{deactivated.Profile.UpdatedAt().Add(time.Second)}
	reactivated, err := Activate(context.Background(), reactivate)
	if err != nil {
		t.Fatal(err)
	}
	stale := DeactivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, ExpectedUpdatedAt: active.Profile.UpdatedAt(),
		Clock: controllerTestClock{reactivated.Profile.UpdatedAt().Add(time.Second)}}
	if _, err := Deactivate(context.Background(), stale); !errors.Is(err, ErrDeactivate) {
		t.Fatalf("stale ABA Deactivate() error = %v", err)
	}
}
