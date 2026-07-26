package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestManagedInstallationVerifiesExactProjectionAndReportsDrift(t *testing.T) {
	workspace, nodeState, bundle := newManagedInstallationWorkspace(t)
	if _, err := InstallNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	installation, err := NewManagedInstallation(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if installation.Revision() != bundle.Manifest().AssetRevision {
		t.Fatalf("Revision() = %q, want %q", installation.Revision(), bundle.Manifest().AssetRevision)
	}
	profile := newManagedInstallationProfile(t, workspace, model.HostCodex,
		installation.Revision())
	if err := installation.Verify(context.Background(), profile); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	skill := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
	if err := os.WriteFile(skill, []byte("drift\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := installation.Verify(context.Background(), profile); !errors.Is(err, ErrProjectionConflict) {
		t.Fatalf("Verify(drift) error = %v", err)
	}
	content, err := os.ReadFile(skill)
	if err != nil || string(content) != "drift\n" {
		t.Fatalf("Verify repaired drift: content %q, error %v", content, err)
	}
}

func TestManagedInstallationDelegatesFrozenTeamworkActions(t *testing.T) {
	workspace, _, bundle := newManagedInstallationWorkspace(t)
	installation, err := NewManagedInstallationFromBundle(workspace, bundle)
	if err != nil {
		t.Fatal(err)
	}
	wantPaths := bundle.TeamworkActionPaths()
	paths := installation.TeamworkActionPaths()
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("TeamworkActionPaths() = %v, want %v", paths, wantPaths)
	}
	paths[0] = "tampered"
	if !reflect.DeepEqual(installation.TeamworkActionPaths(), wantPaths) {
		t.Fatal("TeamworkActionPaths() exposed mutable bundle state")
	}
	for _, path := range wantPaths {
		want, wantErr := bundle.ReadTeamworkAction(path)
		got, gotErr := installation.ReadTeamworkAction(path)
		if wantErr != nil || gotErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("ReadTeamworkAction(%q) = (%q, %v), bundle = (%q, %v)",
				path, got, gotErr, want, wantErr)
		}
		got[0] ^= 0xff
		fresh, freshErr := installation.ReadTeamworkAction(path)
		if freshErr != nil || bytes.Equal(got, fresh) {
			t.Fatalf("ReadTeamworkAction(%q) returned mutable bytes: %v", path, freshErr)
		}
	}
	if raw, readErr := installation.ReadTeamworkAction("SKILL.md"); !errors.Is(readErr, ErrManagedInstallation) || raw != nil {
		t.Fatalf("ReadTeamworkAction(non-action) = (%q, %v)", raw, readErr)
	}

	var nilInstallation *ManagedInstallation
	if nilInstallation.Revision() != "" || nilInstallation.TeamworkActionPaths() != nil {
		t.Fatal("nil ManagedInstallation exposed frozen action authority")
	}
	if raw, readErr := nilInstallation.ReadTeamworkAction(wantPaths[0]); !errors.Is(readErr, ErrManagedInstallation) || raw != nil {
		t.Fatalf("nil ReadTeamworkAction() = (%q, %v)", raw, readErr)
	}
}

func TestManagedInstallationFromBundleRejectsMissingAuthority(t *testing.T) {
	workspace, _, _ := newManagedInstallationWorkspace(t)
	if installation, err := NewManagedInstallationFromBundle(workspace, assets.Bundle{}); installation != nil ||
		!errors.Is(err, ErrManagedInstallation) {
		t.Fatalf("NewManagedInstallationFromBundle(zero) = (%#v, %v)", installation, err)
	}
}

func TestManagedInstallationRejectsCancelledAndDifferentProfileAuthority(t *testing.T) {
	workspace, _, bundle := newManagedInstallationWorkspace(t)
	inspectCalls := 0
	installation, err := newManagedInstallation(workspace, bundle,
		func(context.Context, assets.Host) (HostObservation, error) {
			inspectCalls++
			return HostObservation{Host: assets.HostCodex, Executable: "/test/codex"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	profile := newManagedInstallationProfile(t, workspace, model.HostCodex,
		installation.Revision())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := installation.Verify(ctx, model.Profile{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify(cancelled) error = %v", err)
	}
	if _, err := installation.RuntimeExecutable(ctx, profile); !errors.Is(err, context.Canceled) {
		t.Fatalf("RuntimeExecutable(cancelled) error = %v", err)
	}
	if inspectCalls != 0 {
		t.Fatalf("cancelled calls reached Host inspector %d times", inspectCalls)
	}

	otherWorkspace, _, _ := newManagedInstallationWorkspace(t)
	tests := []struct {
		name   string
		change func(*model.ProfileSpec)
	}{
		{name: "disabled", change: func(spec *model.ProfileSpec) {
			spec.Enabled = false
		}},
		{name: "workspace", change: func(spec *model.ProfileSpec) {
			spec.WorkspaceRoot = otherWorkspace
		}},
		{name: "revision", change: func(spec *model.ProfileSpec) {
			spec.ActiveAssetRevision = model.Sum([]byte("different revision")).String()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := profile.Spec()
			test.change(&spec)
			changed, profileErr := model.NewProfile(spec)
			if profileErr != nil {
				t.Fatal(profileErr)
			}
			if err := installation.Verify(context.Background(), changed); !errors.Is(err, ErrManagedInstallation) {
				t.Fatalf("Verify() error = %v", err)
			}
		})
	}
}

func TestManagedInstallationResolvesOnlyTheProfileHostRuntime(t *testing.T) {
	workspace, _, bundle := newManagedInstallationWorkspace(t)
	wantExecutable := "/test/codex"
	var inspected assets.Host
	installation, err := newManagedInstallation(workspace, bundle,
		func(_ context.Context, host assets.Host) (HostObservation, error) {
			inspected = host
			return HostObservation{Host: host, Executable: wantExecutable, Version: "test"}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	profile := newManagedInstallationProfile(t, workspace, model.HostCodex,
		installation.Revision())
	executable, err := installation.RuntimeExecutable(context.Background(), profile)
	if err != nil || executable != wantExecutable || inspected != assets.HostCodex {
		t.Fatalf("RuntimeExecutable() = (%q, %v), inspected %q", executable, err, inspected)
	}

	installation.inspectHost = func(context.Context, assets.Host) (HostObservation, error) {
		return HostObservation{Host: assets.Host("unsupported"), Executable: "/test/other"}, nil
	}
	if executable, err := installation.RuntimeExecutable(context.Background(), profile); !errors.Is(err, ErrManagedInstallation) || executable != "" {
		t.Fatalf("RuntimeExecutable(mismatched Host) = (%q, %v)", executable, err)
	}
}

func TestManagedInstallationRequiresAPhysicalWorkspace(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	physical, _, _ := newManagedInstallationWorkspace(t)
	linked := filepath.Join(t.TempDir(), "workspace")
	if err := os.Symlink(physical, linked); err != nil {
		t.Fatal(err)
	}
	for _, workspace := range []string{"relative", linked} {
		if installation, err := newManagedInstallation(workspace, bundle, InspectHost); !errors.Is(err, ErrManagedInstallation) || installation != nil {
			t.Fatalf("newManagedInstallation(%q) = (%#v, %v)", workspace, installation, err)
		}
	}
}

func newManagedInstallationWorkspace(t *testing.T) (string, string, assets.Bundle) {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		filepath.Join(workspace, ".mnemon"),
		filepath.Join(workspace, ".mnemon", "harness"),
		nodeState,
	} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	return workspace, nodeState, bundle
}

func newManagedInstallationProfile(t *testing.T, workspace string,
	host model.HostKind, revision string,
) model.Profile {
	t.Helper()
	runtimeKind, ok := model.RuntimeForHost(host)
	if !ok {
		t.Fatalf("test Host %q has no Runtime", host)
	}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	profile, err := model.NewProfile(model.ProfileSpec{
		ID: model.TeamworkProfileID(), Principal: "managed-installation-test",
		WorkspaceRoot: workspace, Host: host, Runtime: runtimeKind,
		CredentialHash:      model.Sum([]byte("managed installation credential")),
		ActiveAssetRevision: revision, HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: true, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}
