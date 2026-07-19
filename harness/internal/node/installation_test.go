package node

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type testActionInstallation struct {
	InstallationVerifier
	provider agent.ActionAssetProvider
}

type incompatibleActionProvider struct{ assets.Bundle }

func (provider incompatibleActionProvider) ReadTeamworkAction(path string) ([]byte, error) {
	raw, err := provider.Bundle.ReadTeamworkAction(path)
	if err == nil && path == "actions/teamwork/offer.json" {
		raw = bytes.Replace(raw, []byte(`"required":true`), []byte(`"required":false`), 1)
	}
	return raw, err
}

func (installation testActionInstallation) Revision() string {
	return installation.provider.Revision()
}

func (installation testActionInstallation) TeamworkActionPaths() []string {
	return installation.provider.TeamworkActionPaths()
}

func (installation testActionInstallation) ReadTeamworkAction(path string) ([]byte, error) {
	return installation.provider.ReadTeamworkAction(path)
}

func testInstallationWithActions(verify InstallationVerifier,
	provider agent.ActionAssetProvider,
) InstallationVerifier {
	return testActionInstallation{InstallationVerifier: verify, provider: provider}
}

func testInstallationWithVerifier(verify, authority InstallationVerifier) InstallationVerifier {
	provider, _ := authority.(agent.ActionAssetProvider)
	return testInstallationWithActions(verify, provider)
}

func TestInstallationVerifierUsesTheCallerContext(t *testing.T) {
	called := false
	verify := InstallationVerifierFunc(func(ctx context.Context, _ model.Profile) error {
		called = true
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verify.Verify(ctx, model.Profile{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify(cancelled) error = %v", err)
	}
	if called {
		t.Fatal("pre-cancelled verification reached the installation callback")
	}
	if err := verify.Verify(nil, model.Profile{}); err == nil {
		t.Fatal("Verify(nil) succeeded")
	}
}

func TestActionPolicyForInstallationRequiresTheSameRawAssetProvider(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	verify := InstallationVerifierFunc(func(context.Context, model.Profile) error { return nil })
	installation := testInstallationWithActions(verify, bundle)
	policy, err := actionPolicyForInstallation(installation)
	if err != nil || policy.AssetRevision().String() != bundle.Revision() {
		t.Fatalf("actionPolicyForInstallation() = (%s, %v)", policy.AssetRevision(), err)
	}
	if policy, err := actionPolicyForInstallation(verify); err == nil || !policy.AssetRevision().IsZero() {
		t.Fatalf("actionPolicyForInstallation(verifier only) = (%#v, %v)", policy, err)
	}
	var nilInstallation *testActionInstallation
	if policy, err := actionPolicyForInstallation(nilInstallation); err == nil || !policy.AssetRevision().IsZero() {
		t.Fatalf("actionPolicyForInstallation(typed nil) = (%#v, %v)", policy, err)
	}
	incompatible := testInstallationWithActions(verify, incompatibleActionProvider{Bundle: bundle})
	if policy, err := actionPolicyForInstallation(incompatible); err == nil || !policy.AssetRevision().IsZero() {
		t.Fatalf("actionPolicyForInstallation(incompatible) = (%#v, %v)", policy, err)
	}
}
