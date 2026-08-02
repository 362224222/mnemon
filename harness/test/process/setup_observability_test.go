//go:build darwin || linux

package process_test

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func setupProcessAssertPublicObservability(t *testing.T, executable, workspace string,
	environment []string, assetRevision string,
) {
	t.Helper()
	setupProcessAssertPublicStatus(t, executable, workspace, environment, assetRevision)
	setupProcessAssertPublicDoctor(t, executable, workspace, environment)
	setupProcessAssertCodexProjectionLayout(t, workspace, true)
}

func setupProcessAssertPublicStatus(t *testing.T, executable, workspace string,
	environment []string, assetRevision string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	result := setupProcessRunHarness(ctx, executable, workspace, environment, "status")
	cancel()
	status, err := setupProcessParseStatus(result)
	if err != nil || status.SchemaVersion != localapi.SchemaVersion || status.Scope != "managed_agent" ||
		status.Status != "ready" || status.AssetRevision != assetRevision ||
		status.Activation.State != "ready" || status.Activation.Issue != "none" ||
		status.Runtime.State != "ready" || status.Runtime.Issue != "none" {
		t.Fatalf("public status after setup = (%#v, %v)", status, err)
	}
}

func setupProcessAssertPublicDoctor(t *testing.T, executable, workspace string,
	environment []string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	result := setupProcessRunHarness(ctx, executable, workspace, environment, "doctor")
	cancel()
	doctor, err := setupProcessParseDoctor(result)
	if err != nil || doctor.ArtifactTransfer == nil || doctor.ArtifactTransfer.ActivePulls != 0 ||
		doctor.SchemaVersion != localapi.SchemaVersion || doctor.Scope != "managed_agent" ||
		doctor.Mode != "online" || doctor.Status != "healthy" || len(doctor.Checks) != 7 ||
		len(doctor.Channels) != 0 {
		t.Fatalf("public doctor after setup = (%#v, %v)", doctor, err)
	}
	for index, name := range []string{"node_authority", "canonical_assets", "host_projection",
		"host_registration", "daemon", "managed_runtime", "channel_progress"} {
		check := doctor.Checks[index]
		if check.Name != name || check.Status != "pass" || check.Issue != "none" ||
			check.Remedy != "none" {
			t.Fatalf("public doctor check %d = %#v", index, check)
		}
	}
}
