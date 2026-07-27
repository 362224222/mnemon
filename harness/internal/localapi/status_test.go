package localapi

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestStatusResponseReportsOnlyTruthfulClosedActivationAndRuntimeState(t *testing.T) {
	revision := model.Sum([]byte("status-assets")).String()
	tests := []struct {
		name        string
		snapshot    StatusSnapshot
		wantStatus  string
		wantExit    int
		wantActive  StatusCheck
		wantRuntime StatusCheck
	}{
		{name: "ready", snapshot: StatusSnapshot{AssetRevision: revision, ActivationReady: true,
			Runtime: RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}},
			wantStatus: statusReady, wantExit: 0,
			wantActive:  StatusCheck{Issue: statusIssueNone, State: activationReady},
			wantRuntime: StatusCheck{Issue: statusIssueNone, State: runtimeReady}},
		{name: "activation drift", snapshot: StatusSnapshot{AssetRevision: revision,
			ActivationIssue: statusIssueAssetMismatch,
			Runtime:         RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}},
			wantStatus: statusDegraded, wantExit: 3,
			wantActive:  StatusCheck{Issue: statusIssueAssetMismatch, State: activationFailed},
			wantRuntime: StatusCheck{Issue: statusIssueNone, State: runtimeReady}},
		{name: "starting", snapshot: StatusSnapshot{AssetRevision: revision, ActivationReady: true,
			Runtime: RuntimeStatusSnapshot{Healthy: true}}, wantStatus: statusDegraded, wantExit: 5,
			wantActive:  StatusCheck{Issue: statusIssueNone, State: activationReady},
			wantRuntime: StatusCheck{Issue: statusIssueNone, State: runtimeStarting}},
		{name: "recovering", snapshot: StatusSnapshot{AssetRevision: revision, ActivationReady: true,
			Runtime: RuntimeStatusSnapshot{Running: true, Healthy: true, Recovering: true,
				Issue: statusIssueRecoveryLive}}, wantStatus: statusDegraded, wantExit: 5,
			wantActive:  StatusCheck{Issue: statusIssueNone, State: activationReady},
			wantRuntime: StatusCheck{Issue: statusIssueRecoveryLive, State: runtimeRecovering}},
		{name: "retrying", snapshot: StatusSnapshot{AssetRevision: revision, ActivationReady: true,
			Runtime: RuntimeStatusSnapshot{Running: true, Healthy: true, Issue: statusIssueWakePrepare}},
			wantStatus: statusDegraded, wantExit: 5,
			wantActive:  StatusCheck{Issue: statusIssueNone, State: activationReady},
			wantRuntime: StatusCheck{Issue: statusIssueWakePrepare, State: runtimeRetrying}},
		{name: "failed", snapshot: StatusSnapshot{AssetRevision: revision, ActivationReady: true,
			Runtime: RuntimeStatusSnapshot{Issue: statusIssueDurableRuntime}}, wantStatus: statusDegraded,
			wantExit:    1,
			wantActive:  StatusCheck{Issue: statusIssueNone, State: activationReady},
			wantRuntime: StatusCheck{Issue: statusIssueDurableRuntime, State: runtimeFailed}},
	}
	for index := range tests {
		tests[index].snapshot.ArtifactTransfer = testStatusArtifactTransferSnapshot(0)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := NewStatusResponse(test.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if response.Status != test.wantStatus || response.Activation != test.wantActive ||
				response.Runtime != test.wantRuntime || response.SchemaVersion != SchemaVersion ||
				response.Scope != statusScopeManagedAgent || response.AssetRevision != revision {
				t.Fatalf("NewStatusResponse() = %#v", response)
			}
			if apiErr := validateStatusResponse(response); apiErr != nil {
				t.Fatalf("validateStatusResponse() = %v", apiErr)
			}
			if exit := response.ExitStatus(); exit != test.wantExit {
				t.Fatalf("StatusResponse.ExitStatus() = %d, want %d", exit, test.wantExit)
			}
		})
	}
}

func TestStatusResponseRedactsUnknownRuntimeIssueAndRejectsInconsistentState(t *testing.T) {
	revision := model.Sum([]byte("status-redaction")).String()
	secret := "provider-key=private-runtime-diagnostic"
	response, err := NewStatusResponse(StatusSnapshot{
		ArtifactTransfer: testStatusArtifactTransferSnapshot(0),
		AssetRevision:    revision, ActivationReady: true,
		Runtime: RuntimeStatusSnapshot{Running: true, Healthy: true, Issue: secret}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Runtime.Issue != statusIssueInternalRuntime || strings.Contains(response.Runtime.Issue, secret) {
		t.Fatalf("unknown Runtime issue escaped redaction: %#v", response.Runtime)
	}
	activation, err := NewStatusResponse(StatusSnapshot{
		ArtifactTransfer: testStatusArtifactTransferSnapshot(0), AssetRevision: revision,
		ActivationIssue: secret, Runtime: RuntimeStatusSnapshot{Healthy: true}})
	if err != nil || activation.Activation.Issue != statusIssueInternalActivation ||
		strings.Contains(activation.Activation.Issue, secret) {
		t.Fatalf("unknown activation issue escaped redaction: (%#v, %v)", activation.Activation, err)
	}
	if _, err := NewStatusResponse(StatusSnapshot{
		ArtifactTransfer: testStatusArtifactTransferSnapshot(0),
		AssetRevision:    revision, ActivationReady: true,
		ActivationIssue: statusIssueAssetMismatch,
		Runtime:         RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}}); err == nil {
		t.Fatal("ready activation accepted an issue")
	}
	for _, snapshot := range []RuntimeStatusSnapshot{
		{Ready: true, Healthy: true},
		{Running: true, Ready: true, Healthy: true, Recovering: true},
		{Running: true, Ready: true, Healthy: true, Issue: statusIssueWakePrepare},
		{Running: true, Recovering: true},
	} {
		if _, err := NewStatusResponse(StatusSnapshot{
			ArtifactTransfer: testStatusArtifactTransferSnapshot(0), AssetRevision: revision,
			ActivationReady: true, Runtime: snapshot}); err == nil {
			t.Fatalf("inconsistent Runtime snapshot accepted: %#v", snapshot)
		}
	}
}

func TestStatusResponseValidationIsClosedAndBounded(t *testing.T) {
	revision := model.Sum([]byte("status-validation")).String()
	response, err := NewStatusResponse(StatusSnapshot{
		ArtifactTransfer: testStatusArtifactTransferSnapshot(0),
		AssetRevision:    revision, ActivationReady: true,
		Runtime: RuntimeStatusSnapshot{Running: true, Ready: true, Healthy: true}})
	if err != nil {
		t.Fatal(err)
	}
	mutations := []StatusResponse{response, response, response, response, response, response,
		response, response}
	mutations[0].SchemaVersion++
	mutations[1].Status = "configured"
	mutations[2].Activation.Issue = statusIssueInternalRuntime
	mutations[3].Runtime.Issue = "private-error"
	mutations[4].Runtime.State = runtimeFailed
	mutations[5].Scope = "node"
	mutations[6].ArtifactTransfer = nil
	mutations[7].ArtifactTransfer = &StatusArtifactTransfer{
		ActivePulls: StatusArtifactTransferPullLimit() + 1,
	}
	for _, mutation := range mutations {
		if apiErr := validateStatusResponse(mutation); apiErr == nil {
			t.Fatalf("invalid status response accepted: %#v", mutation)
		}
		if exit := mutation.ExitStatus(); exit != 1 {
			t.Fatalf("invalid status response exit = %d", exit)
		}
	}
	if raw, err := model.CanonicalMarshal(response); err != nil || len(raw)+1 > MaxStatusResponseBytes {
		t.Fatalf("status response bound = (%d, %v)", len(raw)+1, err)
	}
}

func testStatusArtifactTransferSnapshot(active int) StatusArtifactTransferSnapshot {
	return StatusArtifactTransferSnapshot{ActivePulls: active,
		MaximumPulls: StatusArtifactTransferPullLimit()}
}
