package cli

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestValidateManagedTeamworkActionUsesCanonicalAssetPolicy(t *testing.T) {
	t.Parallel()
	validated, apiErr := validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "offer", Content: "review", Artifacts: []string{"z.md", "a.md"},
	}, false)
	if apiErr != nil || validated.validated.Deadline != 24*time.Hour ||
		len(validated.validated.ArtifactPaths) != 2 ||
		validated.validated.ArtifactPaths[0] != "a.md" ||
		validated.validated.ArtifactPaths[1] != "z.md" || validated.receipt.MaxResults() != 7 {
		t.Fatalf("canonical Action validation = (%#v, %v)", validated, apiErr)
	}
	_, apiErr = validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "accept", Artifacts: []string{"forbidden.md"},
	}, true)
	if apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid {
		t.Fatalf("asset-forbidden Artifact error = %#v", apiErr)
	}
	_, apiErr = validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "deliver", Content: "done",
	}, false)
	if apiErr == nil || apiErr.Code != localapi.CodeContextRequired {
		t.Fatalf("asset-required context error = %#v", apiErr)
	}
	_, apiErr = validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "accept", To: "peer", Content: "accepted",
	}, true)
	if apiErr == nil || apiErr.Code != localapi.CodeInvalidArgument {
		t.Fatalf("asset-forbidden selector error = %#v", apiErr)
	}
	_, apiErr = validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "future-action", Content: "unsupported",
	}, false)
	if apiErr == nil || apiErr.Code != localapi.CodeUnknownAction {
		t.Fatalf("unknown managed Action error = %#v", apiErr)
	}
}

func TestManagedTeamworkReceiptUsesCanonicalActionPolicy(t *testing.T) {
	t.Parallel()
	offer, apiErr := validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "offer", To: "auto", Content: "review"}, false)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	accepted := localapi.OperationResponse{Status: "accepted", Action: "teamwork.offer",
		Results: make([]localapi.OperationResult, 7)}
	if apiErr := validateManagedTeamworkReceipt(offer, accepted); apiErr != nil {
		t.Fatalf("canonical offer receipt = %#v", apiErr)
	}
	accepted.Results = []localapi.OperationResult{}
	if apiErr := validateManagedTeamworkReceipt(offer, accepted); apiErr == nil ||
		apiErr.Code != localapi.CodeInternal {
		t.Fatalf("empty offer receipt = %#v", apiErr)
	}
	accepted.Results = make([]localapi.OperationResult, 7)
	accepted.Results = append(accepted.Results, localapi.OperationResult{})
	if apiErr := validateManagedTeamworkReceipt(offer, accepted); apiErr == nil ||
		apiErr.Code != localapi.CodeInternal {
		t.Fatalf("oversize offer receipt = %#v", apiErr)
	}

	accept, apiErr := validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "accept", Content: "accepted"}, true)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	accepted = localapi.OperationResponse{Status: "accepted", Action: "teamwork.accept",
		Handling: &localapi.HandlingReceipt{Status: "completed"},
		Results:  []localapi.OperationResult{{}}}
	if apiErr := validateManagedTeamworkReceipt(accept, accepted); apiErr != nil {
		t.Fatalf("canonical accept receipt = %#v", apiErr)
	}
	accepted.Results = nil
	if apiErr := validateManagedTeamworkReceipt(accept, accepted); apiErr == nil ||
		apiErr.Code != localapi.CodeInternal {
		t.Fatalf("missing accept result = %#v", apiErr)
	}
}
