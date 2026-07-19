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
	if apiErr != nil || validated.Deadline != 24*time.Hour || len(validated.ArtifactPaths) != 2 ||
		validated.ArtifactPaths[0] != "a.md" || validated.ArtifactPaths[1] != "z.md" {
		t.Fatalf("canonical Action validation = (%#v, %v)", validated, apiErr)
	}
	_, apiErr = validateManagedTeamworkAction(localapi.TeamworkActionRequest{
		Action: "accept", Artifacts: []string{"forbidden.md"},
	}, true)
	if apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid {
		t.Fatalf("asset-forbidden Artifact error = %#v", apiErr)
	}
}
