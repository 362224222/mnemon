package agency

import (
	"errors"
	"testing"
)

func TestViewAuthorityResolvesOnlyItsExactArtifactOffer(t *testing.T) {
	principal := mustPrincipal(t, "agent:artifact-reader")
	attachment := mustAttachment(t, "attachment:artifact-reader", principal, true)
	offered := mustHandle(t, "artifact:offered")
	digest := Sum([]byte("offered bytes"))
	view := mustView(t, MachineViewSpec{Attachment: attachment,
		Artifacts: []ViewArtifactOffer{mustViewOffer(t, offered, "offered bytes")}})

	resolved, err := view.ResolveOfferedArtifact(offered)
	if err != nil || resolved != digest {
		t.Fatalf("ResolveOfferedArtifact(offered) = (%s, %v), want %s", resolved, err, digest)
	}
	for _, handle := range []OpaqueHandle{{}, mustHandle(t, "artifact:known-but-unoffered")} {
		if _, err := view.ResolveOfferedArtifact(handle); err == nil ||
			(!handle.IsZero() && !errors.Is(err, ErrInvariant)) {
			t.Fatalf("ResolveOfferedArtifact(%q) error = %v", handle.String(), err)
		}
	}
}
