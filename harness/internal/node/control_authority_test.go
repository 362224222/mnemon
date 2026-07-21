package node

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestControlAuthorityResponseIsCanonicalAndDigestible(t *testing.T) {
	peer, err := model.ParsePeerID("12D3KooWBGZcLJWATvD7zmYyooNARUfxeecfUrmsLrNdeu4Wq2eD")
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("authority-contract")).String()
	response, err := NewAuthorityResponse(AuthoritySnapshot{Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		Enabled: true, AssetRevision: revision, ActiveAssetRevision: revision,
		UpdatedAt: time.Unix(1, 2).UTC(), PeerID: peer})
	if err != nil {
		t.Fatalf("NewAuthorityResponse() error = %v", err)
	}
	if digest, err := AuthorityDigest(response); err != nil || digest.IsZero() {
		t.Fatalf("AuthorityDigest() = (%s, %v)", digest.String(), err)
	}
}
