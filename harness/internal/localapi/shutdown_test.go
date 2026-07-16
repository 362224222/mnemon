package localapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestShutdownResponseIsFixedCanonicalAndBounded(t *testing.T) {
	t.Parallel()
	authority := testShutdownAuthority(t)
	digest, err := AuthorityDigest(authority)
	if err != nil {
		t.Fatal(err)
	}
	response := newShutdownResponse(digest)
	raw, err := model.CanonicalMarshal(response)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"authority_digest":"` + digest.String() +
		`","schema_version":1,"status":"stopping"}`
	if string(raw) != want || len(raw)+1 > MaxShutdownResponseBytes ||
		validateShutdownResponse(response, digest) != nil {
		t.Fatalf("shutdown response = %q %#v", raw, response)
	}
}

func TestAuthorityDigestValidatesAndHashesCanonicalResponse(t *testing.T) {
	t.Parallel()
	authority := testShutdownAuthority(t)
	raw, err := model.CanonicalMarshal(authority)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := AuthorityDigest(authority)
	if err != nil || digest != model.Sum(raw) {
		t.Fatalf("AuthorityDigest() = (%s, %v), want %s", digest.String(), err,
			model.Sum(raw).String())
	}
	authority.SchemaVersion++
	if _, err := AuthorityDigest(authority); err == nil {
		t.Fatal("AuthorityDigest() accepted an open authority response")
	}
}

func TestShutdownResponseRejectsOpenOrUnsupportedState(t *testing.T) {
	t.Parallel()
	digest, err := AuthorityDigest(testShutdownAuthority(t))
	if err != nil {
		t.Fatal(err)
	}
	tests := []ShutdownResponse{
		{},
		{AuthorityDigest: digest.String(), SchemaVersion: SchemaVersion + 1, Status: "stopping"},
		{AuthorityDigest: digest.String(), SchemaVersion: SchemaVersion, Status: "stopped"},
		{AuthorityDigest: model.Sum([]byte("different authority")).String(),
			SchemaVersion: SchemaVersion, Status: "stopping"},
	}
	for _, response := range tests {
		if apiErr := validateShutdownResponse(response, digest); apiErr == nil || apiErr.Code != CodeInternal {
			t.Fatalf("validateShutdownResponse(%#v) = %#v", response, apiErr)
		}
	}
}

func testShutdownAuthority(t testing.TB) AuthorityResponse {
	t.Helper()
	response, err := NewAuthorityResponse(testShutdownAuthoritySnapshot(t))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func testShutdownAuthoritySnapshot(t testing.TB) AuthoritySnapshot {
	t.Helper()
	revision := model.Sum([]byte("shutdown-authority-assets")).String()
	peerID, err := model.ParsePeerID("peer-shutdown-authority")
	if err != nil {
		t.Fatal(err)
	}
	return AuthoritySnapshot{Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, Enabled: true, AssetRevision: revision,
		UpdatedAt: time.Date(2026, 7, 17, 1, 2, 3, 4, time.UTC), PeerID: peerID,
		ActiveAssetRevision: revision}
}

func setShutdownAuthorityDigest(t testing.TB, request *http.Request, authority AuthorityResponse) {
	t.Helper()
	digest, err := AuthorityDigest(authority)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(authorityDigestHeader, digest.String())
}
