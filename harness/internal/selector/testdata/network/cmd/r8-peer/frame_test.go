package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

func TestSignedFrameBindsClaimToIndependentKey(t *testing.T) {
	peerA, privateA := testIdentity(t, "peer-a")
	peerB, _ := testIdentity(t, "peer-b")
	config := runtimeConfig{peers: map[string]peerRuntime{
		peerA.id.String(): peerA,
		peerB.id.String(): peerB,
	}}
	selection, err := selector.ParseSelectionID(agency.Sum([]byte("selection")).String())
	if err != nil {
		t.Fatal(err)
	}
	query, err := selector.NewSampleQuery(selection, 1, agency.Sum([]byte("nonce")))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := query.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := signFrame(kindQuery, peerA.id, payload, privateA)
	if err != nil {
		t.Fatal(err)
	}
	kind, source, gotPayload, err := verifyFrame(frame, config)
	if err != nil || kind != kindQuery || source != peerA.id || string(gotPayload) != string(payload) {
		t.Fatalf("valid frame was not preserved: kind=%q source=%q err=%v", kind, source.String(), err)
	}

	forged, err := signFrame(kindQuery, peerB.id, payload, privateA)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := verifyFrame(forged, config); err == nil {
		t.Fatal("claimed peer B gained authority from peer A's key")
	}
}

func TestSignedFrameRejectsUnknownFieldsAndNoncanonicalBytes(t *testing.T) {
	peer, private := testIdentity(t, "peer-a")
	config := runtimeConfig{peers: map[string]peerRuntime{peer.id.String(): peer}}
	frame, err := signFrame(kindNoVote, peer.id, nil, private)
	if err != nil {
		t.Fatal(err)
	}
	withUnknown := append(append([]byte(nil), frame[:len(frame)-1]...), []byte(`,"extra":true}`)...)
	if _, _, _, err := verifyFrame(withUnknown, config); err == nil {
		t.Fatal("unknown field was accepted")
	}
	withNewline := append(append([]byte(nil), frame...), '\n')
	if _, _, _, err := verifyFrame(withNewline, config); err == nil {
		t.Fatal("noncanonical trailing newline was accepted")
	}
}

func testIdentity(t testing.TB, value string) (peerRuntime, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := selector.NewParticipantID(value)
	if err != nil {
		t.Fatal(err)
	}
	return peerRuntime{id: id, address: value + ":8448", key: public}, private
}
