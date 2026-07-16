package peer

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCanonicalIDBytesAndComparison(t *testing.T) {
	t.Parallel()
	left := testCanonicalPeerID(t, "left")
	right := testCanonicalPeerID(t, "right")
	leftBytes, err := CanonicalIDBytes(left)
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := CanonicalIDBytes(right)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := CompareCanonicalIDs(left, right)
	if err != nil || comparison != bytes.Compare(leftBytes, rightBytes) {
		t.Fatalf("CompareCanonicalIDs() = (%d, %v)", comparison, err)
	}
	leftBytes[0] ^= 0xff
	reloaded, err := CanonicalIDBytes(left)
	if err != nil || bytes.Equal(leftBytes, reloaded) {
		t.Fatal("CanonicalIDBytes exposed mutable storage")
	}

	invalid, _ := model.ParsePeerID("not-a-libp2p-peer")
	if _, err := CanonicalIDBytes(invalid); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("invalid PeerID error = %v", err)
	}
	if _, err := CompareCanonicalIDs(left, invalid); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("invalid comparison error = %v", err)
	}
}

func testCanonicalPeerID(t *testing.T, label string) model.PeerID {
	t.Helper()
	seed := sha256.Sum256([]byte(label))
	standardPrivate := ed25519.NewKeyFromSeed(seed[:])
	privateKey, err := libp2pcrypto.UnmarshalEd25519PrivateKey(standardPrivate)
	if err != nil {
		t.Fatal(err)
	}
	id, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	result, err := model.ParsePeerID(id.String())
	if err != nil {
		t.Fatal(err)
	}
	return result
}
