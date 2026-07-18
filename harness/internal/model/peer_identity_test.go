package model

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestCanonicalPeerIDBytesAndComparison(t *testing.T) {
	t.Parallel()
	left := canonicalModelPeerID(t, "left")
	right := canonicalModelPeerID(t, "right")
	leftBytes, err := CanonicalPeerIDBytes(left)
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := CanonicalPeerIDBytes(right)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := ComparePeerIDs(left, right)
	if err != nil || comparison != bytes.Compare(leftBytes, rightBytes) {
		t.Fatalf("ComparePeerIDs() = (%d, %v)", comparison, err)
	}
	leftBytes[0] ^= 0xff
	reloaded, err := CanonicalPeerIDBytes(left)
	if err != nil || bytes.Equal(leftBytes, reloaded) {
		t.Fatal("CanonicalPeerIDBytes exposed mutable storage")
	}

	invalid, _ := ParsePeerID("not-a-libp2p-peer")
	if _, err := CanonicalPeerIDBytes(invalid); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("invalid PeerID error = %v", err)
	}
	if _, err := ComparePeerIDs(left, invalid); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("invalid comparison error = %v", err)
	}
}

func canonicalModelPeerID(t *testing.T, label string) PeerID {
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
	result, err := ParsePeerID(id.String())
	if err != nil {
		t.Fatal(err)
	}
	return result
}
