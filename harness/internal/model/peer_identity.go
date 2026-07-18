package model

import (
	"bytes"
	"errors"
	"fmt"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
)

var ErrPeerIDEncoding = errors.New("invalid canonical libp2p PeerID")

// CanonicalPeerIDBytes returns the multihash bytes represented by a canonical
// libp2p PeerID. Identity encoding belongs to the leaf model layer so Store and
// Teamwork policy never depend on the network runtime package.
func CanonicalPeerIDBytes(id PeerID) ([]byte, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("%w: zero PeerID", ErrPeerIDEncoding)
	}
	decoded, err := libp2ppeer.Decode(id.String())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPeerIDEncoding, err)
	}
	if decoded.String() != id.String() {
		return nil, fmt.Errorf("%w: noncanonical text", ErrPeerIDEncoding)
	}
	return append([]byte(nil), []byte(decoded)...), nil
}

// ComparePeerIDs compares canonical PeerIDs by their decoded libp2p bytes.
func ComparePeerIDs(left, right PeerID) (int, error) {
	leftBytes, err := CanonicalPeerIDBytes(left)
	if err != nil {
		return 0, err
	}
	rightBytes, err := CanonicalPeerIDBytes(right)
	if err != nil {
		return 0, err
	}
	return bytes.Compare(leftBytes, rightBytes), nil
}
