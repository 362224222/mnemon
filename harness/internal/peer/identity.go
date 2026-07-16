package peer

import (
	"bytes"
	"errors"
	"fmt"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrPeerIDEncoding = errors.New("invalid canonical libp2p PeerID")

// CanonicalIDBytes returns the multihash bytes represented by a canonical
// libp2p PeerID. Text is accepted only when decoding and re-encoding produces
// the exact same representation; alternate spellings cannot affect ordering.
func CanonicalIDBytes(id model.PeerID) ([]byte, error) {
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

// CompareCanonicalIDs compares two PeerIDs by their decoded libp2p bytes.
func CompareCanonicalIDs(left, right model.PeerID) (int, error) {
	leftBytes, err := CanonicalIDBytes(left)
	if err != nil {
		return 0, err
	}
	rightBytes, err := CanonicalIDBytes(right)
	if err != nil {
		return 0, err
	}
	return bytes.Compare(leftBytes, rightBytes), nil
}
