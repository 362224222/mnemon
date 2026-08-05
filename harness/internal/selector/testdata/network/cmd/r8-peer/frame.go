package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

const (
	frameVersion  = 1
	maxFrameBytes = 4 << 10
	kindQuery     = "sample.query"
	kindVote      = "sample.vote"
	kindNoVote    = "sample.no-vote"
)

type signedFrame struct {
	Version   uint32 `json:"version"`
	Kind      string `json:"kind"`
	Source    string `json:"source"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

type unsignedFrame struct {
	Version uint32 `json:"version"`
	Kind    string `json:"kind"`
	Source  string `json:"source"`
	Payload string `json:"payload"`
}

func signFrame(kind string, source selector.ParticipantID, payload []byte,
	private ed25519.PrivateKey,
) ([]byte, error) {
	if source.IsZero() || len(private) != ed25519.PrivateKeySize || !validFrameShape(kind, payload) {
		return nil, errors.New("signed frame input is incomplete")
	}
	unsigned := unsignedFrame{Version: frameVersion, Kind: kind, Source: source.String(),
		Payload: base64.StdEncoding.EncodeToString(payload)}
	canonical, err := json.Marshal(unsigned)
	if err != nil {
		return nil, err
	}
	wire := signedFrame{Version: unsigned.Version, Kind: unsigned.Kind, Source: unsigned.Source,
		Payload: unsigned.Payload, Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(private, canonical))}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxFrameBytes {
		return nil, errors.New("signed frame exceeds fixed bound")
	}
	return encoded, nil
}

func verifyFrame(raw []byte, config runtimeConfig) (string, selector.ParticipantID, []byte, error) {
	if len(raw) == 0 || len(raw) > maxFrameBytes {
		return "", selector.ParticipantID{}, nil, errors.New("signed frame is empty or over its bound")
	}
	var wire signedFrame
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || requireEOF(decoder) != nil {
		return "", selector.ParticipantID{}, nil, errors.New("signed frame is not closed JSON")
	}
	peer, err := config.peer(wire.Source)
	if err != nil || wire.Version != frameVersion {
		return "", selector.ParticipantID{}, nil, errors.New("signed frame source or version is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(wire.Payload)
	if err != nil || !validFrameShape(wire.Kind, payload) {
		return "", selector.ParticipantID{}, nil, errors.New("signed frame payload is invalid")
	}
	unsigned := unsignedFrame{Version: wire.Version, Kind: wire.Kind, Source: wire.Source,
		Payload: wire.Payload}
	canonical, err := json.Marshal(unsigned)
	if err != nil {
		return "", selector.ParticipantID{}, nil, err
	}
	signature, err := base64.StdEncoding.DecodeString(wire.Signature)
	if err != nil || !ed25519.Verify(peer.key, canonical, signature) {
		return "", selector.ParticipantID{}, nil, errors.New("signed frame identity verification failed")
	}
	reencoded, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(raw, reencoded) {
		return "", selector.ParticipantID{}, nil, errors.New("signed frame is not exact canonical JSON")
	}
	return wire.Kind, peer.id, payload, nil
}

func validFrameShape(kind string, payload []byte) bool {
	switch kind {
	case kindQuery:
		return len(payload) > 0 && len(payload) <= selector.MaxSampleQueryFrameBytes
	case kindVote:
		return len(payload) > 0 && len(payload) <= selector.MaxSampleVoteFrameBytes
	case kindNoVote:
		return len(payload) == 0
	default:
		return false
	}
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > maximum {
		return nil, fmt.Errorf("body is empty, unreadable, or exceeds %d bytes", maximum)
	}
	return raw, nil
}
