package event

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const PublicationSignatureDomain = model.PublicationSignatureDomain

type PublicationSigner interface {
	Sign(context.Context, []byte) ([]byte, error)
}

type Ed25519Signer struct {
	privateKey ed25519.PrivateKey
}

func NewEd25519Signer(privateKey ed25519.PrivateKey) (*Ed25519Signer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: Ed25519 private key must contain %d bytes", ErrSignature, ed25519.PrivateKeySize)
	}
	return &Ed25519Signer{append(ed25519.PrivateKey(nil), privateKey...)}, nil
}

func (signer *Ed25519Signer) Sign(ctx context.Context, message []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if signer == nil || len(signer.privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: unavailable Ed25519 private key", ErrSignature)
	}
	return ed25519.Sign(signer.privateKey, message), nil
}

func (signer *Ed25519Signer) PublicKey() ed25519.PublicKey {
	if signer == nil || len(signer.privateKey) != ed25519.PrivateKeySize {
		return nil
	}
	publicKey := signer.privateKey.Public().(ed25519.PublicKey)
	return append(ed25519.PublicKey(nil), publicKey...)
}

func VerifyPublication(publicKey ed25519.PublicKey, publication model.SignedPublication) error {
	if err := model.VerifyPublication(publicKey, publication); err != nil {
		return fmt.Errorf("%w: %v", ErrSignature, err)
	}
	return nil
}

func publicationSigningMessage(channelID model.ChannelID, publicationDigest model.Digest) []byte {
	message, _ := model.PublicationSigningMessage(channelID, publicationDigest)
	return message
}

func hasPublicationDomain(message []byte) bool {
	prefix := append([]byte(PublicationSignatureDomain), 0)
	return bytes.HasPrefix(message, prefix)
}
