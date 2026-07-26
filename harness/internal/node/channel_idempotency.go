package node

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (manager *ChannelManager) buildToken(ctx context.Context, descriptor model.SignedChannelDescriptor,
	addresses []string, at time.Time, lifetime time.Duration, uses uint8,
	operation store.ChannelMutationOperation, operationSecret []byte,
) (model.EnrollmentToken, error) {
	grantID, err := manager.newGrantID()
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	return manager.signChannelMutationToken(ctx, descriptor, addresses, grantID, at.Add(lifetime),
		uses, operation, operationSecret)
}

func (manager *ChannelManager) signChannelMutationToken(ctx context.Context,
	descriptor model.SignedChannelDescriptor, addresses []string, grantID model.GrantID,
	expiresAt time.Time, uses uint8, operation store.ChannelMutationOperation,
	operationSecret []byte,
) (model.EnrollmentToken, error) {
	secret, err := deriveChannelMutationBearer(operation, operationSecret,
		descriptor.Descriptor().ID(), grantID)
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	defer clear(secret)
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: addresses, GrantID: grantID, BearerSecret: secret, ExpiresAt: expiresAt,
		MaxUses: uses, ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion})
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	message, _ := model.EnrollmentTokenSigningMessage(descriptor.Descriptor().ID(), payload.Digest())
	signature, err := manager.identity.PublicationSigner().Sign(ctx, message)
	if err != nil {
		return model.EnrollmentToken{}, err
	}
	return model.AttachEnrollmentTokenSignature(payload, signature)
}

func (manager *ChannelManager) channelMutationOperation(ctx context.Context,
	metadata RequestMetadata, kind store.ChannelMutationKind,
) (store.ChannelMutationOperation, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return store.ChannelMutationOperation{}, apiErr
	}
	if !kind.Valid() || !metadata.HasOperationKey || metadata.OperationKeyHash.IsZero() ||
		!metadata.HasRequestDigest || metadata.RequestDigest.IsZero() ||
		len(metadata.OperationKeySecret) != model.EnrollmentSecretBytes ||
		model.Sum(metadata.OperationKeySecret) != metadata.OperationKeyHash {
		return store.ChannelMutationOperation{},
			NewAPIError(CodeOperationMismatch, "Channel mutation operation identity is invalid")
	}
	return store.ChannelMutationOperation{Kind: kind, OperationKeyHash: metadata.OperationKeyHash,
		RequestDigest: metadata.RequestDigest}, nil
}

func channelLeaveOperation(metadata RequestMetadata) (store.ChannelLeaveOperation, *APIError) {
	if !metadata.HasOperationKey || metadata.OperationKeyHash.IsZero() ||
		!metadata.HasRequestDigest || metadata.RequestDigest.IsZero() {
		return store.ChannelLeaveOperation{},
			NewAPIError(CodeOperationMismatch, "Channel leave operation identity is invalid")
	}
	return store.ChannelLeaveOperation{OperationKeyHash: metadata.OperationKeyHash,
		RequestDigest: metadata.RequestDigest}, nil
}

func deriveChannelMutationBearer(operation store.ChannelMutationOperation,
	operationSecret []byte, channelID model.ChannelID, grantID model.GrantID,
) ([]byte, error) {
	if !operation.Kind.Valid() || operation.OperationKeyHash.IsZero() ||
		operation.RequestDigest.IsZero() || len(operationSecret) != model.EnrollmentSecretBytes ||
		model.Sum(operationSecret) != operation.OperationKeyHash ||
		channelID.IsZero() || grantID.IsZero() {
		return nil, errors.New("Channel mutation bearer authority is invalid")
	}
	message, err := model.CanonicalMarshal(struct {
		ChannelID     string `json:"channel_id"`
		Domain        string `json:"domain"`
		GrantID       string `json:"grant_id"`
		Kind          string `json:"kind"`
		RequestDigest string `json:"request_digest"`
		SchemaVersion int    `json:"schema_version"`
	}{ChannelID: channelID.String(), Domain: "mnemon/r5/channel-mutation-bearer/1",
		GrantID: grantID.String(), Kind: string(operation.Kind),
		RequestDigest: operation.RequestDigest.String(), SchemaVersion: SchemaVersion})
	if err != nil {
		return nil, err
	}
	derived := hmac.New(sha256.New, operationSecret)
	_, _ = derived.Write(message)
	return derived.Sum(nil), nil
}

func (manager *ChannelManager) channelCreateMutationResponse(ctx context.Context,
	metadata RequestMetadata, mutation store.ChannelMutationAuthority,
) (ChannelCreateResponse, *APIError) {
	view, invite, token, apiErr := manager.projectChannelMutation(ctx, metadata,
		store.ChannelMutationCreate, mutation)
	if apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	return ChannelCreateResponse{SchemaVersion: SchemaVersion, Status: "created",
		Channel: view, Invite: invite, InviteToken: token.Reveal()}, nil
}

func (manager *ChannelManager) channelInviteMutationResponse(ctx context.Context,
	metadata RequestMetadata, mutation store.ChannelMutationAuthority,
) (ChannelInviteResponse, *APIError) {
	view, invite, token, apiErr := manager.projectChannelMutation(ctx, metadata,
		store.ChannelMutationInvite, mutation)
	if apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	return ChannelInviteResponse{SchemaVersion: SchemaVersion, Status: "created",
		Channel: view, Invite: invite, InviteToken: token.Reveal()}, nil
}

func (manager *ChannelManager) projectChannelMutation(ctx context.Context,
	metadata RequestMetadata, kind store.ChannelMutationKind,
	mutation store.ChannelMutationAuthority,
) (ChannelView, ChannelInviteView, model.EnrollmentToken, *APIError) {
	operation, apiErr := manager.channelMutationOperation(ctx, metadata, kind)
	if apiErr != nil || mutation.IsZero() || mutation.Kind() != kind {
		if apiErr != nil {
			return ChannelView{}, ChannelInviteView{}, model.EnrollmentToken{}, apiErr
		}
		return ChannelView{}, ChannelInviteView{}, model.EnrollmentToken{},
			NewAPIError(CodeInternal, "durable Channel mutation result is invalid")
	}
	token, err := manager.signChannelMutationToken(ctx, mutation.Channel().Descriptor(),
		mutation.OwnerMultiaddrs(), mutation.GrantID(), mutation.GrantExpiresAt(),
		mutation.GrantMaxUses(), operation, metadata.OperationKeySecret)
	if err != nil {
		return ChannelView{}, ChannelInviteView{}, model.EnrollmentToken{},
			NewAPIError(CodeInternal, "durable Channel mutation token cannot be reconstructed")
	}
	grant, err := model.NewOpenEnrollmentGrantForToken(token, mutation.GrantCreatedAt())
	if err != nil || token.Payload().Digest() != mutation.TokenPayloadDigest() ||
		!bytes.Equal(grant.Verifier().Bytes(), mutation.GrantVerifier().Bytes()) {
		return ChannelView{}, ChannelInviteView{}, model.EnrollmentToken{},
			NewAPIError(CodeInternal, "durable Channel mutation token commitment differs")
	}
	view, err := manager.readChannelView(ctx, mutation.Channel().ID())
	if err != nil {
		return ChannelView{}, ChannelInviteView{}, model.EnrollmentToken{}, channelAPIError(err)
	}
	invite := inviteView(mutation.GrantExpiresAt(), mutation.GrantMaxUses(),
		mutation.GrantUsedUses(), mutation.GrantStatus(), manager.clock.Now())
	view.Invite = &invite
	return view, invite, token, nil
}

func channelMutationAPIError(err error) *APIError {
	if errors.Is(err, store.ErrChannelMutationMismatch) ||
		errors.Is(err, store.ErrChannelMutationInput) {
		return NewAPIError(CodeOperationMismatch, "Channel mutation operation does not match request")
	}
	return channelAPIError(err)
}

func channelLeaveOperationAPIError(err error) *APIError {
	if errors.Is(err, store.ErrChannelLeaveOperationMismatch) ||
		errors.Is(err, store.ErrChannelLeaveInput) {
		return NewAPIError(CodeOperationMismatch, "Channel leave operation does not match request")
	}
	return channelAPIError(err)
}
