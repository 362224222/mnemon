package localapi

import "github.com/mnemon-dev/mnemon/harness/internal/model"

const (
	channelCreateMutationKind = "create"
	channelInviteMutationKind = "invite"
)

// ChannelCreateRequestDigest binds the typed canonical body to its closed
// route. In particular, an empty create request and an empty invite request
// cannot share a PendingJournal identity even though both bodies encode as {}.
func ChannelCreateRequestDigest(request ChannelCreateRequest) (model.Digest, *APIError) {
	return channelMutationRequestDigest(channelCreateMutationKind, request)
}

// ChannelInviteRequestDigest is the independent caller/server digest for one
// typed Channel invite request.
func ChannelInviteRequestDigest(request ChannelInviteRequest) (model.Digest, *APIError) {
	return channelMutationRequestDigest(channelInviteMutationKind, request)
}

func channelMutationRequestDigest(kind string, request any) (model.Digest, *APIError) {
	raw, err := model.CanonicalMarshal(struct {
		Kind          string `json:"kind"`
		Request       any    `json:"request"`
		SchemaVersion int    `json:"schema_version"`
	}{Kind: kind, Request: request, SchemaVersion: SchemaVersion})
	if err != nil || len(raw) == 0 || len(raw) > MaxRequestBodyBytes {
		return model.Digest{}, NewAPIError(CodeInvalidArgument,
			"Channel mutation request cannot be encoded canonically")
	}
	return model.Sum(raw), nil
}
