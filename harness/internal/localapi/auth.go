package localapi

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	authorizationHeader = "Authorization"
	operationKeyHeader  = "Mnemon-Operation-Key"
	claimContextHeader  = "Mnemon-Claim-Context"
	runAttachmentHeader = "Mnemon-Run-Attachment"
	profileScheme       = "MnemonProfile "
	opaqueSecretBytes   = 32
)

type headerPolicy struct {
	operationRequired bool
	operationAllowed  bool
	claimRequired     bool
	claimAllowed      bool
	attachmentAllowed bool
}

func authenticateRequest(ctx context.Context, request *http.Request, authenticator Authenticator,
	policy headerPolicy,
) (RequestMetadata, *APIError) {
	if ctx == nil || request == nil || authenticator == nil {
		return RequestMetadata{}, NewAPIError(CodeInternal, "local authentication is unavailable")
	}
	authorization, err := singleHeader(request.Header, authorizationHeader)
	if err != nil || !strings.HasPrefix(authorization, profileScheme) {
		return RequestMetadata{}, NewAPIError(CodeAuthenticationFailed, "profile authentication failed")
	}
	credential, err := decodeOpaqueSecret(strings.TrimPrefix(authorization, profileScheme))
	if err != nil {
		return RequestMetadata{}, NewAPIError(CodeAuthenticationFailed, "profile authentication failed")
	}
	profile, err := authenticator.AuthenticateProfile(ctx, model.Sum(credential))
	clear(credential)
	if err != nil || profile.ID().IsZero() {
		return RequestMetadata{}, NewAPIError(CodeAuthenticationFailed, "profile authentication failed")
	}
	metadata := RequestMetadata{Profile: profile}

	operation, present, apiErr := optionalSecretHeader(request.Header, operationKeyHeader)
	if apiErr != nil {
		return RequestMetadata{}, apiErr
	}
	if present && !policy.operationAllowed {
		return RequestMetadata{}, NewAPIError(CodeInvalidArgument, "operation key is not allowed on this route")
	}
	if !present && policy.operationRequired {
		return RequestMetadata{}, NewAPIError(CodeInvalidArgument, "operation key is required")
	}
	if present {
		metadata.OperationKeyHash, metadata.HasOperationKey = model.Sum(operation), true
		clear(operation)
	}

	claim, present, apiErr := optionalSecretHeader(request.Header, claimContextHeader)
	if apiErr != nil {
		return RequestMetadata{}, apiErr
	}
	if present && !policy.claimAllowed {
		return RequestMetadata{}, NewAPIError(CodeInvalidArgument, "claim context is not allowed on this route")
	}
	if !present && policy.claimRequired {
		return RequestMetadata{}, NewAPIError(CodeContextRequired, "managed context is required")
	}
	if present {
		metadata.ClaimContextHash, metadata.HasClaimContext = model.Sum(claim), true
		clear(claim)
	}

	attachment, present, apiErr := optionalSecretHeader(request.Header, runAttachmentHeader)
	if apiErr != nil {
		return RequestMetadata{}, apiErr
	}
	if present && !policy.attachmentAllowed {
		return RequestMetadata{}, NewAPIError(CodeInvalidArgument, "run attachment is not allowed on this route")
	}
	if present {
		metadata.RunAttachmentHash, metadata.HasRunAttachment = model.Sum(attachment), true
		clear(attachment)
	}
	return metadata, nil
}

func optionalSecretHeader(header http.Header, name string) ([]byte, bool, *APIError) {
	values := header.Values(name)
	if len(values) == 0 {
		return nil, false, nil
	}
	if len(values) != 1 {
		return nil, false, NewAPIError(CodeInvalidArgument, "duplicate control metadata header")
	}
	decoded, err := decodeOpaqueSecret(values[0])
	if err != nil {
		code := CodeInvalidArgument
		if name == claimContextHeader {
			code = CodeContextInvalid
		}
		return nil, false, NewAPIError(code, "invalid control metadata")
	}
	return decoded, true, nil
}

func singleHeader(header http.Header, name string) (string, error) {
	values := header.Values(name)
	if len(values) != 1 || values[0] == "" {
		return "", errors.New("header must occur exactly once")
	}
	return values[0], nil
}

func decodeOpaqueSecret(value string) ([]byte, error) {
	if strings.TrimSpace(value) != value || strings.ContainsAny(value, "= \t\r\n") {
		return nil, errors.New("secret is not unpadded base64url")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) != opaqueSecretBytes {
		clear(decoded)
		return nil, errors.New("secret must encode 32 bytes")
	}
	return decoded, nil
}
