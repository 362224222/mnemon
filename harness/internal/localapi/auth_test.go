package localapi

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type fakeAuthenticator struct {
	want model.Digest
	seen model.Digest
}

func (a *fakeAuthenticator) AuthenticateProfile(_ context.Context, credential model.Digest) (model.Profile, error) {
	a.seen = credential
	if credential != a.want {
		return model.Profile{}, errors.New("denied")
	}
	return localAPITestProfile(), nil
}

func TestAuthenticateRequestFreezesAuthorityAndMetadata(t *testing.T) {
	t.Parallel()
	credential := make([]byte, opaqueSecretBytes)
	operation := make([]byte, opaqueSecretBytes)
	claim := make([]byte, opaqueSecretBytes)
	for index := range credential {
		credential[index] = byte(index + 1)
		operation[index] = byte(index + 33)
		claim[index] = byte(index + 65)
	}
	auth := &fakeAuthenticator{want: model.Sum(credential)}
	request := httptest.NewRequest("POST", "/v1/teamwork/action", nil)
	request.Header.Set(authorizationHeader, profileScheme+base64.RawURLEncoding.EncodeToString(credential))
	request.Header.Set(operationKeyHeader, base64.RawURLEncoding.EncodeToString(operation))
	request.Header.Set(claimContextHeader, base64.RawURLEncoding.EncodeToString(claim))
	metadata, apiErr := authenticateRequest(context.Background(), request, auth, headerPolicy{
		operationRequired: true, operationAllowed: true, claimAllowed: true,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if metadata.Profile.Principal() != "principal-localapi" || auth.seen != model.Sum(credential) ||
		!metadata.HasOperationKey || metadata.OperationKeyHash != model.Sum(operation) ||
		!metadata.HasClaimContext || metadata.ClaimContextHash != model.Sum(claim) ||
		metadata.HasRunAttachment {
		t.Fatalf("request metadata = %#v", metadata)
	}
}

func TestAuthenticateRequestRejectsHeaderAuthoritySmuggling(t *testing.T) {
	t.Parallel()
	secret := make([]byte, opaqueSecretBytes)
	encoded := base64.RawURLEncoding.EncodeToString(secret)
	auth := &fakeAuthenticator{want: model.Sum(secret)}
	tests := []struct {
		name   string
		mutate func(*httpRequestFixture)
		policy headerPolicy
		code   ErrorCode
	}{
		{name: "wrong scheme", mutate: func(f *httpRequestFixture) { f.authorization = "Bearer " + encoded }, code: CodeAuthenticationFailed},
		{name: "padded token", mutate: func(f *httpRequestFixture) { f.authorization += "=" }, code: CodeAuthenticationFailed},
		{name: "operation missing", policy: headerPolicy{operationAllowed: true, operationRequired: true}, code: CodeInvalidArgument},
		{name: "operation on hook", mutate: func(f *httpRequestFixture) { f.operation = encoded }, code: CodeInvalidArgument},
		{name: "claim missing", policy: headerPolicy{operationAllowed: true, operationRequired: true, claimAllowed: true, claimRequired: true},
			mutate: func(f *httpRequestFixture) { f.operation = encoded }, code: CodeContextRequired},
		{name: "attachment on action", policy: headerPolicy{operationAllowed: true, operationRequired: true},
			mutate: func(f *httpRequestFixture) { f.operation, f.attachment = encoded, encoded }, code: CodeInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := httpRequestFixture{authorization: profileScheme + encoded}
			if test.mutate != nil {
				test.mutate(&fixture)
			}
			request := httptest.NewRequest("POST", "/", nil)
			request.Header.Set(authorizationHeader, fixture.authorization)
			if fixture.operation != "" {
				request.Header.Set(operationKeyHeader, fixture.operation)
			}
			if fixture.attachment != "" {
				request.Header.Set(runAttachmentHeader, fixture.attachment)
			}
			_, apiErr := authenticateRequest(context.Background(), request, auth, test.policy)
			if apiErr == nil || apiErr.Code != test.code {
				t.Fatalf("authentication error = %#v, want %s", apiErr, test.code)
			}
		})
	}
}

func TestDecodeOpaqueSecretRejectsAmbiguousEncoding(t *testing.T) {
	t.Parallel()
	valid := base64.RawURLEncoding.EncodeToString(make([]byte, opaqueSecretBytes))
	if decoded, err := decodeOpaqueSecret(valid); err != nil || len(decoded) != opaqueSecretBytes {
		t.Fatalf("valid decode = %d, %v", len(decoded), err)
	}
	for _, value := range []string{"", valid + "=", " " + valid, valid[:len(valid)-1], valid + "\n"} {
		if _, err := decodeOpaqueSecret(value); err == nil {
			t.Errorf("ambiguous secret %q was accepted", value)
		}
	}
}

type httpRequestFixture struct {
	authorization string
	operation     string
	attachment    string
}

func localAPITestProfile() model.Profile {
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	credential := model.Sum([]byte("credential-localapi"))
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-localapi", WorkspaceRoot: "/workspace", Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: credential,
		ActiveAssetRevision: "asset-one", HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: true,
		CreatedAt: now, UpdatedAt: now})
	if err != nil {
		panic(err)
	}
	return profile
}
