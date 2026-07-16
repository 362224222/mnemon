package localapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAuthorityResponseIsClosedCanonicalAndSecretFree(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("authority-assets")).String()
	peerID, _ := model.ParsePeerID("peer-authority-response")
	at := time.Date(2026, 7, 17, 8, 9, 10, 123, time.FixedZone("offset", 8*60*60))
	for _, enabled := range []bool{false, true} {
		response, err := NewAuthorityResponse(AuthoritySnapshot{Host: model.HostCodex,
			Runtime: model.RuntimeCodexAppServer, Enabled: enabled, AssetRevision: revision,
			UpdatedAt: at, PeerID: peerID, ActiveAssetRevision: revision})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := model.CanonicalMarshal(response)
		want := `{"active_asset_revision":"` + revision + `","asset_revision":"` + revision +
			`","enabled":` + map[bool]string{false: "false", true: "true"}[enabled]
		if err != nil || !strings.Contains(string(raw), `"peer_id":"peer-authority-response"`) ||
			strings.Contains(string(raw), "principal") || strings.Contains(string(raw), "credential") ||
			response.UpdatedAt != "2026-07-17T00:09:10.000000123Z" ||
			(response.Enabled != enabled) || !strings.HasPrefix(string(raw), want) {
			t.Fatalf("authority response = %s (%#v, %v)", raw, response, err)
		}
		if apiErr := validateAuthorityResponse(response); apiErr != nil {
			t.Fatalf("validateAuthorityResponse() = %v", apiErr)
		}
	}
}

func TestAuthorityResponseRejectsInvalidState(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("authority-assets")).String()
	peerID, _ := model.ParsePeerID("peer-authority-invalid")
	valid := AuthoritySnapshot{Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		Enabled: true, AssetRevision: revision, UpdatedAt: time.Now(), PeerID: peerID,
		ActiveAssetRevision: revision}
	tests := []struct {
		name   string
		mutate func(*AuthoritySnapshot)
	}{
		{name: "Host", mutate: func(value *AuthoritySnapshot) { value.Host = "multica" }},
		{name: "Runtime", mutate: func(value *AuthoritySnapshot) { value.Runtime = model.RuntimeClaudeCLI }},
		{name: "Profile revision", mutate: func(value *AuthoritySnapshot) { value.AssetRevision = "asset-r5" }},
		{name: "Node revision", mutate: func(value *AuthoritySnapshot) {
			value.ActiveAssetRevision = model.Sum([]byte("other-assets")).String()
		}},
		{name: "PeerID", mutate: func(value *AuthoritySnapshot) { value.PeerID = model.PeerID{} }},
		{name: "time", mutate: func(value *AuthoritySnapshot) { value.UpdatedAt = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if _, err := NewAuthorityResponse(value); err == nil {
				t.Fatal("NewAuthorityResponse() accepted invalid authority")
			}
		})
	}
}

func TestServerAuthorityIsAuthenticatedStrictAndCurrent(t *testing.T) {
	t.Parallel()
	credential := repeatedOpaqueBytes(0x3a)
	revision := model.Sum([]byte("server-authority-assets")).String()
	peerID, _ := model.ParsePeerID("peer-server-authority")
	at := time.Date(2026, 7, 17, 4, 5, 6, 7, time.UTC)
	called := 0
	provider := AuthorityProviderFunc(func(ctx context.Context,
		metadata RequestMetadata,
	) (AuthoritySnapshot, *APIError) {
		called++
		if ctx == nil || metadata.Profile.ID() != model.TeamworkProfileID() ||
			metadata.HasOperationKey || metadata.HasClaimContext || metadata.HasRunAttachment {
			t.Fatalf("authority metadata = %#v", metadata)
		}
		return AuthoritySnapshot{Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
			Enabled: called == 1, AssetRevision: revision, UpdatedAt: at,
			PeerID: peerID, ActiveAssetRevision: revision}, nil
	})
	server, err := NewServerWithAuthority(&fakeAuthenticator{want: modelDigest(credential)},
		&fakeService{}, HealthProviderFunc(func(context.Context, RequestMetadata) (HealthSnapshot, *APIError) {
			return HealthSnapshot{AssetRevision: revision, WorkersReady: true}, nil
		}), provider)
	if err != nil {
		t.Fatal(err)
	}
	for index, enabled := range []bool{true, false} {
		request := httptest.NewRequest(http.MethodGet, RouteAuthority, nil)
		request.Header.Set(authorizationHeader, profileScheme+encodeSecret(credential))
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(),
			`"enabled":`+map[bool]string{true: "true", false: "false"}[enabled]) || called != index+1 ||
			recorder.Header().Get("Cache-Control") != "no-store" ||
			IsAgentRoute(RouteAuthority) || !IsControlRoute(RouteAuthority) {
			t.Fatalf("authority response = %d %q called=%d", recorder.Code,
				recorder.Body.String(), called)
		}
	}
}

func TestServerAuthorityRejectsMethodContentCapabilitiesAndAuthentication(t *testing.T) {
	t.Parallel()
	credential := repeatedOpaqueBytes(0x3b)
	secret := encodeSecret(repeatedOpaqueBytes(0x3c))
	called := 0
	provider := AuthorityProviderFunc(func(context.Context, RequestMetadata) (AuthoritySnapshot, *APIError) {
		called++
		return AuthoritySnapshot{}, nil
	})
	health := HealthProviderFunc(func(context.Context, RequestMetadata) (HealthSnapshot, *APIError) {
		return HealthSnapshot{AssetRevision: model.Sum([]byte("strict-health-assets")).String(),
			WorkersReady: true}, nil
	})
	server, err := NewServerWithAuthority(&fakeAuthenticator{want: modelDigest(credential)},
		&fakeService{}, health, provider)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		mutate func(*http.Request)
		status int
		allow  string
	}{
		{name: "method", method: http.MethodPost, status: http.StatusMethodNotAllowed, allow: http.MethodGet},
		{name: "body", method: http.MethodGet, body: `{}`, status: http.StatusBadRequest},
		{name: "content type", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set("Content-Type", "application/json")
		}, status: http.StatusBadRequest},
		{name: "query", method: http.MethodGet, path: RouteAuthority + "?detail=1", status: http.StatusBadRequest},
		{name: "operation", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(operationKeyHeader, secret)
		}, status: http.StatusBadRequest},
		{name: "claim", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(claimContextHeader, secret)
		}, status: http.StatusBadRequest},
		{name: "attachment", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(runAttachmentHeader, secret)
		}, status: http.StatusBadRequest},
		{name: "authentication", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(authorizationHeader,
				profileScheme+encodeSecret(repeatedOpaqueBytes(0x3d)))
		}, status: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := test.path
			if path == "" {
				path = RouteAuthority
			}
			var body io.Reader
			if test.body != "" {
				body = strings.NewReader(test.body)
			}
			request := httptest.NewRequest(test.method, path, body)
			request.Header.Set(authorizationHeader, profileScheme+encodeSecret(credential))
			if test.mutate != nil {
				test.mutate(request)
			}
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != test.status || recorder.Header().Get("Allow") != test.allow {
				t.Fatalf("authority rejection = %d %s Allow=%q", recorder.Code,
					recorder.Body.String(), recorder.Header().Get("Allow"))
			}
		})
	}
	if called != 0 {
		t.Fatalf("rejected authority requests called provider %d times", called)
	}
}

func TestClientReadAuthorityUsesClosedGETAndRejectsNonclosedResponses(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("client-authority-assets")).String()
	peerID, _ := model.ParsePeerID("peer-client-authority")
	at := time.Date(2026, 7, 17, 7, 8, 9, 10, time.UTC)
	t.Run("request", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		credential := repeatedOpaqueBytes(0x3e)
		installClientCredential(t, nodeState, credential)
		seen := make(chan error, 1)
		stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
			request *http.Request,
		) {
			body, err := io.ReadAll(request.Body)
			if err == nil && (request.Method != http.MethodGet || request.URL.Path != RouteAuthority ||
				request.ContentLength != 0 || len(body) != 0 || request.Header.Get("Content-Type") != "" ||
				request.Header.Get(operationKeyHeader) != "" || request.Header.Get(claimContextHeader) != "" ||
				request.Header.Get(runAttachmentHeader) != "" ||
				request.Header.Get(authorizationHeader) != profileScheme+encodeSecret(credential)) {
				err = errors.New("authority request violates the closed transport")
			}
			seen <- err
			response, _ := NewAuthorityResponse(AuthoritySnapshot{Host: model.HostCodex,
				Runtime: model.RuntimeCodexAppServer, Enabled: false, AssetRevision: revision,
				UpdatedAt: at, PeerID: peerID, ActiveAssetRevision: revision})
			writeResponse(writer, http.StatusOK, response)
		}))
		defer stop()
		client, err := NewClient(nodeState)
		if err != nil {
			t.Fatal(err)
		}
		response, apiErr := client.ReadAuthority(context.Background())
		if apiErr != nil || response.Enabled || response.PeerID != peerID.String() ||
			response.AssetRevision != revision {
			t.Fatalf("ReadAuthority() = (%#v, %v)", response, apiErr)
		}
		if err := <-seen; err != nil {
			t.Fatal(err)
		}
	})

	valid, _ := NewAuthorityResponse(AuthoritySnapshot{Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, Enabled: false, AssetRevision: revision,
		UpdatedAt: at, PeerID: peerID, ActiveAssetRevision: revision})
	validRaw, _ := model.CanonicalMarshal(valid)
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: strings.Replace(string(validRaw)+"\n", `,"runtime"`,
			`,"principal":"secret","runtime"`, 1)},
		{name: "missing enabled", body: strings.Replace(string(validRaw)+"\n", `"enabled":false,`, "", 1)},
		{name: "noncanonical time", body: strings.Replace(string(validRaw)+"\n", valid.UpdatedAt,
			"2026-07-17T07:08:09.000000010+00:00", 1)},
		{name: "revision drift", body: strings.Replace(string(validRaw)+"\n", revision,
			model.Sum([]byte("different-client-assets")).String(), 1)},
		{name: "oversize", body: `{"padding":"` + strings.Repeat("x", MaxAuthorityResponseBytes) + `"}` + "\n"},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeState := newClientNodeState(t)
			credential := repeatedOpaqueBytes(byte(0x40 + index))
			installClientCredential(t, nodeState, credential)
			stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
				_ *http.Request,
			) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(writer, test.body)
			}))
			defer stop()
			client, _ := NewClient(nodeState)
			if _, apiErr := client.ReadAuthority(context.Background()); apiErr == nil ||
				apiErr.Code != CodeInternal {
				t.Fatalf("nonclosed authority error = %#v", apiErr)
			}
		})
	}
}
