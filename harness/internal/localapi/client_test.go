package localapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type clientRoundtripService struct {
	mu       sync.Mutex
	called   []string
	metadata []RequestMetadata
	secret   string
}

type clientAttachmentService struct {
	mu       sync.Mutex
	runID    model.RunID
	metadata []RequestMetadata
	stale    bool
}

func (s *clientAttachmentService) HookCheck(_ context.Context, metadata RequestMetadata,
	_ HookCheckRequest,
) (HookCheckResponse, *APIError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = append(s.metadata, metadata)
	if s.stale {
		return HookCheckResponse{}, NewAPIError(CodeContextStale, "managed Run attachment is stale")
	}
	return HookCheckResponse{Pending: true}, nil
}

func (s *clientAttachmentService) AgentCurrent(_ context.Context, metadata RequestMetadata,
	_ AgentCurrentRequest,
) (AgentCurrentResponse, *APIError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = append(s.metadata, metadata)
	return AgentCurrentResponse{Status: "actionable", RunID: s.runID.String(),
		Projection: []byte(`{"allowed_actions":["teamwork.deliver"],"schema_version":1,"status":"actionable"}`)}, nil
}

func (*clientAttachmentService) TeamworkAction(context.Context, RequestMetadata,
	TeamworkActionRequest,
) (OperationResponse, *APIError) {
	return OperationResponse{}, NewAPIError(CodeInternal, "unexpected action")
}

func (*clientAttachmentService) AgentResolve(context.Context, RequestMetadata,
	AgentResolveRequest,
) (OperationResponse, *APIError) {
	return OperationResponse{}, NewAPIError(CodeInternal, "unexpected resolution")
}

func (s *clientRoundtripService) record(name string, metadata RequestMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called = append(s.called, name)
	s.metadata = append(s.metadata, metadata)
}

func (s *clientRoundtripService) HookCheck(_ context.Context, metadata RequestMetadata,
	_ HookCheckRequest,
) (HookCheckResponse, *APIError) {
	s.record("hook", metadata)
	return HookCheckResponse{Pending: true}, nil
}

func (s *clientRoundtripService) AgentCurrent(_ context.Context, metadata RequestMetadata,
	_ AgentCurrentRequest,
) (AgentCurrentResponse, *APIError) {
	s.record("current", metadata)
	return AgentCurrentResponse{Status: "actionable", RunID: "run-client-one", ClaimSecret: s.secret,
		Projection: []byte(`{"allowed_actions":["teamwork.deliver"],"schema_version":1,"status":"actionable"}`)}, nil
}

func (s *clientRoundtripService) TeamworkAction(_ context.Context, metadata RequestMetadata,
	request TeamworkActionRequest,
) (OperationResponse, *APIError) {
	s.record("action", metadata)
	return OperationResponse{Status: "accepted", Action: "teamwork." + request.Action,
		OperationID: "operation-client-action", Handling: &HandlingReceipt{Status: "completed"},
		Results: []OperationResult{{EventID: "event-client-action", EventType: "review.delivery.ready",
			Work: WorkReceipt{Ref: "peer/work", Version: 2, State: "DELIVERED"}}}, Receipt: "receipt-action"}, nil
}

func (s *clientRoundtripService) AgentResolve(_ context.Context, metadata RequestMetadata,
	request AgentResolveRequest,
) (OperationResponse, *APIError) {
	s.record("resolve", metadata)
	return OperationResponse{Status: "resolved", Action: "agent.resolve." + request.Decision,
		OperationID: "operation-client-resolve", Handling: &HandlingReceipt{Status: "requeued"},
		Results: []OperationResult{}, Receipt: "receipt-resolve"}, nil
}

func TestClientUsesOnlyOwnerUnixControlAndOpaqueHeaders(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x81)
	installClientCredential(t, nodeState, credential)
	claim := base64.RawURLEncoding.EncodeToString(repeatedOpaqueBytes(0x82))
	service := &clientRoundtripService{secret: claim}
	stop := serveClientControl(t, nodeState, model.Sum(credential), service)
	defer stop()

	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	hook, apiErr := client.HookCheck(context.Background())
	if apiErr != nil || !hook.Pending {
		t.Fatalf("HookCheck() = %#v, %#v", hook, apiErr)
	}
	current, apiErr := client.AgentCurrent(context.Background())
	if apiErr != nil || current.Status != "actionable" || current.ClaimSecret != claim {
		t.Fatalf("AgentCurrent() = %#v, %#v", current, apiErr)
	}
	runID, _ := model.ParseRunID(current.RunID)
	contextFile, err := WriteContextFile(nodeState, runID, current.ClaimSecret)
	if err != nil {
		t.Fatal(err)
	}

	action := TeamworkActionRequest{Action: "deliver", Content: "review complete",
		Artifacts: []string{"findings.md"}}
	actionRaw, _ := model.CanonicalMarshal(action)
	journalStore, err := NewPendingJournalStore(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	actionJournal, _, err := journalStore.FindOrCreate(model.Sum(actionRaw), digestPointer(contextFile.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	accepted, apiErr := client.TeamworkAction(context.Background(), action, &contextFile, actionJournal)
	if apiErr != nil || accepted.Action != "teamwork.deliver" || accepted.Status != "accepted" {
		t.Fatalf("TeamworkAction() = %#v, %#v", accepted, apiErr)
	}

	resolve := AgentResolveRequest{Decision: "retry", Content: "provider unavailable"}
	resolveRaw, _ := model.CanonicalMarshal(resolve)
	resolveJournal, _, err := journalStore.FindOrCreate(model.Sum(resolveRaw), digestPointer(contextFile.Digest()))
	if err != nil {
		t.Fatal(err)
	}
	resolved, apiErr := client.AgentResolve(context.Background(), resolve, contextFile, resolveJournal)
	if apiErr != nil || resolved.Action != "agent.resolve.retry" || resolved.Handling == nil ||
		resolved.Handling.Status != "requeued" {
		t.Fatalf("AgentResolve() = %#v, %#v", resolved, apiErr)
	}

	service.mu.Lock()
	defer service.mu.Unlock()
	if strings.Join(service.called, ",") != "hook,current,action,resolve" {
		t.Fatalf("calls = %v", service.called)
	}
	for index, metadata := range service.metadata {
		if metadata.Profile.ID() != model.TeamworkProfileID() {
			t.Fatalf("metadata[%d] Profile = %s", index, metadata.Profile.ID())
		}
		if index < 2 && (metadata.HasOperationKey || metadata.HasClaimContext) {
			t.Fatalf("read metadata[%d] carries operation authority: %#v", index, metadata)
		}
		if index >= 2 && (!metadata.HasOperationKey || !metadata.HasClaimContext ||
			metadata.ClaimContextHash != model.Sum(repeatedOpaqueBytes(0x82))) {
			t.Fatalf("operation metadata[%d] = %#v", index, metadata)
		}
	}
}

func TestClientRunAttachmentEnvPeeksThenConsumesWithoutLeakingToken(t *testing.T) {
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x83)
	installClientCredential(t, nodeState, credential)
	attachmentToken := repeatedOpaqueBytes(0x84)
	stageID := bytes.Repeat([]byte{0x85}, runAttachmentStageIDBytes)
	staged, err := StageRunAttachment(nodeState, bytes.NewReader(append(attachmentToken, stageID...)))
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-client-attachment")
	attachment, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	service := &clientAttachmentService{runID: runID}
	stop := serveClientControl(t, nodeState, model.Sum(credential), service)
	defer stop()
	t.Setenv(RunAttachmentEnv, attachment.Path())
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	hook, apiErr := client.HookCheck(context.Background())
	if apiErr != nil || !hook.Pending {
		t.Fatalf("attached HookCheck() = (%#v, %#v)", hook, apiErr)
	}
	if _, err := os.Lstat(attachment.Path()); err != nil {
		t.Fatalf("Hook consumed attachment: %v", err)
	}
	current, apiErr := client.AgentCurrent(context.Background())
	if apiErr != nil || current.RunID != runID.String() ||
		current.ClaimSecret != base64.RawURLEncoding.EncodeToString(attachmentToken) {
		t.Fatalf("attached AgentCurrent() = (%#v, %#v)", current, apiErr)
	}
	if _, err := os.Lstat(attachment.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumed attachment remains: %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.metadata) != 2 {
		t.Fatalf("attached metadata calls = %d", len(service.metadata))
	}
	wantHash := model.Sum(attachmentToken)
	for index, metadata := range service.metadata {
		if !metadata.HasRunAttachment || metadata.RunAttachmentHash != wantHash ||
			metadata.HasClaimContext || metadata.HasOperationKey {
			t.Fatalf("attached metadata[%d] = %#v", index, metadata)
		}
	}
}

func TestClientPreservesStaleRunAttachmentForAuthoritativeExpiryCleanup(t *testing.T) {
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x86)
	installClientCredential(t, nodeState, credential)
	staged, err := StageRunAttachment(nodeState, bytes.NewReader(bytes.Repeat([]byte{0x87},
		opaqueSecretBytes+runAttachmentStageIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-client-stale-attachment")
	attachment, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	service := &clientAttachmentService{runID: runID, stale: true}
	stop := serveClientControl(t, nodeState, model.Sum(credential), service)
	defer stop()
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	client.attachmentPath = attachment.Path()
	if _, apiErr := client.HookCheck(context.Background()); apiErr == nil || apiErr.Code != CodeContextStale {
		t.Fatalf("stale attachment error = %#v", apiErr)
	}
	if _, err := os.Lstat(attachment.Path()); err != nil {
		t.Fatalf("stale request removed attachment before authoritative cleanup: %v", err)
	}
}

func TestClientProbeHealthUsesGETAuthenticationAndNoCapabilities(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x88)
	installClientCredential(t, nodeState, credential)
	revision := model.Sum([]byte("client-health-assets")).String()
	requestSeen := make(chan error, 1)
	stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
		request *http.Request,
	) {
		body, err := io.ReadAll(request.Body)
		if err == nil && (request.Method != http.MethodGet || request.URL.Path != RouteHealth ||
			request.ContentLength != 0 || len(body) != 0 || request.Header.Get("Content-Type") != "" ||
			request.Header.Get(operationKeyHeader) != "" || request.Header.Get(claimContextHeader) != "" ||
			request.Header.Get(runAttachmentHeader) != "" ||
			request.Header.Get(authorizationHeader) != profileScheme+encodeSecret(credential)) {
			err = errors.New("probe request violates the closed health transport")
		}
		requestSeen <- err
		writeResponse(writer, http.StatusOK, HealthResponse{AssetRevision: revision,
			SchemaVersion: SchemaVersion, Status: "not_ready"})
	}))
	defer stop()
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	response, apiErr := client.ProbeHealth(context.Background())
	if apiErr != nil || response.Status != "not_ready" || response.AssetRevision != revision {
		t.Fatalf("ProbeHealth() = %#v, %#v", response, apiErr)
	}
	if err := <-requestSeen; err != nil {
		t.Fatal(err)
	}
}

func TestVerifyProfileCredentialBindsTokenToDurableDigest(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x87)
	installClientCredential(t, nodeState, credential)
	if err := VerifyProfileCredential(nodeState, model.Sum(credential)); err != nil {
		t.Fatalf("VerifyProfileCredential() error = %v", err)
	}
	if err := VerifyProfileCredential(nodeState, model.Sum([]byte("different credential"))); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("wrong credential binding error = %v", err)
	}
	if err := VerifyProfileCredential(nodeState, model.Digest{}); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("zero credential binding error = %v", err)
	}
}

func TestClientProbeHealthRejectsNonclosedAndOversizeResponses(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("client-health-assets")).String()
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"asset_revision":"` + revision +
			`","peer_id":"secret","schema_version":1,"status":"ready"}` + "\n"},
		{name: "invalid status", body: `{"asset_revision":"` + revision +
			`","schema_version":1,"status":"starting"}` + "\n"},
		{name: "oversize", body: `{"padding":"` + strings.Repeat("x", MaxHealthResponseBytes) + `"}` + "\n"},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeState := newClientNodeState(t)
			credential := repeatedOpaqueBytes(byte(0x89 + index))
			installClientCredential(t, nodeState, credential)
			stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter,
				_ *http.Request,
			) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(test.body))
			}))
			defer stop()
			client, err := NewClient(nodeState)
			if err != nil {
				t.Fatal(err)
			}
			if _, apiErr := client.ProbeHealth(context.Background()); apiErr == nil ||
				apiErr.Code != CodeInternal {
				t.Fatalf("invalid health error = %#v", apiErr)
			}
		})
	}
}

func TestClientContextlessOfferAndStableRemoteError(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0x91)
	installClientCredential(t, nodeState, credential)
	service := &clientErrorService{err: NewAPIError(CodePeerUnavailable, "selected Peer is unavailable")}
	stop := serveClientControl(t, nodeState, model.Sum(credential), service)
	defer stop()
	client, err := NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	offer := TeamworkActionRequest{Action: "offer", Channel: "beta", To: "auto", Content: "review"}
	raw, _ := model.CanonicalMarshal(offer)
	store, _ := NewPendingJournalStore(nodeState)
	journal, _, err := store.FindOrCreate(model.Sum(raw), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := client.TeamworkAction(context.Background(), offer, nil, journal)
	if apiErr == nil || apiErr.Code != CodePeerUnavailable || !apiErr.Retryable || apiErr.Replayed {
		t.Fatalf("remote error = %#v", apiErr)
	}
	if service.metadata.HasClaimContext || !service.metadata.HasOperationKey {
		t.Fatalf("contextless metadata = %#v", service.metadata)
	}
}

type clientErrorService struct {
	err      *APIError
	metadata RequestMetadata
}

func (s *clientErrorService) HookCheck(context.Context, RequestMetadata, HookCheckRequest) (HookCheckResponse, *APIError) {
	return HookCheckResponse{}, s.err
}
func (s *clientErrorService) AgentCurrent(context.Context, RequestMetadata, AgentCurrentRequest) (AgentCurrentResponse, *APIError) {
	return AgentCurrentResponse{}, s.err
}
func (s *clientErrorService) TeamworkAction(_ context.Context, metadata RequestMetadata,
	_ TeamworkActionRequest,
) (OperationResponse, *APIError) {
	s.metadata = metadata
	return OperationResponse{}, s.err
}
func (s *clientErrorService) AgentResolve(context.Context, RequestMetadata, AgentResolveRequest) (OperationResponse, *APIError) {
	return OperationResponse{}, s.err
}

func TestClientFailsClosedBeforeSendingChangedReplayHandles(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	credential := repeatedOpaqueBytes(0xa1)
	installClientCredential(t, nodeState, credential)
	service := &clientRoundtripService{secret: testOpaqueValue(0xa2)}
	stop := serveClientControl(t, nodeState, model.Sum(credential), service)
	defer stop()
	client, _ := NewClient(nodeState)
	runID, _ := model.ParseRunID("run-client-fence")
	contextFile, _ := WriteContextFile(nodeState, runID, testOpaqueValue(0xa2))
	action := TeamworkActionRequest{Action: "deliver", Content: "done"}
	raw, _ := model.CanonicalMarshal(action)
	store, _ := NewPendingJournalStore(nodeState)
	journal, _, _ := store.FindOrCreate(model.Sum(raw), digestPointer(contextFile.Digest()))

	changed := action
	changed.Content = "changed"
	if _, apiErr := client.TeamworkAction(context.Background(), changed, &contextFile, journal); apiErr == nil ||
		apiErr.Code != CodeOperationMismatch {
		t.Fatalf("changed request error = %#v", apiErr)
	}
	if len(service.called) != 0 {
		t.Fatal("changed request reached mnemond")
	}

	moved := contextFile.Path() + ".original"
	if err := os.Rename(contextFile.Path(), moved); err != nil {
		t.Fatal(err)
	}
	bytesOnDisk, _ := os.ReadFile(moved)
	mustWrite(t, contextFile.Path(), bytesOnDisk, ownerRegularFileMode)
	if _, apiErr := client.TeamworkAction(context.Background(), action, &contextFile, journal); apiErr == nil ||
		apiErr.Code != CodeContextInvalid {
		t.Fatalf("replacement context error = %#v", apiErr)
	}
	if len(service.called) != 0 {
		t.Fatal("replacement context reached mnemond")
	}
}

func TestClientCredentialSocketAndResponseValidationFailClosed(t *testing.T) {
	t.Parallel()
	t.Run("credential", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		profiles := filepath.Join(nodeState, "profiles")
		if err := os.Mkdir(profiles, ownerDirectoryMode); err != nil {
			t.Fatal(err)
		}
		secret := testOpaqueValue(0xb1)
		path := filepath.Join(profiles, model.TeamworkProfileID().String()+profileTokenSuffix)
		mustWrite(t, path, []byte(secret+"\n"), 0o644)
		if _, err := NewClient(nodeState); !errors.Is(err, ErrUnsafeClientState) || strings.Contains(err.Error(), secret) {
			t.Fatalf("unsafe credential error = %v", err)
		}
	})

	t.Run("missing socket", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		installClientCredential(t, nodeState, repeatedOpaqueBytes(0xb2))
		client, err := NewClient(nodeState)
		if err != nil {
			t.Fatal(err)
		}
		_, apiErr := client.HookCheck(context.Background())
		if apiErr == nil || apiErr.Code != CodeMnemondUnavailable || !apiErr.Retryable {
			t.Fatalf("missing socket error = %#v", apiErr)
		}
	})

	t.Run("noncanonical response", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		installClientCredential(t, nodeState, repeatedOpaqueBytes(0xb3))
		stop := serveRawClientControl(t, nodeState, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte("{ \"pending\":true,\"schema_version\":1}\n"))
		}))
		defer stop()
		client, _ := NewClient(nodeState)
		_, apiErr := client.HookCheck(context.Background())
		if apiErr == nil || apiErr.Code != CodeInternal {
			t.Fatalf("noncanonical response error = %#v", apiErr)
		}
	})
}

func installClientCredential(t *testing.T, nodeState string, raw []byte) {
	t.Helper()
	profiles := filepath.Join(nodeState, "profiles")
	if err := os.Mkdir(profiles, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	mustWrite(t, filepath.Join(profiles, model.TeamworkProfileID().String()+profileTokenSuffix),
		[]byte(encoded+"\n"), ownerRegularFileMode)
}

func serveClientControl(t *testing.T, nodeState string, credential model.Digest,
	service Service,
) func() {
	t.Helper()
	server, err := NewServer(&fakeAuthenticator{want: credential}, service)
	if err != nil {
		t.Fatal(err)
	}
	return serveRawClientControl(t, nodeState, server.Handler())
}

func serveRawClientControl(t *testing.T, nodeState string, handler http.Handler) func() {
	t.Helper()
	listener, err := ListenOwnerUnix(filepath.Join(nodeState, "control.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	done := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(done)
	}()
	return func() {
		_ = server.Close()
		_ = listener.Close()
		<-done
	}
}

func digestPointer(value model.Digest) *model.Digest { return &value }
