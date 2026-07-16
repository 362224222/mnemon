package localapi

import (
	"context"
	"encoding/base64"
	"errors"
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
