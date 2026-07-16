package localapi

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	profileTokenSuffix = ".token"
	profileTokenBytes  = 43 + 1
	maxControlResponse = (256 << 10) + 1024
)

// Client is the only Agent-side transport to mnemond. It can dial only the
// owner-only Unix socket inside one Node state directory.
type Client struct {
	nodeState      string
	socket         string
	attachmentPath string
	ownerUID       uint32
	profile        model.ProfileID
	token          [opaqueSecretBytes]byte
	http           *http.Client
}

// NewClient loads the fixed Teamwork Profile credential from owner-only Node
// state. It deliberately does not require mnemond to be running yet; a failed
// dial is reported by each command as mnemond_unavailable.
func NewClient(nodeState string) (*Client, error) {
	ownerUID, err := validateNodeStateDirectory(nodeState)
	if err != nil {
		return nil, err
	}
	profiles, _, err := requireOwnerSubdirectory(nodeState, "profiles")
	if err != nil {
		return nil, err
	}
	profile := model.TeamworkProfileID()
	path := filepath.Join(profiles, profile.String()+profileTokenSuffix)
	raw, _, err := readOwnerRegularFile(path, profileTokenBytes, ownerUID)
	if err != nil {
		return nil, unsafeClientState("Profile credential is unavailable")
	}
	if len(raw) != profileTokenBytes || raw[len(raw)-1] != '\n' {
		return nil, unsafeClientState("Profile credential has noncanonical bytes")
	}
	decoded, err := decodeOpaqueSecret(string(raw[:len(raw)-1]))
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != string(raw[:len(raw)-1]) {
		clear(decoded)
		return nil, unsafeClientState("Profile credential has noncanonical bytes")
	}
	client := &Client{
		nodeState:      nodeState,
		socket:         filepath.Join(nodeState, "control.sock"),
		attachmentPath: os.Getenv(RunAttachmentEnv),
		ownerUID:       ownerUID,
		profile:        profile,
	}
	copy(client.token[:], decoded)
	clear(decoded)
	transport := &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		ForceAttemptHTTP2: false,
		DialContext:       client.dialContext,
	}
	client.http = &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return client, nil
}

// VerifyProfileCredential binds the owner-only token projection back to the
// immutable Profile digest in the Store without exposing the raw credential.
// Daemon startup uses it before opening the local control socket.
func VerifyProfileCredential(nodeState string, expected model.Digest) error {
	if expected.IsZero() {
		return unsafeClientState("Profile credential binding is unavailable")
	}
	client, err := NewClient(nodeState)
	if err != nil {
		return err
	}
	defer clear(client.token[:])
	actual := model.Sum(client.token[:])
	wantBytes := expected.Bytes()
	actualBytes := actual.Bytes()
	matched := subtle.ConstantTimeCompare(wantBytes, actualBytes)
	clear(wantBytes)
	clear(actualBytes)
	if matched != 1 {
		return unsafeClientState("Profile credential differs from durable authority")
	}
	return nil
}

// ProbeHealth performs the authenticated, identity-free daemon readiness
// probe. A valid not_ready response is returned to the caller as health state,
// not rewritten into a transport error.
func (c *Client) ProbeHealth(ctx context.Context) (HealthResponse, *APIError) {
	var response HealthResponse
	if c == nil || c.http == nil || ctx == nil {
		return HealthResponse{}, invalidControlResponse("local control client is unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://mnemond"+RouteHealth, nil)
	if err != nil {
		return HealthResponse{}, invalidControlResponse("local control request cannot be created")
	}
	request.Header.Set(authorizationHeader,
		profileScheme+base64.RawURLEncoding.EncodeToString(c.token[:]))
	if apiErr := c.send(request, &response, MaxHealthResponseBytes); apiErr != nil {
		return HealthResponse{}, apiErr
	}
	if apiErr := validateHealthResponse(response); apiErr != nil {
		return HealthResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) HookCheck(ctx context.Context) (HookCheckResponse, *APIError) {
	attachment, apiErr := c.runtimeAttachment()
	if apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	headers := clientHeaders{}
	if attachment != nil {
		headers.attachment = attachment.HeaderValue()
	}
	var response HookCheckResponse
	if apiErr := c.post(ctx, RouteHookCheck, HookCheckRequest{}, headers, &response); apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	if response.SchemaVersion != SchemaVersion {
		return HookCheckResponse{}, invalidControlResponse("hook response schema is unsupported")
	}
	if attachment != nil && !response.Pending {
		return HookCheckResponse{}, invalidControlResponse("attached Hook did not confirm its preclaim")
	}
	return response, nil
}

func (c *Client) AgentCurrent(ctx context.Context) (AgentCurrentResponse, *APIError) {
	attachment, apiErr := c.runtimeAttachment()
	if apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	headers := clientHeaders{}
	if attachment != nil {
		headers.attachment = attachment.HeaderValue()
	}
	var response AgentCurrentResponse
	if apiErr := c.post(ctx, RouteAgentCurrent, AgentCurrentRequest{}, headers, &response); apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	if attachment != nil {
		runID, err := model.ParseRunID(response.RunID)
		if response.Status != "actionable" || err != nil || runID != attachment.RunID() ||
			response.ClaimSecret != "" {
			return AgentCurrentResponse{}, invalidControlResponse(
				"attached current response differs from its preclaimed Run")
		}
		response.ClaimSecret = attachment.HeaderValue()
	}
	if apiErr := validateCurrentResponse(response); apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	if attachment != nil {
		if err := RemoveRunAttachment(c.nodeState, *attachment); err != nil {
			return AgentCurrentResponse{}, NewAPIError(CodeContextInvalid,
				"consumed Run attachment could not be removed safely")
		}
	}
	return response, nil
}

// TeamworkAction sends one already validated closed action. The pending
// journal must describe the exact canonical request and exact context file;
// otherwise no request is sent.
func (c *Client) TeamworkAction(ctx context.Context, request TeamworkActionRequest,
	contextFile *ContextFile, journal PendingJournal,
) (OperationResponse, *APIError) {
	if !validTeamworkAction(request.Action) {
		return OperationResponse{}, NewAPIError(CodeUnknownAction, "unknown Teamwork action")
	}
	headers, apiErr := c.operationHeaders(request, contextFile, journal)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	var response OperationResponse
	if apiErr := c.post(ctx, RouteTeamworkAction, request, headers, &response); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if apiErr := validateOperationResponse(response, "teamwork."+request.Action); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if contextFile == nil && response.Handling != nil {
		return OperationResponse{}, invalidControlResponse("contextless offer returned a handling receipt")
	}
	if contextFile != nil && (response.Handling == nil || response.Handling.Status != "completed") {
		return OperationResponse{}, invalidControlResponse("context-bound action lacks completed handling evidence")
	}
	if request.Action != "offer" && len(response.Results) != 1 {
		return OperationResponse{}, invalidControlResponse("non-offer action returned the wrong result count")
	}
	return response, nil
}

func (c *Client) AgentResolve(ctx context.Context, request AgentResolveRequest,
	contextFile ContextFile, journal PendingJournal,
) (OperationResponse, *APIError) {
	if !validResolveDecision(request.Decision) {
		return OperationResponse{}, NewAPIError(CodeUnknownAction, "unknown handling resolution")
	}
	headers, apiErr := c.operationHeaders(request, &contextFile, journal)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	var response OperationResponse
	if apiErr := c.post(ctx, RouteAgentResolve, request, headers, &response); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	if apiErr := validateOperationResponse(response, "agent.resolve."+request.Decision); apiErr != nil {
		return OperationResponse{}, apiErr
	}
	wantHandling := map[string]string{"no-action": "completed", "retry": "requeued", "reject": "rejected"}[request.Decision]
	if response.Handling == nil || response.Handling.Status != wantHandling {
		return OperationResponse{}, invalidControlResponse("resolution handling status differs from its decision")
	}
	return response, nil
}

type clientHeaders struct {
	operation  string
	claim      string
	attachment string
}

func (c *Client) operationHeaders(request any, contextFile *ContextFile,
	journal PendingJournal,
) (clientHeaders, *APIError) {
	if c == nil {
		return clientHeaders{}, invalidControlResponse("local control client is unavailable")
	}
	body, err := model.CanonicalMarshal(request)
	if err != nil || len(body) == 0 || body[0] != '{' {
		return clientHeaders{}, NewAPIError(CodeInvalidArgument, "request cannot be encoded canonically")
	}
	if journal.requestDigest != model.Sum(body) {
		return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation does not match request")
	}
	currentJournal, err := c.readExpectedJournal(journal)
	if err != nil {
		return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation identity is invalid")
	}
	if currentJournal.requestDigest != journal.requestDigest ||
		subtle.ConstantTimeCompare(currentJournal.operationKey[:], journal.operationKey[:]) != 1 {
		return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation identity changed")
	}
	headers := clientHeaders{operation: currentJournal.OperationKeyHeader()}
	if contextFile == nil {
		if currentJournal.hasContext {
			return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation expects managed context")
		}
		return headers, nil
	}
	verified, err := ReadContextFile(c.nodeState, contextFile.path)
	if err != nil || contextFile.identity == nil || !os.SameFile(verified.identity, contextFile.identity) ||
		verified.runID != contextFile.runID || verified.digest != contextFile.digest ||
		subtle.ConstantTimeCompare(verified.token[:], contextFile.token[:]) != 1 {
		return clientHeaders{}, NewAPIError(CodeContextInvalid, "managed context identity is invalid")
	}
	if !currentJournal.hasContext || currentJournal.contextFileDigest != verified.digest {
		return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation context does not match")
	}
	headers.claim = verified.HeaderValue()
	return headers, nil
}

func (c *Client) readExpectedJournal(expected PendingJournal) (PendingJournal, error) {
	if expected.identity == nil || expected.path == "" || expected.requestDigest.IsZero() {
		return PendingJournal{}, ErrUnsafeClientState
	}
	operations, uid, err := requireOwnerSubdirectory(c.nodeState, "operations")
	if err != nil || uid != c.ownerUID {
		return PendingJournal{}, ErrUnsafeClientState
	}
	if _, err := parsePendingJournalPath(operations, expected.path); err != nil {
		return PendingJournal{}, err
	}
	var current PendingJournal
	err = withOwnerDirectoryLock(operations, c.ownerUID, func() error {
		var readErr error
		current, readErr = readPendingJournalFile(operations, expected.path, c.ownerUID)
		return readErr
	})
	if err != nil || !os.SameFile(current.identity, expected.identity) ||
		current.fileDigest != expected.fileDigest || current.createdAt != expected.createdAt ||
		current.requestDigest != expected.requestDigest ||
		!sameContextDigest(current, expected.contextFileDigest, expected.hasContext) ||
		subtle.ConstantTimeCompare(current.operationKey[:], expected.operationKey[:]) != 1 {
		return PendingJournal{}, ErrUnsafeClientState
	}
	return current, nil
}

func (c *Client) post(ctx context.Context, route string, input any, headers clientHeaders,
	response any,
) *APIError {
	if c == nil || c.http == nil || ctx == nil || !IsAgentRoute(route) {
		return invalidControlResponse("local control client is unavailable")
	}
	body, err := model.CanonicalMarshal(input)
	if err != nil || len(body) == 0 || body[0] != '{' || len(body) > MaxRequestBodyBytes {
		return NewAPIError(CodeInvalidArgument, "request cannot be encoded canonically")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://mnemond"+route, bytes.NewReader(body))
	if err != nil {
		return invalidControlResponse("local control request cannot be created")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(authorizationHeader, profileScheme+base64.RawURLEncoding.EncodeToString(c.token[:]))
	if headers.operation != "" {
		request.Header.Set(operationKeyHeader, headers.operation)
	}
	if headers.claim != "" {
		request.Header.Set(claimContextHeader, headers.claim)
	}
	if headers.attachment != "" {
		request.Header.Set(runAttachmentHeader, headers.attachment)
	}

	return c.send(request, response, maxControlResponse)
}

func (c *Client) runtimeAttachment() (*RunAttachment, *APIError) {
	if c == nil || c.attachmentPath == "" {
		return nil, nil
	}
	attachment, err := ReadRunAttachment(c.nodeState, c.attachmentPath)
	if err != nil {
		return nil, NewAPIError(CodeContextInvalid, "managed Run attachment is unavailable")
	}
	return &attachment, nil
}

func (c *Client) send(request *http.Request, response any, maxResponse int64) *APIError {
	if c == nil || c.http == nil || request == nil || response == nil || maxResponse <= 0 {
		return invalidControlResponse("local control client is unavailable")
	}
	httpResponse, err := c.http.Do(request)
	if err != nil {
		return NewAPIError(CodeMnemondUnavailable, "mnemond local control is unavailable")
	}
	defer httpResponse.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(httpResponse.Body, maxResponse+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > maxResponse {
		return invalidControlResponse("local control response exceeds its closed bound")
	}
	if httpResponse.Header.Get("Content-Type") != "application/json" {
		return invalidControlResponse("local control response has the wrong content type")
	}
	object, apiErr := canonicalResponseObject(raw)
	if apiErr != nil {
		return apiErr
	}
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		var remote APIError
		if err := decodeClosedObject(object, &remote); err != nil || validateAPIError(&remote) != nil {
			return invalidControlResponse("mnemond returned an invalid error envelope")
		}
		if httpStatusForError(&remote) != httpResponse.StatusCode {
			return invalidControlResponse("mnemond error status differs from its envelope")
		}
		return &remote
	}
	if httpResponse.StatusCode != http.StatusOK {
		return invalidControlResponse("mnemond success used an unsupported HTTP status")
	}
	if err := decodeClosedObject(object, response); err != nil {
		return invalidControlResponse("mnemond returned an invalid success envelope")
	}
	return nil
}

func (c *Client) dialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	identity, err := validateOwnerSocket(c.socket, c.ownerUID)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "unix", c.socket)
	if err != nil {
		return nil, err
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return nil, errors.New("local API: control connection is not Unix")
	}
	peer, err := peerUID(unixConnection)
	if err != nil || peer != c.ownerUID {
		_ = connection.Close()
		return nil, errors.New("local API: control peer owner mismatch")
	}
	current, err := validateOwnerSocket(c.socket, c.ownerUID)
	if err != nil || !os.SameFile(identity, current) {
		_ = connection.Close()
		return nil, errors.New("local API: control socket changed while connecting")
	}
	return connection, nil
}

func validateOwnerSocket(path string, ownerUID uint32) (os.FileInfo, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("local API: control socket path is not canonical")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeType != os.ModeSocket || info.Mode().Perm() != ownerSocketMode {
		return nil, errors.New("local API: control socket is not owner-only")
	}
	uid, err := fileOwnerUID(info)
	if err != nil || uid != ownerUID {
		return nil, errors.New("local API: control socket has the wrong owner")
	}
	return info, nil
}

func canonicalResponseObject(raw []byte) ([]byte, *APIError) {
	if len(raw) < 2 || raw[len(raw)-1] != '\n' || raw[len(raw)-2] == '\n' {
		return nil, invalidControlResponse("local control response is not canonical")
	}
	object := raw[:len(raw)-1]
	canonical, err := model.CanonicalizeJSON(object)
	if err != nil || len(canonical) == 0 || canonical[0] != '{' || !bytes.Equal(canonical, object) {
		return nil, invalidControlResponse("local control response is not canonical")
	}
	return object, nil
}

func decodeClosedObject(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return expectJSONEOF(decoder)
}

func validateAPIError(apiErr *APIError) error {
	if apiErr == nil || apiErr.SchemaVersion != SchemaVersion || apiErr.Status != "error" ||
		!apiErr.Code.Valid() || apiErr.Retryable != apiErr.Code.Retryable() ||
		strings.TrimSpace(apiErr.Message) != apiErr.Message || apiErr.Message == "" ||
		len([]byte(apiErr.Message)) > MaxDiagnosticBytes {
		return errors.New("invalid API error")
	}
	if apiErr.Replayed && apiErr.OperationID == nil {
		return errors.New("replayed API error lacks operation identity")
	}
	if apiErr.OperationID != nil {
		if _, err := model.ParseOperationID(*apiErr.OperationID); err != nil {
			return errors.New("invalid API error operation identity")
		}
	}
	return nil
}

func validateCurrentResponse(response AgentCurrentResponse) *APIError {
	if response.SchemaVersion != SchemaVersion ||
		(response.Status != "none" && response.Status != "busy" && response.Status != "waiting" &&
			response.Status != "actionable") {
		return invalidControlResponse("current response has an invalid status")
	}
	if len(response.Projection) > 256<<10 {
		return NewAPIError(CodeCurrentTooLarge, "current projection exceeds 256 KiB")
	}
	if len(response.Projection) != 0 {
		value, err := model.NewJSON(response.Projection)
		if err != nil || value.Bytes()[0] != '{' || !bytes.Equal(value.Bytes(), response.Projection) {
			return invalidControlResponse("current projection is not canonical")
		}
	}
	if response.Status == "actionable" {
		if _, err := model.ParseRunID(response.RunID); err != nil || len(response.Projection) == 0 {
			return invalidControlResponse("actionable current lacks Run or projection")
		}
		secret, err := decodeOpaqueSecret(response.ClaimSecret)
		clear(secret)
		if err != nil {
			return invalidControlResponse("actionable current has an invalid claim capability")
		}
		return nil
	}
	if response.RunID != "" || response.ClaimSecret != "" {
		return invalidControlResponse("non-actionable current carries claim authority")
	}
	if response.Status != "none" && len(response.Projection) != 0 {
		return invalidControlResponse("busy or waiting current carries a projection")
	}
	return nil
}

func validateOperationResponse(response OperationResponse, expectedAction string) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Action != expectedAction ||
		(response.Status != "accepted" && response.Status != "resolved") ||
		response.OperationID == "" || response.Receipt == "" ||
		len(response.Receipt) > MaxDiagnosticBytes || response.Results == nil ||
		len(response.Results) > model.MaxChildWorks {
		return invalidControlResponse("operation response has an invalid envelope")
	}
	if _, err := model.ParseOperationID(response.OperationID); err != nil {
		return invalidControlResponse("operation response has an invalid identity")
	}
	if strings.HasPrefix(expectedAction, "teamwork.") != (response.Status == "accepted") ||
		strings.HasPrefix(expectedAction, "agent.resolve.") != (response.Status == "resolved") {
		return invalidControlResponse("operation response status does not match its action")
	}
	if response.Status == "resolved" && len(response.Results) != 0 {
		return invalidControlResponse("handling resolution returned domain results")
	}
	for _, result := range response.Results {
		if _, err := model.ParseEventID(result.EventID); err != nil || result.EventType == "" ||
			result.Work.Ref == "" || result.Work.Version == 0 || !model.WorkState(result.Work.State).Valid() {
			return invalidControlResponse("operation response contains an invalid result")
		}
	}
	if response.Handling != nil && response.Handling.Status != "completed" &&
		response.Handling.Status != "requeued" && response.Handling.Status != "rejected" {
		return invalidControlResponse("operation response has an invalid handling receipt")
	}
	if response.Status == "resolved" && response.Handling == nil {
		return invalidControlResponse("handling resolution lacks a handling receipt")
	}
	return nil
}

func invalidControlResponse(message string) *APIError {
	return NewAPIError(CodeInternal, fmt.Sprintf("%s", message))
}
