package localapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

// AgencyClient is the machine-side R7 client. It loads no R5 Profile and has
// no Channel, Work, or Teamwork methods. Callers are responsible for keeping
// attachments, operations, and capture digests in owner-private journals and
// projecting only View, Intent, Receipt, and candidate Handle to an Agent.
type AgencyClient struct {
	transport *Client
}

func NewAgencyClient(nodeState string) (*AgencyClient, error) {
	ownerUID, err := validateNodeStateDirectory(nodeState)
	if err != nil {
		return nil, err
	}
	transport := &Client{nodeState: nodeState, socket: filepath.Join(nodeState, "control.sock"),
		ownerUID: ownerUID}
	httpTransport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ForceAttemptHTTP2: false,
		DialContext: transport.dialContext}
	transport.http = &http.Client{Transport: httpTransport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	return &AgencyClient{transport: transport}, nil
}

func (client *AgencyClient) Attach(ctx context.Context) (node.AgencyAttachment, *APIError) {
	var response agencyAttachmentWire
	if apiErr := client.post(ctx, RouteAgencyAttachments, struct{}{}, nil, &response,
		maxAgencyPrivateResponse); apiErr != nil {
		return node.AgencyAttachment{}, apiErr
	}
	if response.Schema != agencyAttachmentSchema || response.Version != agencyWireVersion {
		return node.AgencyAttachment{}, invalidControlResponse("Agency attachment response schema is unsupported")
	}
	credential, decodeErr := decodeOpaqueSecret(response.Credential)
	if decodeErr != nil {
		clear(credential)
		return node.AgencyAttachment{}, invalidControlResponse("Agency attachment response is invalid")
	}
	expiresAt, err := time.Parse(timeWireLayout, response.ExpiresAt)
	if err != nil || expiresAt.IsZero() || response.ExpiresAt != expiresAt.UTC().Format(timeWireLayout) {
		clear(credential)
		return node.AgencyAttachment{}, invalidControlResponse("Agency attachment expiry is invalid")
	}
	attachment := node.AgencyAttachment{ID: response.Attachment, Credential: credential, ExpiresAt: expiresAt}
	if node.ValidateAgencyAttachment(attachment) != nil {
		clear(credential)
		return node.AgencyAttachment{}, invalidControlResponse("Agency attachment response is invalid")
	}
	return attachment, nil
}

func (client *AgencyClient) Current(ctx context.Context, attachment node.AgencyAttachment,
	operation string,
) ([]byte, *APIError) {
	headers, apiErr := agencyAuthorityHeaders(attachment, operation, "")
	if apiErr != nil {
		return nil, apiErr
	}
	response, apiErr := client.postProjection(ctx, RouteAgencyCurrent, struct{}{}, headers,
		node.MaxAgencyViewCanonicalBytes)
	if apiErr != nil {
		return nil, apiErr
	}
	if apiErr := validateAgencyProjection(response, node.AgencyViewSchema, node.AgencyViewVersion,
		node.MaxAgencyViewCanonicalBytes); apiErr != nil {
		return nil, apiErr
	}
	return append([]byte(nil), response...), nil
}

func (client *AgencyClient) Submit(ctx context.Context, attachment node.AgencyAttachment,
	currentOperation, operation string, intent []byte,
	candidates []node.AgencyCandidateBinding,
) ([]byte, *APIError) {
	if _, err := node.NewAgencySubmission(operation, intent, candidates); err != nil {
		return nil, NewAPIError(CodeInvalidArgument, "Intent or candidate binding is invalid")
	}
	headers, apiErr := agencyAuthorityHeaders(attachment, currentOperation, operation)
	if apiErr != nil {
		return nil, apiErr
	}
	wires := make([]agencyCandidateWire, len(candidates))
	for index, candidate := range candidates {
		if candidate.Handle == "" || candidate.Digest == "" {
			return nil, NewAPIError(CodeArtifactInvalid, "candidate binding is invalid")
		}
		wires[index] = agencyCandidateWire{Handle: candidate.Handle, Digest: candidate.Digest}
	}
	request := agencySubmitWire{Intent: json.RawMessage(append([]byte(nil), intent...)), Candidates: wires}
	response, apiErr := client.postProjection(ctx, RouteAgencySubmit, request, headers,
		node.MaxAgencyReceiptCanonicalBytes)
	if apiErr != nil {
		return nil, apiErr
	}
	if apiErr := validateAgencyProjection(response, node.AgencyReceiptSchema,
		node.AgencyReceiptVersion, node.MaxAgencyReceiptCanonicalBytes); apiErr != nil {
		return nil, apiErr
	}
	return append([]byte(nil), response...), nil
}

func (client *AgencyClient) Capture(ctx context.Context, content []byte) (
	node.AgencyArtifactCapture, *APIError,
) {
	if len(content) > node.MaxAgencyArtifactBytes {
		return node.AgencyArtifactCapture{}, NewAPIError(CodeArtifactTooLarge,
			"Artifact exceeds the closed byte bound")
	}
	request := agencyArtifactRequestWire{Content: base64.RawStdEncoding.EncodeToString(content)}
	var response agencyArtifactResponseWire
	if apiErr := client.post(ctx, RouteAgencyArtifacts, request, nil, &response,
		maxAgencyPrivateResponse); apiErr != nil {
		return node.AgencyArtifactCapture{}, apiErr
	}
	if response.Schema != agencyArtifactSchema || response.Version != agencyWireVersion ||
		response.ByteSize != int64(len(content)) {
		return node.AgencyArtifactCapture{}, invalidControlResponse("Agency Artifact response is invalid")
	}
	capture := node.AgencyArtifactCapture{Handle: response.Handle, Digest: response.Digest,
		ByteSize: response.ByteSize}
	if node.ValidateAgencyArtifactCapture(capture) != nil || response.Digest != node.AgencyContentDigest(content) {
		return node.AgencyArtifactCapture{}, invalidControlResponse("Agency Artifact response differs from content")
	}
	return capture, nil
}

func (client *AgencyClient) Status(ctx context.Context) (node.AgencyStatusSnapshot, *APIError) {
	if client == nil {
		return node.AgencyStatusSnapshot{}, invalidControlResponse("local Agency client is unavailable")
	}
	return probeAgencyStatus(ctx, client.transport)
}

// ProbeAgencyStatus lets controller readiness prove that the optional R7
// routes were mounted on the same control.sock as the existing local API.
func (client *Client) ProbeAgencyStatus(ctx context.Context) (node.AgencyStatusSnapshot, *APIError) {
	return probeAgencyStatus(ctx, client)
}

func probeAgencyStatus(ctx context.Context, transport *Client) (node.AgencyStatusSnapshot, *APIError) {
	if transport == nil || transport.http == nil || ctx == nil {
		return node.AgencyStatusSnapshot{}, invalidControlResponse("local Agency client is unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://mnemond"+RouteAgencyStatus, nil)
	if err != nil {
		return node.AgencyStatusSnapshot{}, invalidControlResponse("local Agency request cannot be created")
	}
	var response agencyStatusWire
	if apiErr := transport.send(request, &response, maxAgencyPrivateResponse); apiErr != nil {
		return node.AgencyStatusSnapshot{}, apiErr
	}
	if response.Schema != agencyStatusSchema || response.Version != agencyWireVersion ||
		(response.Status != "ready" && response.Status != "not_ready") {
		return node.AgencyStatusSnapshot{}, invalidControlResponse("Agency status response is invalid")
	}
	return node.AgencyStatusSnapshot{Ready: response.Status == "ready"}, nil
}

var _ node.AgencyStatusProbe = (*Client)(nil)

func (client *AgencyClient) post(ctx context.Context, route string, input any,
	headers http.Header, response any, maximum int64,
) *APIError {
	if client == nil || client.transport == nil || client.transport.http == nil || ctx == nil ||
		!IsAgencyRoute(route) || route == RouteAgencyStatus || response == nil || maximum <= 0 {
		return invalidControlResponse("local Agency client is unavailable")
	}
	request, apiErr := newAgencyPostRequest(ctx, route, input, headers)
	if apiErr != nil {
		return apiErr
	}
	return client.transport.send(request, response, maximum)
}

func (client *AgencyClient) postProjection(ctx context.Context, route string, input any,
	headers http.Header, maximum int,
) ([]byte, *APIError) {
	if client == nil || client.transport == nil || client.transport.http == nil || ctx == nil ||
		(route != RouteAgencyCurrent && route != RouteAgencySubmit) || maximum <= 0 {
		return nil, invalidControlResponse("local Agency client is unavailable")
	}
	request, apiErr := newAgencyPostRequest(ctx, route, input, headers)
	if apiErr != nil {
		return nil, apiErr
	}
	response, err := client.transport.http.Do(request)
	if err != nil {
		return nil, NewAPIError(CodeMnemondUnavailable, "mnemond local control is unavailable")
	}
	return readAgencyProjectionResponse(response, maximum)
}

func newAgencyPostRequest(ctx context.Context, route string, input any,
	headers http.Header,
) (*http.Request, *APIError) {
	body, err := model.CanonicalMarshal(input)
	if err != nil || len(body) == 0 || body[0] != '{' || int64(len(body)) > maxAgencyArtifactRequest {
		return nil, NewAPIError(CodeInvalidArgument, "Agency request cannot be encoded canonically")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://mnemond"+route, bytes.NewReader(body))
	if err != nil {
		return nil, invalidControlResponse("local Agency request cannot be created")
	}
	request.Header.Set("Content-Type", "application/json")
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	return request, nil
}

func readAgencyProjectionResponse(response *http.Response, maximum int) ([]byte, *APIError) {
	if response == nil || response.Body == nil {
		return nil, invalidControlResponse("mnemond returned no Agency projection")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, int64(maximum)+2))
	if err != nil || maximum <= 0 || len(raw) < 3 || len(raw) > maximum+1 || raw[len(raw)-1] != '\n' ||
		response.Header.Get("Content-Type") != "application/json" {
		return nil, invalidControlResponse("Agency projection response exceeds its closed bound")
	}
	object := raw[:len(raw)-1]
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, decodeAgencyRemoteError(raw, response.StatusCode)
	}
	if response.StatusCode != http.StatusOK || !validAgencyCanonicalObject(object, maximum) {
		return nil, invalidControlResponse("mnemond returned an invalid Agency projection")
	}
	return append([]byte(nil), object...), nil
}

func decodeAgencyRemoteError(raw []byte, status int) *APIError {
	canonical, apiErr := canonicalResponseObject(raw)
	if apiErr != nil {
		return apiErr
	}
	var remote APIError
	if err := decodeClosedObject(canonical, &remote); err != nil || validateAPIError(&remote) != nil ||
		httpStatusForError(&remote) != status {
		return invalidControlResponse("mnemond returned an invalid Agency error envelope")
	}
	return &remote
}

func agencyAuthorityHeaders(attachment node.AgencyAttachment, current,
	operation string,
) (http.Header, *APIError) {
	if _, err := node.NewAgencyAuthority(attachment.ID, attachment.Credential, current); err != nil {
		if errors.Is(err, node.ErrAgencyCurrentInput) {
			return nil, NewAPIError(CodeInvalidArgument, "Agency current operation is invalid")
		}
		return nil, NewAPIError(CodeAuthenticationFailed, "Agency attachment authority is unavailable")
	}
	headers := make(http.Header)
	headers.Set(agencyAttachmentHeader, attachment.ID)
	headers.Set(agencyCredentialHeader, base64.RawURLEncoding.EncodeToString(attachment.Credential))
	headers.Set(agencyCurrentOperationHeader, current)
	if operation != "" {
		headers.Set(agencyOperationHeader, operation)
	}
	return headers, nil
}

func validateAgencyProjection(raw []byte, schema string, version, maximum int) *APIError {
	if len(raw) == 0 || len(raw) > maximum || raw[0] != '{' {
		return invalidControlResponse("Agency projection exceeds its closed bound")
	}
	if !validAgencyCanonicalObject(raw, maximum) {
		return invalidControlResponse("Agency projection is not canonical")
	}
	var envelope struct {
		Schema  string `json:"schema"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Schema != schema || envelope.Version != version {
		return invalidControlResponse("Agency projection schema is unsupported")
	}
	return nil
}
