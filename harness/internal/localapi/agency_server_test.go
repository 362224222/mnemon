package localapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type fakeAgencyService struct {
	attachment node.AgencyAttachment
	view       node.AgencyView
	receipt    node.AgencyReceipt
	capture    node.AgencyArtifactCapture
	status     node.AgencyStatusSnapshot
	currentErr error
	submitErr  error
	calls      map[string]int
}

type fakeAgencyControlService struct {
	*fakeService
	*fakeAgencyService
}

func TestControlServerMountsOptionalAgencyCapabilityOnSameHandler(t *testing.T) {
	t.Parallel()
	fixture := newAgencyHTTPFixture(t)
	service := &fakeAgencyControlService{fakeService: &fakeService{},
		fakeAgencyService: fixture.service}
	server, err := NewServer(fixedAuthenticator{}, service)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, RouteAgencyStatus, nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	assertAgencyResponse(t, response, http.StatusOK)
	if fixture.service.calls["status"] != 1 || !IsControlRoute(RouteAgencyStatus) {
		t.Fatalf("Agency status calls/control route = (%d,%v)",
			fixture.service.calls["status"], IsControlRoute(RouteAgencyStatus))
	}

	withoutAgency, err := NewServer(fixedAuthenticator{}, &fakeService{})
	if err != nil {
		t.Fatal(err)
	}
	missing := httptest.NewRecorder()
	withoutAgency.Handler().ServeHTTP(missing,
		httptest.NewRequest(http.MethodGet, RouteAgencyStatus, nil))
	if missing.Code != http.StatusNotFound || missing.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("missing Agency capability = %d %s", missing.Code, missing.Body.String())
	}
}

func (service *fakeAgencyService) called(name string) { service.calls[name]++ }

func (service *fakeAgencyService) AgencyAttach(context.Context) (node.AgencyAttachment, error) {
	service.called("attach")
	return service.attachment, nil
}

func (service *fakeAgencyService) AgencyCurrent(context.Context,
	node.AgencyAuthority,
) (node.AgencyView, error) {
	service.called("current")
	return service.view, service.currentErr
}

func (service *fakeAgencyService) AgencySubmit(context.Context, node.AgencyAuthority,
	node.AgencySubmission,
) (node.AgencyReceipt, error) {
	service.called("submit")
	return service.receipt, service.submitErr
}

func (service *fakeAgencyService) AgencyCapture(_ context.Context,
	content []byte,
) (node.AgencyArtifactCapture, error) {
	service.called("capture")
	if int64(len(content)) != service.capture.ByteSize {
		return node.AgencyArtifactCapture{}, errors.New("wrong capture bytes")
	}
	return service.capture, nil
}

func (service *fakeAgencyService) AgencyStatus(context.Context) (node.AgencyStatusSnapshot, error) {
	service.called("status")
	return service.status, nil
}

func TestAgencyCurrentReturnsOnlyCanonicalViewAndRejectsForeignAuthority(t *testing.T) {
	fixture := newAgencyHTTPFixture(t)
	server := newTestAgencyServer(t, fixture.service)

	request := agencyRequest(t, http.MethodPost, RouteAgencyCurrent, `{}`, fixture.attachment,
		fixture.current, "")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	assertAgencyResponse(t, response, http.StatusOK)
	if got := strings.TrimSuffix(response.Body.String(), "\n"); got != string(fixture.view.CanonicalJSON()) {
		t.Fatalf("current response = %s", got)
	}
	for _, private := range []string{fixture.attachment.ID, fixture.current,
		base64.RawURLEncoding.EncodeToString(fixture.attachment.Credential)} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("current response leaked private authority %q", private)
		}
	}

	for name, mutate := range map[string]func(*http.Request){
		"missing proof": func(request *http.Request) { request.Header.Del(agencyCredentialHeader) },
		"duplicate proof": func(request *http.Request) {
			request.Header.Add(agencyCredentialHeader,
				base64.RawURLEncoding.EncodeToString(fixture.attachment.Credential))
		},
		"R5 metadata": func(request *http.Request) { request.Header.Set(authorizationHeader, "forbidden") },
	} {
		t.Run(name, func(t *testing.T) {
			request := agencyRequest(t, http.MethodPost, RouteAgencyCurrent, `{}`, fixture.attachment,
				fixture.current, "")
			mutate(request)
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized && response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body %s", response.Code, response.Body.String())
			}
			assertNoStore(t, response)
		})
	}
}

func TestAgencySubmitKeepsUnboundInputOutsideDurableReceipt(t *testing.T) {
	fixture := newAgencyHTTPFixture(t)
	server := newTestAgencyServer(t, fixture.service)

	invalid := []string{
		`{"intent":{"kind":"work.request"},"unknown":true}`,
		`{"intent":{"kind":"work.request","payload":"x","consequence":"unknown"}}`,
		`{"intent":{"kind":"work.request","kind":"duplicate","payload":"x","consequence":"handling.create"}}`,
	}
	for _, body := range invalid {
		request := agencyRequest(t, http.MethodPost, RouteAgencySubmit, body, fixture.attachment,
			fixture.current, fixture.operation)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid submit status = %d, body %s", response.Code, response.Body.String())
		}
		assertNoStore(t, response)
	}
	if fixture.service.calls["submit"] != 0 {
		t.Fatal("unparsed or unbound Intent reached AgencyService")
	}

	body, err := json.Marshal(agencySubmitWire{Intent: fixture.intent.CanonicalJSON()})
	if err != nil {
		t.Fatal(err)
	}
	request := agencyRequest(t, http.MethodPost, RouteAgencySubmit, string(body), fixture.attachment,
		fixture.current, fixture.operation)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	assertAgencyResponse(t, response, http.StatusOK)
	if got := strings.TrimSuffix(response.Body.String(), "\n"); got != string(fixture.receipt.CanonicalJSON()) {
		t.Fatalf("submit response = %s", got)
	}
	if fixture.service.calls["submit"] != 1 {
		t.Fatalf("submit calls = %d", fixture.service.calls["submit"])
	}

	fixture.service.submitErr = authority.ErrOperationConflict
	response = httptest.NewRecorder()
	server.ServeHTTP(response, agencyRequest(t, http.MethodPost, RouteAgencySubmit,
		string(body), fixture.attachment, fixture.current, fixture.operation))
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "operation_mismatch") {
		t.Fatalf("operation conflict = %d %s", response.Code, response.Body.String())
	}
}

func TestAgencyArtifactCaptureIsBoundedCanonicalMachineIO(t *testing.T) {
	fixture := newAgencyHTTPFixture(t)
	server := newTestAgencyServer(t, fixture.service)
	content := []byte("bounded artifact")
	fixture.service.capture = node.AgencyArtifactCapture{Handle: "artifact:candidate",
		Digest: node.AgencyContentDigest(content), ByteSize: int64(len(content))}
	body, _ := json.Marshal(agencyArtifactRequestWire{Content: base64.RawStdEncoding.EncodeToString(content)})
	request := httptest.NewRequest(http.MethodPost, RouteAgencyArtifacts, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	assertAgencyResponse(t, response, http.StatusOK)
	if fixture.service.calls["capture"] != 1 || !strings.Contains(response.Body.String(), "artifact:candidate") {
		t.Fatalf("capture response = %s", response.Body.String())
	}

	for name, value := range map[string]string{
		"padding":  base64.StdEncoding.EncodeToString(content),
		"invalid":  "not+raw/base64",
		"oversize": strings.Repeat("A", base64.RawStdEncoding.EncodedLen(node.MaxAgencyArtifactBytes)+1),
	} {
		t.Run(name, func(t *testing.T) {
			body, _ := json.Marshal(agencyArtifactRequestWire{Content: value})
			request := httptest.NewRequest(http.MethodPost, RouteAgencyArtifacts, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body %s", response.Code, response.Body.String())
			}
			assertNoStore(t, response)
		})
	}
	if fixture.service.calls["capture"] != 1 {
		t.Fatalf("invalid Artifact reached service, calls = %d", fixture.service.calls["capture"])
	}
}

func TestAgencyAttachAndStatusAreClosedOwnerOnlyShapes(t *testing.T) {
	fixture := newAgencyHTTPFixture(t)
	server := newTestAgencyServer(t, fixture.service)

	attach := httptest.NewRequest(http.MethodPost, RouteAgencyAttachments, strings.NewReader(`{}`))
	attach.Header.Set("Content-Type", "application/json")
	attachResponse := httptest.NewRecorder()
	server.ServeHTTP(attachResponse, attach)
	assertAgencyResponse(t, attachResponse, http.StatusOK)
	if !strings.Contains(attachResponse.Body.String(), fixture.attachment.ID) {
		t.Fatalf("attachment response = %s", attachResponse.Body.String())
	}

	unknown := httptest.NewRequest(http.MethodPost, RouteAgencyAttachments, strings.NewReader(`{"extra":1}`))
	unknown.Header.Set("Content-Type", "application/json")
	unknownResponse := httptest.NewRecorder()
	server.ServeHTTP(unknownResponse, unknown)
	if unknownResponse.Code != http.StatusBadRequest || fixture.service.calls["attach"] != 1 {
		t.Fatalf("unknown attachment body = %d %s", unknownResponse.Code, unknownResponse.Body.String())
	}

	status := httptest.NewRequest(http.MethodGet, RouteAgencyStatus, nil)
	statusResponse := httptest.NewRecorder()
	server.ServeHTTP(statusResponse, status)
	assertAgencyResponse(t, statusResponse, http.StatusOK)
	if got := statusResponse.Body.String(); !strings.Contains(got, `"status":"ready"`) ||
		strings.Contains(got, "channel") || strings.Contains(got, "profile") {
		t.Fatalf("status response = %s", got)
	}

	queried := httptest.NewRequest(http.MethodGet, RouteAgencyStatus+"?verbose=true", nil)
	queriedResponse := httptest.NewRecorder()
	server.ServeHTTP(queriedResponse, queried)
	if queriedResponse.Code != http.StatusBadRequest {
		t.Fatalf("queried status = %d", queriedResponse.Code)
	}
}

type agencyHTTPFixture struct {
	service    *fakeAgencyService
	attachment node.AgencyAttachment
	view       node.AgencyView
	intent     agency.AgentIntent
	receipt    node.AgencyReceipt
	current    string
	operation  string
}

func newAgencyHTTPFixture(t *testing.T) agencyHTTPFixture {
	t.Helper()
	now := time.Date(2026, 8, 3, 8, 9, 10, 11, time.UTC)
	principal := mustAgencyPrincipal(t, "agent:http")
	attachmentID := mustAgencyAttachmentID(t, "attachment:http")
	credential := bytes.Repeat([]byte{0x42}, opaqueSecretBytes)
	attachment := node.AgencyAttachment{ID: attachmentID.String(), Credential: credential,
		ExpiresAt: now.Add(time.Hour)}
	machineAttachment, err := agency.NewAttachment(attachmentID, principal, true, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	target, err := agency.ResolveLocalTarget(agency.SelfTarget(), principal)
	if err != nil {
		t.Fatal(err)
	}
	viewAuthority, err := agency.NewViewAuthority(agency.MachineViewSpec{Attachment: machineAttachment,
		Consequences: []agency.Consequence{agency.ConsequenceCreateHandlings},
		Targets:      []agency.ResolvedTarget{target}})
	if err != nil {
		t.Fatal(err)
	}
	view, err := agency.NewAgentView(agency.AgentViewSpec{Handle: mustAgencyHandle(t, "view:http"),
		Authority: viewAuthority})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := agency.NewAgentIntent(agency.IntentSpec{Kind: mustAgencyLabel(t, "work.request"),
		Payload:     mustAgencyPayload(t, "Review the bounded request."),
		Consequence: agency.ConsequenceCreateHandlings, Successors: []agency.TargetRef{agency.SelfTarget()}})
	if err != nil {
		t.Fatal(err)
	}
	operation := mustAgencyOperation(t, "operation:http-submit")
	bound, err := agency.BindIntent(agency.BoundIntentSpec{Intent: intent, OperationKey: operation,
		View: viewAuthority})
	if err != nil {
		t.Fatal(err)
	}
	event, err := agency.NewEvent(bound, agency.EventStamp{ID: mustAgencyEventID(t, "event:http"),
		AcceptedAt: now, OriginSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	privateReceipt, err := agency.NewAcceptedReceipt(bound, event, now)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := agency.ProjectAgentReceipt(privateReceipt, false)
	if err != nil {
		t.Fatal(err)
	}
	transportView, err := node.ProjectAgencyView(view)
	if err != nil {
		t.Fatal(err)
	}
	transportReceipt, err := node.ProjectAgencyReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	service := &fakeAgencyService{attachment: attachment, view: transportView, receipt: transportReceipt,
		status: node.AgencyStatusSnapshot{Ready: true}, calls: make(map[string]int)}
	return agencyHTTPFixture{service: service, attachment: attachment, view: transportView, intent: intent,
		receipt: transportReceipt, current: "operation:http-current", operation: operation.String()}
}

func newTestAgencyServer(t *testing.T, service node.AgencyService) *AgencyServer {
	t.Helper()
	server, err := NewAgencyServer(service)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func agencyRequest(t *testing.T, method, route, body string, attachment node.AgencyAttachment,
	current, operation string,
) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, route, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	headers, apiErr := agencyAuthorityHeaders(attachment, current, operation)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	return request
}

func assertAgencyResponse(t *testing.T, response *httptest.ResponseRecorder, status int) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, body %s", response.Code, response.Body.String())
	}
	assertNoStore(t, response)
	if response.Header().Get("Content-Type") != "application/json" ||
		response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("response headers = %#v", response.Header())
	}
}

func assertNoStore(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", response.Header().Get("Cache-Control"))
	}
}

func mustAgencyPrincipal(t *testing.T, value string) agency.AgentPrincipalID {
	t.Helper()
	result, err := agency.NewAgentPrincipalID(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustAgencyAttachmentID(t *testing.T, value string) agency.AttachmentID {
	t.Helper()
	result, err := agency.NewAttachmentID(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustAgencyHandle(t *testing.T, value string) agency.OpaqueHandle {
	t.Helper()
	result, err := agency.NewOpaqueHandle(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustAgencyLabel(t *testing.T, value string) agency.SemanticLabel {
	t.Helper()
	result, err := agency.NewSemanticLabel(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustAgencyPayload(t *testing.T, value string) agency.SemanticPayload {
	t.Helper()
	result, err := agency.NewSemanticPayload(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustAgencyOperation(t *testing.T, value string) agency.OperationKey {
	t.Helper()
	result, err := agency.NewOperationKey(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustAgencyEventID(t *testing.T, value string) agency.EventID {
	t.Helper()
	result, err := agency.NewEventID(value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
