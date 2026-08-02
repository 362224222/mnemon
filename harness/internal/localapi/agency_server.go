package localapi

import (
	"encoding/base64"
	"errors"
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

const (
	RouteAgencyAttachments = "/v1/agency/attachments"
	RouteAgencyCurrent     = "/v1/agency/current"
	RouteAgencySubmit      = "/v1/agency/submit"
	RouteAgencyArtifacts   = "/v1/agency/artifacts"
	RouteAgencyStatus      = "/v1/agency/status"

	agencyAttachmentHeader       = "Mnemon-Agency-Attachment"
	agencyCredentialHeader       = "Mnemon-Agency-Credential"
	agencyCurrentOperationHeader = "Mnemon-Agency-Current-Operation"
	agencyOperationHeader        = "Mnemon-Agency-Operation"

	agencyAttachmentSchema = "mnemon.agency.attachment"
	agencyArtifactSchema   = "mnemon.agency.artifact"
	agencyStatusSchema     = "mnemon.agency.status"
	agencyWireVersion      = 1

	maxAgencyPrivateResponse = 4 << 10
	maxAgencyArtifactRequest = ((node.MaxAgencyArtifactBytes + 2) / 3 * 4) + 256
)

type AgencyServer struct {
	service node.AgencyService
	handler http.Handler
}

// NewAgencyServer builds only the R7 local agency routes. It intentionally
// accepts no R5 Profile authenticator or Teamwork service. Production must
// serve the returned Handler only behind ListenOwnerUnix; attachment proof is
// additional authority for Current and Submit, not a replacement for the
// owner-only transport boundary.
func NewAgencyServer(service node.AgencyService) (*AgencyServer, error) {
	if service == nil {
		return nil, errors.New("local API: Agency service is required")
	}
	server := &AgencyServer{service: service}
	mux := http.NewServeMux()
	mux.HandleFunc(RouteAgencyAttachments, server.handleAttach)
	mux.HandleFunc(RouteAgencyCurrent, server.handleCurrent)
	mux.HandleFunc(RouteAgencySubmit, server.handleSubmit)
	mux.HandleFunc(RouteAgencyArtifacts, server.handleArtifact)
	mux.HandleFunc(RouteAgencyStatus, server.handleStatus)
	server.handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request == nil || !IsAgencyRoute(request.URL.Path) {
			writeErrorStatus(writer, http.StatusNotFound,
				NewAPIError(CodeInvalidArgument, "local agency route does not exist"))
			return
		}
		mux.ServeHTTP(writer, request)
	})
	return server, nil
}

func (server *AgencyServer) Handler() http.Handler {
	if server == nil {
		return nil
	}
	return server.handler
}

func IsAgencyRoute(path string) bool {
	return path == RouteAgencyAttachments || path == RouteAgencyCurrent ||
		path == RouteAgencySubmit || path == RouteAgencyArtifacts || path == RouteAgencyStatus
}

func (server *AgencyServer) handleAttach(writer http.ResponseWriter, request *http.Request) {
	if !prepareAgencyJSON(writer, request, false) || !rejectAgencyHeaders(writer, request, false, false) {
		return
	}
	var input struct{}
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	attachment, err := server.service.AgencyAttach(request.Context())
	if err != nil {
		writeError(writer, classifyAgencyError(err))
		return
	}
	if node.ValidateAgencyAttachment(attachment) != nil {
		writeError(writer, NewAPIError(CodeInternal, "Agency service returned invalid attachment"))
		return
	}
	writeResponse(writer, http.StatusOK, agencyAttachmentWire{Schema: agencyAttachmentSchema,
		Version: agencyWireVersion, Attachment: attachment.ID,
		Credential: base64.RawURLEncoding.EncodeToString(attachment.Credential),
		ExpiresAt:  attachment.ExpiresAt.UTC().Format(timeWireLayout)})
}

func (server *AgencyServer) handleCurrent(writer http.ResponseWriter, request *http.Request) {
	if !prepareAgencyJSON(writer, request, false) || !rejectAgencyHeaders(writer, request, true, false) {
		return
	}
	authorityValue, apiErr := parseAgencyAuthority(request)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	var input struct{}
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	view, err := server.service.AgencyCurrent(request.Context(), authorityValue)
	if err != nil {
		writeError(writer, classifyAgencyError(err))
		return
	}
	writeAgencyCanonical(writer, view.CanonicalJSON(), node.MaxAgencyViewCanonicalBytes)
}

func (server *AgencyServer) handleSubmit(writer http.ResponseWriter, request *http.Request) {
	if !prepareAgencyJSON(writer, request, false) || !rejectAgencyHeaders(writer, request, true, true) {
		return
	}
	authorityValue, apiErr := parseAgencyAuthority(request)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	var input agencySubmitWire
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	submission, apiErr := parseAgencySubmission(request.Header, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	receipt, err := server.service.AgencySubmit(request.Context(), authorityValue, submission)
	if err != nil {
		writeError(writer, classifyAgencyError(err))
		return
	}
	writeAgencyCanonical(writer, receipt.CanonicalJSON(), node.MaxAgencyReceiptCanonicalBytes)
}

func (server *AgencyServer) handleArtifact(writer http.ResponseWriter, request *http.Request) {
	if !prepareAgencyJSON(writer, request, false) || !rejectAgencyHeaders(writer, request, false, false) {
		return
	}
	var input agencyArtifactRequestWire
	if apiErr := decodeRequestBounded(request, &input, maxAgencyArtifactRequest); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	content, apiErr := decodeAgencyArtifactContent(input.Content)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	capture, err := server.service.AgencyCapture(request.Context(), content)
	clear(content)
	if err != nil {
		writeError(writer, classifyAgencyError(err))
		return
	}
	if node.ValidateAgencyArtifactCapture(capture) != nil {
		writeError(writer, NewAPIError(CodeInternal, "Agency service returned invalid Artifact capture"))
		return
	}
	writeResponse(writer, http.StatusOK, agencyArtifactResponseWire{Schema: agencyArtifactSchema,
		Version: agencyWireVersion, Handle: capture.Handle,
		Digest: capture.Digest, ByteSize: capture.ByteSize})
}

func (server *AgencyServer) handleStatus(writer http.ResponseWriter, request *http.Request) {
	if !prepareAgencyJSON(writer, request, true) || !rejectAgencyHeaders(writer, request, false, false) {
		return
	}
	status, err := server.service.AgencyStatus(request.Context())
	if err != nil {
		writeError(writer, classifyAgencyError(err))
		return
	}
	value := "not_ready"
	if status.Ready {
		value = "ready"
	}
	writeResponse(writer, http.StatusOK, agencyStatusWire{Schema: agencyStatusSchema,
		Version: agencyWireVersion, Status: value})
}

func prepareAgencyJSON(writer http.ResponseWriter, request *http.Request, readOnly bool) bool {
	wantMethod := http.MethodPost
	if readOnly {
		wantMethod = http.MethodGet
	}
	if request == nil || request.Method != wantMethod {
		writer.Header().Set("Allow", wantMethod)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return false
	}
	if request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument, "local agency request must not contain a query"))
		return false
	}
	if readOnly {
		if request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
			request.Header.Get("Content-Type") != "" {
			writeError(writer, NewAPIError(CodeInvalidArgument, "status request must not contain content"))
			return false
		}
		return true
	}
	if request.Header.Get("Content-Type") != "application/json" {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Content-Type must be application/json"))
		return false
	}
	return true
}

var _ http.Handler = (*AgencyServer)(nil)

func (server *AgencyServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if server == nil || server.handler == nil {
		writeError(writer, NewAPIError(CodeInternal, "local Agency server is unavailable"))
		return
	}
	server.handler.ServeHTTP(writer, request)
}
