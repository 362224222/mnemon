package localapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func rejectAgencyHeaders(writer http.ResponseWriter, request *http.Request,
	requireAttachment, requireOperation bool,
) bool {
	present := func(name string) bool { return len(request.Header.Values(name)) != 0 }
	if present(authorizationHeader) || present(claimContextHeader) || present(runAttachmentHeader) ||
		present(operationKeyHeader) {
		writeError(writer, NewAPIError(CodeInvalidArgument, "R5 control metadata is not allowed"))
		return false
	}
	unexpectedAttachment := !requireAttachment && (present(agencyAttachmentHeader) ||
		present(agencyCredentialHeader) || present(agencyCurrentOperationHeader))
	unexpectedOperation := !requireOperation && present(agencyOperationHeader)
	if unexpectedAttachment || unexpectedOperation {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Agency authority is not allowed on this route"))
		return false
	}
	return true
}

func parseAgencyAuthority(request *http.Request) (node.AgencyAuthority, *APIError) {
	attachmentValue, attachmentErr := singleHeader(request.Header, agencyAttachmentHeader)
	credentialValue, credentialErr := singleHeader(request.Header, agencyCredentialHeader)
	if attachmentErr != nil || credentialErr != nil {
		return node.AgencyAuthority{},
			NewAPIError(CodeAuthenticationFailed, "Agency attachment proof is required")
	}
	credential, decodeErr := decodeOpaqueSecret(credentialValue)
	defer clear(credential)
	if decodeErr != nil {
		return node.AgencyAuthority{},
			NewAPIError(CodeAuthenticationFailed, "Agency attachment proof is invalid")
	}
	currentValue, currentErr := singleHeader(request.Header, agencyCurrentOperationHeader)
	if currentErr != nil {
		return node.AgencyAuthority{},
			NewAPIError(CodeInvalidArgument, "Agency current operation is required exactly once")
	}
	authorityValue, err := node.NewAgencyAuthority(attachmentValue, credential, currentValue)
	if err != nil {
		if errors.Is(err, node.ErrAgencyCurrentInput) {
			return node.AgencyAuthority{}, NewAPIError(CodeInvalidArgument,
				"Agency current operation is invalid")
		}
		return node.AgencyAuthority{}, NewAPIError(CodeAuthenticationFailed, "Agency authority is invalid")
	}
	return authorityValue, nil
}

func parseAgencySubmission(header http.Header, wire agencySubmitWire) (node.AgencySubmission, *APIError) {
	operation, err := singleHeader(header, agencyOperationHeader)
	if err != nil {
		return node.AgencySubmission{},
			NewAPIError(CodeInvalidArgument, "Agency operation is required exactly once")
	}
	if len(wire.Candidates) > node.MaxAgencyArtifactInputs {
		return node.AgencySubmission{}, NewAPIError(CodeArtifactTooLarge,
			"candidate count exceeds the closed bound")
	}
	bindings := make([]node.AgencyCandidateBinding, len(wire.Candidates))
	for index, candidate := range wire.Candidates {
		bindings[index] = node.AgencyCandidateBinding{Handle: candidate.Handle, Digest: candidate.Digest}
	}
	submission, parseErr := node.NewAgencySubmission(operation, wire.Intent, bindings)
	if parseErr != nil {
		return node.AgencySubmission{}, NewAPIError(CodeInvalidArgument,
			"Intent or candidate binding is invalid")
	}
	return submission, nil
}

func decodeAgencyArtifactContent(value string) ([]byte, *APIError) {
	if strings.TrimSpace(value) != value || strings.ContainsAny(value, "=\t\r\n ") {
		return nil, NewAPIError(CodeArtifactInvalid, "Artifact content must be canonical raw base64")
	}
	if len(value) > base64.RawStdEncoding.EncodedLen(node.MaxAgencyArtifactBytes) {
		return nil, NewAPIError(CodeArtifactTooLarge, "Artifact exceeds the closed byte bound")
	}
	content, err := base64.RawStdEncoding.Strict().DecodeString(value)
	if err != nil {
		clear(content)
		return nil, NewAPIError(CodeArtifactInvalid, "Artifact content is invalid")
	}
	if len(content) > node.MaxAgencyArtifactBytes {
		clear(content)
		return nil, NewAPIError(CodeArtifactTooLarge, "Artifact exceeds the closed byte bound")
	}
	return content, nil
}

func decodeRequestBounded(request *http.Request, target any, maximum int64) *APIError {
	if request == nil || request.Body == nil || maximum <= 0 {
		return NewAPIError(CodeInvalidArgument, "JSON object body is required")
	}
	if request.ContentLength > maximum {
		return NewAPIError(CodeInvalidArgument, "request body exceeds the local control limit")
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, maximum+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > maximum {
		return NewAPIError(CodeInvalidArgument, "request body exceeds the local control limit")
	}
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil || len(canonical) == 0 || canonical[0] != '{' {
		return NewAPIError(CodeInvalidArgument, "request body must be one valid JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return NewAPIError(CodeInvalidArgument, "request body does not match the closed route schema")
	}
	if token, err := decoder.Token(); err == nil || token != nil {
		return NewAPIError(CodeInvalidArgument, "request body contains a trailing value")
	}
	return nil
}

func writeAgencyCanonical(writer http.ResponseWriter, raw []byte, maximum int) {
	if !validAgencyCanonicalObject(raw, maximum) {
		writeError(writer, NewAPIError(CodeInternal, "Agency service returned invalid canonical JSON"))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(append(append([]byte(nil), raw...), '\n'))
}

func validAgencyCanonicalObject(raw []byte, maximum int) bool {
	if len(raw) == 0 || len(raw) > maximum || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return false
	}
	// Agency values deliberately use their own frozen field order rather than
	// the legacy model.JSON key order. CanonicalizeJSON is still used as a
	// duplicate-key and integer-only validator; json.Compact proves the Agency
	// value contains no alternate whitespace encoding without reordering it.
	if _, err := model.CanonicalizeJSON(raw); err != nil {
		return false
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return false
	}
	return bytes.Equal(compact.Bytes(), raw)
}

func classifyAgencyError(err error) *APIError {
	switch node.ClassifyAgencyError(err) {
	case node.AgencyErrorAuthentication:
		return NewAPIError(CodeAuthenticationFailed, "Agency attachment authentication failed")
	case node.AgencyErrorContextStale:
		return NewAPIError(CodeContextStale, "Agency context is stale")
	case node.AgencyErrorOperationConflict:
		return NewAPIError(CodeOperationMismatch, "Agency operation conflicts with its prior request")
	case node.AgencyErrorArtifact:
		return NewAPIError(CodeArtifactInvalid, "Artifact is unavailable")
	case node.AgencyErrorLimit:
		return NewAPIError(CodeInvalidArgument, "Agency input exceeds a closed bound")
	case node.AgencyErrorInvalid:
		return NewAPIError(CodeInvalidArgument, "Agency input is invalid")
	case node.AgencyErrorActionNotAllowed:
		return NewAPIError(CodeActionNotAllowed, "Intent is not allowed by the current View")
	case node.AgencyErrorUnavailable:
		return NewAPIError(CodeMnemondUnavailable, "local Agency authority is unavailable")
	default:
		return NewAPIError(CodeInternal, "internal Agency error")
	}
}

type agencyAttachmentWire struct {
	Schema     string `json:"schema"`
	Version    int    `json:"version"`
	Attachment string `json:"attachment"`
	Credential string `json:"credential"`
	ExpiresAt  string `json:"expires_at"`
}

type agencyCandidateWire struct {
	Handle string `json:"handle"`
	Digest string `json:"digest"`
}

type agencySubmitWire struct {
	Intent     json.RawMessage       `json:"intent"`
	Candidates []agencyCandidateWire `json:"candidates,omitempty"`
}

type agencyArtifactRequestWire struct {
	Content string `json:"content_base64"`
}

type agencyArtifactResponseWire struct {
	Schema   string `json:"schema"`
	Version  int    `json:"version"`
	Handle   string `json:"handle"`
	Digest   string `json:"digest"`
	ByteSize int64  `json:"byte_size"`
}

type agencyStatusWire struct {
	Schema  string `json:"schema"`
	Version int    `json:"version"`
	Status  string `json:"status"`
}

const timeWireLayout = "2006-01-02T15:04:05.000000000Z"
