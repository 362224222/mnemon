package node

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type nodeTestControlServer struct {
	authenticator Authenticator
	service       Service
	health        HealthProvider
	status        StatusProvider
	authority     AuthorityProvider
	lifecycle     LifecycleFunc
	mutation      MutationShutdownPreparer
	channels      ChannelService
	shutdownOnce  sync.Once
	handler       http.Handler
}

func (server *nodeTestControlServer) attachRoutes() {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", server.handleHealth)
	mux.HandleFunc("/v1/status", server.handleStatus)
	mux.HandleFunc("/v1/authority", server.handleAuthority)
	mux.HandleFunc("/v1/shutdown", server.handleShutdown)
	mux.HandleFunc("/v1/hook/check", server.handleHookCheck)
	mux.HandleFunc("/v1/agent/current", server.handleAgentCurrent)
	mux.HandleFunc("/v1/teamwork/action", server.handleTeamworkAction)
	mux.HandleFunc("/v1/agent/resolve", server.handleAgentResolve)
	mux.HandleFunc("/v1/channel/create", server.handleChannelCreate)
	mux.HandleFunc("/v1/channel/join", server.handleChannelJoin)
	mux.HandleFunc("/v1/channel/status", server.handleChannelStatus)
	server.handler = mux
}

func (server *nodeTestControlServer) Handler() http.Handler { return server.handler }

func (server *nodeTestControlServer) metadata(ctx context.Context,
	request *http.Request,
) (RequestMetadata, *APIError) {
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "MnemonProfile ") {
		return RequestMetadata{}, NewAPIError(CodeAuthenticationFailed, "authentication failed")
	}
	token, err := decodeTestSecret(strings.TrimPrefix(values[0], "MnemonProfile "))
	if err != nil {
		return RequestMetadata{}, NewAPIError(CodeAuthenticationFailed, "authentication failed")
	}
	profile, err := server.authenticator.AuthenticateProfile(ctx, model.Sum(token))
	if err != nil {
		return RequestMetadata{}, NewAPIError(CodeAuthenticationFailed, "authentication failed")
	}
	return RequestMetadata{Profile: profile}, nil
}

func (server *nodeTestControlServer) handleHealth(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, apiErr := server.metadata(request.Context(), request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	snapshot, apiErr := server.health.Health(request.Context(), metadata)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	response, err := NewHealthResponse(snapshot)
	if err != nil {
		writeTestError(writer, NewAPIError(CodeInternal, "health provider returned invalid state"))
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleStatus(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, apiErr := server.metadata(request.Context(), request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	snapshot, apiErr := server.status.Status(request.Context(), metadata)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	response, err := NewStatusResponse(snapshot)
	if err != nil {
		writeTestError(writer, NewAPIError(CodeInternal, "status provider returned invalid state"))
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleAuthority(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, apiErr := server.metadata(request.Context(), request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	snapshot, apiErr := server.authority.Authority(request.Context(), metadata)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	response, err := NewAuthorityResponse(snapshot)
	if err != nil {
		writeTestError(writer, NewAPIError(CodeInternal, "authority provider returned invalid state"))
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleShutdown(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, apiErr := server.metadata(request.Context(), request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	expected, apiErr := requestAuthorityDigest(request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	if request.Header.Get("Mnemon-Mutation-Shutdown") == "required" {
		server.handleMutationShutdown(writer, request, metadata, expected)
		return
	}
	snapshot, apiErr := server.authority.Authority(request.Context(), metadata)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	server.finishShutdown(writer, snapshot, expected, nil)
}

func (server *nodeTestControlServer) handleMutationShutdown(writer http.ResponseWriter,
	request *http.Request, metadata RequestMetadata, expected model.Digest,
) {
	if server.mutation == nil {
		writeTestError(writer, NewAPIError(CodeInternal,
			"mutation shutdown preparation is unavailable"))
		return
	}
	snapshot, release, apiErr := server.mutation.PrepareMutationShutdown(request.Context(), metadata)
	if apiErr != nil {
		if release != nil {
			release()
		}
		writeTestError(writer, apiErr)
		return
	}
	if release == nil {
		writeTestError(writer, NewAPIError(CodeInternal,
			"mutation shutdown admission release is unavailable"))
		return
	}
	server.finishShutdown(writer, snapshot, expected, release)
}

func (server *nodeTestControlServer) finishShutdown(writer http.ResponseWriter,
	snapshot AuthoritySnapshot, expected model.Digest, release AdmissionReleaseFunc,
) {
	retain := false
	if release != nil {
		defer func() {
			if !retain {
				release()
			}
		}()
	}
	current, err := NewAuthorityResponse(snapshot)
	if err != nil {
		writeTestError(writer, NewAPIError(CodeInternal, "shutdown authority is invalid"))
		return
	}
	digest, err := AuthorityDigest(current)
	if err != nil {
		writeTestError(writer, NewAPIError(CodeInternal, "shutdown authority is invalid"))
		return
	}
	if digest != expected {
		writeTestError(writer, NewAPIError(CodeOperationMismatch,
			"durable authority does not match the shutdown precondition"))
		return
	}
	retain = true
	writeTestJSON(writer, http.StatusOK, newShutdownResponse(digest))
	server.shutdownOnce.Do(server.lifecycle)
}

func (server *nodeTestControlServer) handleHookCheck(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, ok := server.preparePost(writer, request, nil)
	if !ok {
		return
	}
	response, apiErr := server.service.HookCheck(request.Context(), metadata, HookCheckRequest{})
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	response.SchemaVersion = SchemaVersion
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleAgentCurrent(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, ok := server.preparePost(writer, request, nil)
	if !ok {
		return
	}
	response, apiErr := server.service.AgentCurrent(request.Context(), metadata, AgentCurrentRequest{})
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	response.SchemaVersion = SchemaVersion
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleTeamworkAction(writer http.ResponseWriter,
	request *http.Request,
) {
	var input TeamworkActionRequest
	metadata, ok := server.preparePost(writer, request, &input)
	if !ok {
		return
	}
	response, apiErr := server.service.TeamworkAction(request.Context(), metadata, input)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleAgentResolve(writer http.ResponseWriter,
	request *http.Request,
) {
	var input AgentResolveRequest
	metadata, ok := server.preparePost(writer, request, &input)
	if !ok {
		return
	}
	response, apiErr := server.service.AgentResolve(request.Context(), metadata, input)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleChannelCreate(writer http.ResponseWriter,
	request *http.Request,
) {
	var input ChannelCreateRequest
	metadata, ok := server.preparePost(writer, request, &input)
	if !ok {
		return
	}
	response, apiErr := server.channels.ChannelCreate(request.Context(), metadata, input)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleChannelJoin(writer http.ResponseWriter,
	request *http.Request,
) {
	var input ChannelJoinRequest
	metadata, ok := server.preparePost(writer, request, &input)
	if !ok {
		return
	}
	response, apiErr := server.channels.ChannelJoin(request.Context(), metadata, input)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) handleChannelStatus(writer http.ResponseWriter,
	request *http.Request,
) {
	metadata, apiErr := server.metadata(request.Context(), request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	response, apiErr := server.channels.ChannelStatus(request.Context(), metadata)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return
	}
	writeTestJSON(writer, http.StatusOK, response)
}

func (server *nodeTestControlServer) preparePost(writer http.ResponseWriter,
	request *http.Request, target any,
) (RequestMetadata, bool) {
	metadata, apiErr := server.metadata(request.Context(), request)
	if apiErr != nil {
		writeTestError(writer, apiErr)
		return RequestMetadata{}, false
	}
	if target == nil {
		return metadata, true
	}
	if err := json.NewDecoder(request.Body).Decode(target); err != nil {
		writeTestError(writer, NewAPIError(CodeInvalidArgument,
			"request body does not match the closed route schema"))
		return RequestMetadata{}, false
	}
	return metadata, true
}

func requestAuthorityDigest(request *http.Request) (model.Digest, *APIError) {
	values := request.Header.Values("Mnemon-Authority-Digest")
	if len(values) != 1 {
		return model.Digest{}, NewAPIError(CodeInvalidArgument,
			"shutdown authority digest is required exactly once")
	}
	digest, err := model.ParseDigest(values[0])
	if err != nil || digest.IsZero() || digest.String() != values[0] {
		return model.Digest{}, NewAPIError(CodeInvalidArgument,
			"shutdown authority digest is invalid")
	}
	return digest, nil
}

func writeTestError(writer http.ResponseWriter, apiErr *APIError) {
	if apiErr == nil {
		apiErr = NewAPIError(CodeInternal, "internal control error")
	}
	writeTestJSON(writer, http.StatusBadRequest, apiErr)
}

func writeTestJSON(writer http.ResponseWriter, status int, value any) {
	raw, err := model.CanonicalMarshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		raw = []byte(`{"code":"internal","message":"internal control error","operation_id":null,"replayed":false,"retryable":false,"schema_version":1,"status":"error"}`)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write(append(raw, '\n'))
}
