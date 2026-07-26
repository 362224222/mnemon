package node

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const testControlResponseBytes = (256 << 10) + 1024

type Client struct {
	nodeState string
	socket    string
	token     [testOpaqueSecretBytes]byte
	http      *http.Client
}

func NewClient(nodeState string) (*Client, error) {
	token, err := readTestProfileToken(filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+testProfileTokenSuffix))
	if err != nil {
		return nil, err
	}
	client := &Client{nodeState: nodeState, socket: filepath.Join(nodeState, controlSocketName)}
	copy(client.token[:], token)
	client.http = &http.Client{Transport: &http.Transport{Proxy: nil,
		DisableKeepAlives: true, ForceAttemptHTTP2: false, DialContext: client.dialContext}}
	return client, nil
}

func (client *Client) ProbeHealth(ctx context.Context) (HealthResponse, *APIError) {
	var response HealthResponse
	if apiErr := client.get(ctx, "/v1/health", &response); apiErr != nil {
		return HealthResponse{}, apiErr
	}
	if apiErr := ValidateHealthResponse(response); apiErr != nil {
		return HealthResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) ReadStatus(ctx context.Context) (StatusResponse, *APIError) {
	var response StatusResponse
	if apiErr := client.get(ctx, "/v1/status", &response); apiErr != nil {
		return StatusResponse{}, apiErr
	}
	if apiErr := ValidateStatusResponse(response); apiErr != nil {
		return StatusResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) ReadAuthority(ctx context.Context) (AuthorityResponse, *APIError) {
	var response AuthorityResponse
	if apiErr := client.get(ctx, "/v1/authority", &response); apiErr != nil {
		return AuthorityResponse{}, apiErr
	}
	if _, err := AuthorityDigest(response); err != nil {
		return AuthorityResponse{}, NewAPIError(CodeInternal,
			"authority response has an invalid state")
	}
	return response, nil
}

func (client *Client) HookCheck(ctx context.Context) (HookCheckResponse, *APIError) {
	var response HookCheckResponse
	if apiErr := client.post(ctx, "/v1/hook/check", HookCheckRequest{}, &response, nil); apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) AgentCurrent(ctx context.Context) (AgentCurrentResponse, *APIError) {
	var response AgentCurrentResponse
	if apiErr := client.post(ctx, "/v1/agent/current", AgentCurrentRequest{},
		&response, nil); apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) Shutdown(ctx context.Context,
	expected AuthorityResponse,
) (ShutdownResponse, *APIError) {
	return client.shutdown(ctx, expected, false)
}

func (client *Client) ShutdownForMutation(ctx context.Context,
	expected AuthorityResponse,
) (ShutdownResponse, *APIError) {
	return client.shutdown(ctx, expected, true)
}

func (client *Client) CreateChannel(ctx context.Context,
	request ChannelCreateRequest,
) (ChannelCreateResponse, *APIError) {
	var response ChannelCreateResponse
	digest := testChannelCreateRequestDigest(request)
	secret := model.Sum([]byte("node-test-channel-operation:" + digest.String())).Bytes()
	defer clear(secret)
	headers := map[string]string{"Mnemon-Operation-Key": base64.RawURLEncoding.EncodeToString(secret)}
	if apiErr := client.post(ctx, "/v1/channel/create", request, &response, headers); apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) JoinChannel(ctx context.Context,
	request ChannelJoinRequest,
) (ChannelJoinResponse, *APIError) {
	var response ChannelJoinResponse
	if apiErr := client.post(ctx, "/v1/channel/join", request, &response, nil); apiErr != nil {
		return ChannelJoinResponse{}, apiErr
	}
	return response, nil
}

func testChannelCreateRequestDigest(request ChannelCreateRequest) model.Digest {
	raw, err := model.CanonicalMarshal(struct {
		Kind          string               `json:"kind"`
		Request       ChannelCreateRequest `json:"request"`
		SchemaVersion int                  `json:"schema_version"`
	}{Kind: "create", Request: request, SchemaVersion: SchemaVersion})
	if err != nil {
		panic(err)
	}
	return model.Sum(raw)
}

func (client *Client) ReadChannelStatus(ctx context.Context) (ChannelStatusResponse, *APIError) {
	var response ChannelStatusResponse
	if apiErr := client.get(ctx, "/v1/channel/status", &response); apiErr != nil {
		return ChannelStatusResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) shutdown(ctx context.Context, expected AuthorityResponse,
	mutation bool,
) (ShutdownResponse, *APIError) {
	digest, err := AuthorityDigest(expected)
	if err != nil {
		return ShutdownResponse{}, NewAPIError(CodeInvalidArgument,
			"shutdown authority digest is invalid")
	}
	headers := map[string]string{"Mnemon-Authority-Digest": digest.String()}
	if mutation {
		headers["Mnemon-Mutation-Shutdown"] = "required"
	}
	var response ShutdownResponse
	if apiErr := client.post(ctx, "/v1/shutdown", nil, &response, headers); apiErr != nil {
		return ShutdownResponse{}, apiErr
	}
	return response, nil
}

func (client *Client) get(ctx context.Context, path string, target any) *APIError {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://mnemond"+path, nil)
	if err != nil {
		return NewAPIError(CodeInternal, "local control request cannot be created")
	}
	return client.send(request, target)
}

func (client *Client) post(ctx context.Context, path string, body any, target any,
	headers map[string]string,
) *APIError {
	var reader io.Reader
	if body != nil {
		raw, err := model.CanonicalMarshal(body)
		if err != nil {
			return NewAPIError(CodeInternal, "local control request cannot be encoded")
		}
		reader = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://mnemond"+path, reader)
	if err != nil {
		return NewAPIError(CodeInternal, "local control request cannot be created")
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	return client.send(request, target)
}

func (client *Client) send(request *http.Request, target any) *APIError {
	if client == nil || client.http == nil {
		return NewAPIError(CodeInternal, "local control client is unavailable")
	}
	request.Header.Set("Authorization",
		"MnemonProfile "+base64.RawURLEncoding.EncodeToString(client.token[:]))
	response, err := client.http.Do(request)
	if err != nil {
		return NewAPIError(CodeMnemondUnavailable, "mnemond unavailable")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, testControlResponseBytes))
	if err != nil {
		return NewAPIError(CodeInternal, "local control response cannot be read")
	}
	if response.StatusCode >= 400 {
		var apiErr APIError
		if err := json.Unmarshal(raw, &apiErr); err != nil {
			return NewAPIError(CodeInternal, "local control error response is invalid")
		}
		return &apiErr
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return NewAPIError(CodeInternal, "local control response is invalid")
	}
	return nil
}

func (client *Client) dialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "unix", client.socket)
}

func ListenOwnerUnix(socketPath string) (net.Listener, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return nil, ErrUnsafeClientState
	}
	if _, err := os.Lstat(socketPath); err == nil {
		return nil, ErrUnsafeClientState
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return nil, err
	}
	return &testOwnerUnixListener{UnixListener: listener, path: socketPath}, nil
}

type testOwnerUnixListener struct {
	*net.UnixListener
	path string
	once sync.Once
	err  error
}

func (listener *testOwnerUnixListener) Close() error {
	listener.once.Do(func() {
		listener.err = listener.UnixListener.Close()
		if removeErr := os.Remove(listener.path); removeErr != nil &&
			!errors.Is(removeErr, os.ErrNotExist) {
			listener.err = errors.Join(listener.err, removeErr)
		}
	})
	return listener.err
}

func RemoveStaleOwnerUnix(ctx context.Context, socketPath string) (bool, error) {
	if ctx == nil {
		return false, ErrUnsafeClientState
	}
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return false, ErrUnsafeClientState
	}
	probeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	connection, err := (&net.Dialer{}).DialContext(probeCtx, "unix", socketPath)
	cancel()
	if err == nil {
		_ = connection.Close()
		return false, ErrOwnerUnixActive
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return false, err
	}
	if err := os.Remove(socketPath); err != nil {
		return false, err
	}
	return true, nil
}
