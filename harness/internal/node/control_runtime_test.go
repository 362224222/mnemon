package node

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	testProfileTokenSuffix = ".token"
	testProfileTokenBytes  = 43 + 1
	testOpaqueSecretBytes  = 32
)

var (
	ErrUnsafeClientState = errors.New("local API: unsafe client state")
	ErrOwnerUnixActive   = errors.New("owner Unix socket is active")
)

func init() {
	defaultControlRuntime = nodeTestControlRuntime{}
	defaultProfileCredentials = nodeTestControlRuntime{}
}

func VerifyProfileCredential(nodeState string, expected model.Digest) error {
	return nodeTestControlRuntime{}.VerifyProfileCredential(nodeState, expected)
}

type nodeTestControlRuntime struct{}

func (nodeTestControlRuntime) EnsureProfileCredential(nodeState string) (model.Digest, bool, error) {
	profiles, err := ensureTestOwnerDirectory(filepath.Join(nodeState, "profiles"))
	if err != nil {
		return model.Digest{}, false, err
	}
	path := filepath.Join(profiles, model.TeamworkProfileID().String()+testProfileTokenSuffix)
	token, err := readTestProfileToken(path)
	if err == nil {
		return model.Sum(token), false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return model.Digest{}, false, err
	}
	token = make([]byte, testOpaqueSecretBytes)
	if _, err := rand.Read(token); err != nil {
		return model.Digest{}, false, err
	}
	raw := append([]byte(base64.RawURLEncoding.EncodeToString(token)), '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return model.Digest{}, false, err
	}
	return model.Sum(token), true, nil
}

func (nodeTestControlRuntime) VerifyProfileCredential(nodeState string,
	expected model.Digest,
) error {
	if expected.IsZero() {
		return ErrUnsafeClientState
	}
	token, err := readTestProfileToken(filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+testProfileTokenSuffix))
	if err != nil {
		return err
	}
	actual := model.Sum(token)
	wantBytes := expected.Bytes()
	actualBytes := actual.Bytes()
	matched := subtle.ConstantTimeCompare(wantBytes, actualBytes) == 1
	clear(wantBytes)
	clear(actualBytes)
	if !matched {
		return ErrUnsafeClientState
	}
	return nil
}

func (nodeTestControlRuntime) NewControlServer(authenticator Authenticator, service Service,
	health HealthProvider, status StatusProvider, authority AuthorityProvider,
	lifecycle LifecycleFunc, mutation MutationShutdownPreparer,
) (ControlServer, error) {
	if authenticator == nil || service == nil {
		return nil, errors.New("test control server requires authenticator and service")
	}
	server := &nodeTestControlServer{authenticator: authenticator, service: service,
		health: health, status: status, authority: authority, lifecycle: lifecycle,
		mutation: mutation}
	if agency, ok := service.(AgencyService); ok {
		server.agency = agency
	}
	if channels, ok := service.(ChannelService); ok {
		server.channels = channels
	}
	server.attachRoutes()
	return server, nil
}

func (nodeTestControlRuntime) NewControlClient(nodeState string) (DaemonHealthProbe, error) {
	return NewClient(nodeState)
}

func (nodeTestControlRuntime) ListenOwnerUnix(socketPath string) (net.Listener, error) {
	return ListenOwnerUnix(socketPath)
}

func (nodeTestControlRuntime) RemoveStaleOwnerUnix(ctx context.Context,
	socketPath string,
) (bool, error) {
	return RemoveStaleOwnerUnix(ctx, socketPath)
}

func (nodeTestControlRuntime) NewRunAttachmentFilesystem(nodeState string) (RunAttachmentFilesystem, error) {
	return testRunAttachmentFilesystem{nodeState: nodeState}, nil
}

func readTestProfileToken(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err != nil {
		return nil, ErrUnsafeClientState
	}
	if len(raw) != testProfileTokenBytes || raw[len(raw)-1] != '\n' {
		return nil, ErrUnsafeClientState
	}
	token, err := decodeTestSecret(string(raw[:len(raw)-1]))
	if err != nil {
		return nil, ErrUnsafeClientState
	}
	return token, nil
}

func decodeTestSecret(value string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != testOpaqueSecretBytes ||
		base64.RawURLEncoding.EncodeToString(decoded) != value {
		return nil, ErrUnsafeClientState
	}
	return decoded, nil
}

func ensureTestOwnerDirectory(path string) (string, error) {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return "", err
	}
	return path, nil
}

var _ ControlRuntime = nodeTestControlRuntime{}
