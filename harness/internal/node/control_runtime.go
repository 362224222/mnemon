package node

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const RunAttachmentEnv = "MNEMON_HARNESS_RUN_ATTACHMENT"

var ErrControlRuntime = errors.New("mnemond local control runtime")

var defaultControlRuntime ControlRuntime

var defaultProfileCredentials ProfileCredentialAuthority

type ProfileCredentialAuthority interface {
	EnsureProfileCredential(string) (model.Digest, bool, error)
	VerifyProfileCredential(string, model.Digest) error
}

type ControlServer interface {
	Handler() http.Handler
}

type ControlServerFactory interface {
	NewControlServer(Authenticator, Service, HealthProvider, StatusProvider,
		AuthorityProvider, LifecycleFunc, MutationShutdownPreparer) (ControlServer, error)
}

type ControlClientFactory interface {
	NewControlClient(string) (DaemonHealthProbe, error)
}

type ControlSocket interface {
	ListenOwnerUnix(string) (net.Listener, error)
	RemoveStaleOwnerUnix(context.Context, string) (bool, error)
}

type RunAttachmentFilesystemFactory interface {
	NewRunAttachmentFilesystem(string) (RunAttachmentFilesystem, error)
}

// ControlRuntime is the concrete host-local API implementation supplied by
// the composition layer. Node owns the durable and lifecycle contracts; the
// runtime owns Unix socket, credential, HTTP, and attachment projection.
type ControlRuntime interface {
	ProfileCredentialAuthority
	ControlServerFactory
	ControlClientFactory
	ControlSocket
	RunAttachmentFilesystemFactory
}

type RunAttachmentCandidate struct {
	RunID     model.RunID
	TokenHash model.Digest
}

type StagedRunAttachment interface {
	TokenHash() model.Digest
	Publish(model.RunID) (RunAttachment, error)
	Discard() error
}

type RunAttachment interface {
	Path() string
}

type RunAttachmentFilesystem interface {
	ListCandidates() ([]RunAttachmentCandidate, error)
	RemoveReapable(model.RunID, model.Digest) (bool, error)
	CleanupStages(time.Time) (int, error)
	Stage(io.Reader) (StagedRunAttachment, error)
	Remove(RunAttachment) error
}

func requireControlRuntime(runtime ControlRuntime) (ControlRuntime, error) {
	if runtime == nil {
		runtime = defaultControlRuntime
	}
	if runtime == nil {
		return nil, errControlRuntimeUnavailable("runtime")
	}
	return runtime, nil
}

func requireProfileCredentialAuthority(
	authority ProfileCredentialAuthority,
) (ProfileCredentialAuthority, error) {
	if authority == nil {
		authority = defaultProfileCredentials
	}
	if authority == nil {
		return nil, errControlRuntimeUnavailable("Profile credential authority")
	}
	return authority, nil
}

func errControlRuntimeUnavailable(component string) error {
	if component == "" {
		component = "runtime"
	}
	return errors.Join(ErrControlRuntime,
		errors.New("local control "+component+" is unavailable"))
}
