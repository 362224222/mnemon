package nodecontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

// ProfileCredentials is the stateless adapter from Node's digest-only port to
// the owner-only local control credential projection.
type ProfileCredentials struct{}

func (ProfileCredentials) Ensure(nodeState string) (model.Digest, bool, error) {
	return localapi.EnsureProfileCredential(nodeState)
}

func (ProfileCredentials) Verify(nodeState string, expected model.Digest) error {
	return localapi.VerifyProfileCredential(nodeState, expected)
}

func RecoverControlSocket(ctx context.Context, path string) (bool, error) {
	return localapi.RemoveStaleOwnerUnix(ctx, path)
}

type HealthClient interface {
	ProbeHealth(context.Context) (localapi.HealthResponse, *localapi.APIError)
}

type healthProbe struct{ client HealthClient }

func AdaptHealthClient(client HealthClient) node.DaemonHealthProbe {
	return healthProbe{client: client}
}

func (probe healthProbe) ProbeDaemonHealth(ctx context.Context) (node.DaemonHealth, error) {
	if nilInterface(probe.client) {
		return node.DaemonHealth{}, errors.New("local health client is unavailable")
	}
	response, apiErr := probe.client.ProbeHealth(ctx)
	if apiErr != nil {
		if !canonicalUnscopedAPIError(apiErr) {
			return node.DaemonHealth{}, errors.New("local health error is not canonical")
		}
		if apiErr.Code == localapi.CodeMnemondUnavailable {
			return node.DaemonHealth{}, errors.Join(node.ErrDaemonControlUnavailable, apiErr)
		}
		return node.DaemonHealth{}, apiErr
	}
	if response.SchemaVersion != localapi.SchemaVersion ||
		(response.Status != "ready" && response.Status != "not_ready") {
		return node.DaemonHealth{}, errors.New("local health response is not canonical")
	}
	if _, err := model.ParseDigest(response.AssetRevision); err != nil {
		return node.DaemonHealth{}, errors.New("local health revision is invalid")
	}
	return node.DaemonHealth{AssetRevision: response.AssetRevision,
		Ready: response.Status == "ready"}, nil
}

type MutationShutdownClient interface {
	ShutdownForMutation(context.Context, localapi.AuthorityResponse) (
		localapi.ShutdownResponse, *localapi.APIError,
	)
}

type lifecycleClient struct{ client MutationShutdownClient }

func AdaptLifecycleClient(client MutationShutdownClient) node.DaemonLifecycleClient {
	return lifecycleClient{client: client}
}

func (client lifecycleClient) ShutdownDaemonForMutation(ctx context.Context,
	expected node.Authority,
) error {
	if nilInterface(client.client) {
		return errors.New("local lifecycle client is unavailable")
	}
	wire, err := AuthorityResponse(expected)
	if err != nil {
		return err
	}
	response, apiErr := client.client.ShutdownForMutation(ctx, wire)
	if apiErr != nil {
		if !canonicalUnscopedAPIError(apiErr) {
			return errors.New("local lifecycle error is not canonical")
		}
		if apiErr.Code == localapi.CodeMnemondUnavailable {
			return errors.Join(node.ErrDaemonControlUnavailable, apiErr)
		}
		return apiErr
	}
	expectedDigest, err := localapi.AuthorityDigest(wire)
	actualDigest, parseErr := model.ParseDigest(response.AuthorityDigest)
	if err != nil || parseErr != nil || response.SchemaVersion != localapi.SchemaVersion ||
		response.Status != "stopping" || actualDigest != expectedDigest {
		return fmt.Errorf("local shutdown response is not canonical")
	}
	return nil
}

func canonicalUnscopedAPIError(apiErr *localapi.APIError) bool {
	return apiErr != nil && apiErr.Validate() == nil && !apiErr.Replayed && apiErr.OperationID == nil
}

// AuthorityResponse closes a transport-neutral Node authority into the exact
// local HTTP/companion response schema.
func AuthorityResponse(authority node.Authority) (localapi.AuthorityResponse, error) {
	if err := authority.Validate(); err != nil {
		return localapi.AuthorityResponse{}, err
	}
	return localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{
		Host: authority.Host, Runtime: authority.Runtime, Enabled: authority.Enabled,
		AssetRevision: authority.AssetRevision, UpdatedAt: authority.UpdatedAt,
		PeerID: authority.PeerID, ActiveAssetRevision: authority.ActiveAssetRevision,
	})
}

// Authority parses and re-closes an already bounded local response before it
// enters Node lifecycle policy.
func Authority(response localapi.AuthorityResponse) (node.Authority, error) {
	if _, err := localapi.AuthorityDigest(response); err != nil {
		return node.Authority{}, err
	}
	peerID, peerErr := model.ParsePeerID(response.PeerID)
	updatedAt, timeErr := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	if peerErr != nil || timeErr != nil {
		return node.Authority{}, errors.New("local authority response is invalid")
	}
	authority := node.Authority{Host: model.HostKind(response.Host), Runtime: model.RuntimeKind(response.Runtime),
		Enabled: response.Enabled, AssetRevision: response.AssetRevision, UpdatedAt: updatedAt,
		PeerID: peerID, ActiveAssetRevision: response.ActiveAssetRevision}
	if err := authority.Validate(); err != nil {
		return node.Authority{}, err
	}
	closed, err := AuthorityResponse(authority)
	if err != nil || closed != response {
		return node.Authority{}, errors.New("local authority response is not canonical")
	}
	return authority, nil
}

var _ node.ProfileCredentialProvisioner = ProfileCredentials{}
