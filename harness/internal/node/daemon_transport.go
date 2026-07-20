package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type daemonArtifactCASFactory func(string) (*artifact.CAS, error)

type daemonMeshTransportFactory func(*peer.MeshRuntime,
	peer.MeshTransportOptions,
) (managedChannelTransport, error)

// managedChannelTransport is the one production transport capability shared
// by the mesh lifecycle and Channel reconciler. Keeping the composition seam
// explicit prevents a daemon from becoming ready with only half of its mesh
// protocol surface installed.
type managedChannelTransport interface {
	managedMeshTransport
	ChannelRuntimeTransport
}

var _ managedChannelTransport = (*peer.MeshTransport)(nil)

func newDaemonArtifactCAS(root string,
	factory daemonArtifactCASFactory,
) (*artifact.CAS, error) {
	if factory == nil {
		factory = artifact.NewCAS
	}
	cas, err := factory(root)
	if err != nil {
		return nil, fmt.Errorf("compose daemon Artifact CAS: %w", err)
	}
	if cas == nil || cas.Root() != root {
		return nil, errors.New("daemon Artifact CAS factory returned a different root")
	}
	return cas, nil
}

func newDaemonMeshTransport(ctx context.Context, runtime *peer.MeshRuntime, st *store.Store,
	identity *Identity, cas *artifact.CAS, clock Clock,
	factory daemonMeshTransportFactory,
) (*ChannelAuthorityCoordinator, managedChannelTransport, error) {
	if ctx == nil || ctx.Err() != nil || runtime == nil || st == nil || identity == nil || cas == nil ||
		isNilNodeInterface(clock) {
		return nil, nil, errors.New(
			"daemon mesh transport requires runtime, Store, identity, CAS and clock")
	}
	coordinator, err := NewChannelAuthorityCoordinator(ctx, st, runtime, identity)
	if err != nil {
		return nil, nil, fmt.Errorf("compose daemon Channel authority: %w", err)
	}
	if factory == nil {
		factory = func(runtime *peer.MeshRuntime,
			options peer.MeshTransportOptions,
		) (managedChannelTransport, error) {
			return peer.NewMeshTransport(runtime, options)
		}
	}
	transport, err := factory(runtime, peer.MeshTransportOptions{
		Enrollment:  peer.ChannelEnrollmentOwnerOptions{Controller: coordinator, Clock: clock},
		Member:      peer.ChannelMemberServiceOptions{Controller: coordinator, Clock: clock},
		EventSource: st, EventClock: clock, ArtifactStore: st, ArtifactCAS: cas,
	})
	if err != nil {
		var closeErr error
		if !isNilNodeInterface(transport) {
			closeErr = transport.Close()
		}
		return nil, nil, errors.Join(fmt.Errorf("compose daemon mesh transport: %w", err),
			closeErr)
	}
	if isNilNodeInterface(transport) {
		return nil, nil, errors.New("daemon mesh transport factory returned no transport")
	}
	return coordinator, transport, nil
}
