package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

var errManagedDaemonMesh = errors.New("managed mnemond mesh bootstrap is invalid")

type managedDaemonMeshPlan struct {
	pending    meshEndpointPending
	hasPending bool
	final      meshEndpoint
	hasFinal   bool
	bind       peer.MeshHostBindSpec
}

// managedDaemonMeshFreezer is invoked synchronously without a Node-state lock
// or SQLite transaction. It may block, must not re-enter daemon composition,
// and on success must transfer the supplied BoundMeshHost into the returned
// runtime; on failure the caller closes any unconsumed bound Host.
type managedDaemonMeshFreezer func(context.Context, *peer.BoundMeshHost,
	store.ChannelMeshAuthority) (*peer.MeshRuntime, error)

func freezeManagedDaemonMesh(ctx context.Context, bound *peer.BoundMeshHost,
	mesh store.ChannelMeshAuthority,
) (*peer.MeshRuntime, error) {
	return bound.Freeze(ctx, mesh)
}

// openManagedDaemonMesh converges the private endpoint crash state while the
// caller owns inherited ensure.lock. The bound Host is never recreated:
// endpoint publication happens while its listener is held, then Freeze moves
// that same Host into the daemon-owned runtime.
func openManagedDaemonMesh(ctx context.Context, nodeState string,
	authority existingDaemonAuthority, freezer managedDaemonMeshFreezer,
) (*peer.MeshRuntime, error) {
	if ctx == nil {
		return nil, managedDaemonMeshError("validate", errors.New("context is unavailable"))
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return nil, managedDaemonMeshError("validate", err)
	}
	if authority.identity == nil || authority.store == nil || freezer == nil {
		return nil, managedDaemonMeshError("validate",
			errors.New("daemon authority or mesh freezer is unavailable"))
	}
	plan, err := prepareManagedDaemonMesh(ctx, nodeState, authority)
	if err != nil {
		return nil, managedDaemonMeshBoundaryError(ctx, err)
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return nil, managedDaemonMeshError("prepare endpoint", err)
	}
	bound, err := peer.BindMeshHost(ctx, authority.identity.PrivateKey(), plan.bind)
	if err != nil {
		return nil, managedDaemonMeshError("bind Host",
			managedDaemonMeshBoundaryError(ctx, err))
	}
	failBound := func(cause error) (*peer.MeshRuntime, error) {
		return nil, errors.Join(cause, bound.Close())
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return failBound(managedDaemonMeshError("bind Host", err))
	}
	snapshot, err := bound.Endpoint()
	if err != nil {
		return failBound(managedDaemonMeshError("read bound endpoint",
			managedDaemonMeshBoundaryError(ctx, err)))
	}
	final, err := settleManagedDaemonEndpoint(nodeState, plan, snapshot)
	if err != nil {
		return failBound(managedDaemonMeshBoundaryError(ctx, err))
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return failBound(managedDaemonMeshError("settle endpoint", err))
	}
	mesh, err := authority.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return failBound(managedDaemonMeshError("read durable mesh",
			managedDaemonMeshBoundaryError(ctx, err)))
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return failBound(managedDaemonMeshError("read durable mesh", err))
	}
	runtime, err := freezer(ctx, bound, mesh)
	if err != nil {
		cause := managedDaemonMeshError("freeze Host",
			managedDaemonMeshBoundaryError(ctx, err))
		if runtime != nil {
			return nil, errors.Join(cause, runtime.Close(), bound.Close())
		}
		return failBound(cause)
	}
	if runtime == nil {
		return failBound(managedDaemonMeshError("freeze Host",
			errors.New("mesh freezer returned no runtime")))
	}
	if err := verifyManagedDaemonMeshReady(ctx, nodeState, final, snapshot, runtime); err != nil {
		return nil, errors.Join(err, runtime.Close())
	}
	return runtime, nil
}

func prepareManagedDaemonMesh(ctx context.Context, nodeState string,
	authority existingDaemonAuthority,
) (managedDaemonMeshPlan, error) {
	state, err := inspectMeshEndpointState(nodeState, authority.identity.PeerID())
	if err != nil {
		return managedDaemonMeshPlan{}, managedDaemonMeshError("inspect endpoint",
			managedDaemonMeshBoundaryError(ctx, err))
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return managedDaemonMeshPlan{}, managedDaemonMeshError("inspect endpoint", err)
	}
	plan := managedDaemonMeshPlan{}
	switch state.stateKind() {
	case meshEndpointStateAbsent:
		if err := verifyManagedDaemonMeshPristine(ctx, authority); err != nil {
			return managedDaemonMeshPlan{}, err
		}
		return managedDaemonMeshPlan{}, managedDaemonMeshError("inspect endpoint",
			errors.New("endpoint is absent; Provision is the only pending publisher"))
	case meshEndpointStatePending:
		plan.pending, plan.hasPending = state.pendingAuthority()
		if err := verifyManagedDaemonMeshPristine(ctx, authority); err != nil {
			return managedDaemonMeshPlan{}, err
		}
	case meshEndpointStateFinal:
		plan.final, plan.hasFinal = state.finalAuthority()
	case meshEndpointStateFinalWithPending:
		plan.pending, plan.hasPending = state.pendingAuthority()
		plan.final, plan.hasFinal = state.finalAuthority()
	default:
		return managedDaemonMeshPlan{}, managedDaemonMeshError("inspect endpoint",
			errors.New("endpoint state is unknown"))
	}
	listen, advertised := plan.pending.listenAddresses(), plan.pending.advertisedAddresses()
	if plan.hasFinal {
		listen, advertised = plan.final.listenAddresses(), plan.final.advertisedAddresses()
	}
	plan.bind.ListenAddrs, err = parseManagedDaemonMeshAddrs(listen)
	if err == nil {
		plan.bind.AdvertisedAddrs, err = parseManagedDaemonMeshAddrs(advertised)
	}
	if err != nil {
		return managedDaemonMeshPlan{}, managedDaemonMeshError("project endpoint", err)
	}
	return plan, nil
}

func verifyManagedDaemonMeshPristine(ctx context.Context,
	authority existingDaemonAuthority,
) error {
	proof, err := authority.store.ReadMeshPristineAuthority(ctx)
	if err != nil {
		return managedDaemonMeshError("prove pending preimage",
			managedDaemonMeshBoundaryError(ctx, err))
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return managedDaemonMeshError("prove pending preimage", err)
	}
	want := authority.authority
	got := store.LocalAuthority{Node: proof.Node(), Profile: proof.Profile()}
	if !sameProvisionAuthority(got, want) {
		return managedDaemonMeshError("prove pending preimage",
			errors.New("mesh-pristine authority differs from daemon authority"))
	}
	return nil
}

func settleManagedDaemonEndpoint(nodeState string, plan managedDaemonMeshPlan,
	snapshot peer.MeshEndpointSnapshot,
) (meshEndpoint, error) {
	final, err := newMeshEndpoint(meshEndpointSpec{PeerID: snapshot.PeerID(),
		ListenAddrs: snapshot.ListenAddrs(), AdvertisedAddrs: snapshot.AdvertisedAddrs()})
	if err != nil {
		return meshEndpoint{}, managedDaemonMeshError("construct final endpoint", err)
	}
	if plan.hasFinal {
		if !bytes.Equal(plan.final.canonicalJSON(), final.canonicalJSON()) {
			return meshEndpoint{}, managedDaemonMeshError("verify reopened endpoint", errMeshEndpointConflict)
		}
		final = plan.final
	} else {
		created, err := publishMeshEndpointFinal(nodeState, plan.pending, final)
		if err != nil || !created {
			return meshEndpoint{}, managedDaemonMeshError("publish final endpoint",
				errors.Join(err, errors.New("pending did not advance exactly once")))
		}
	}
	if plan.hasPending {
		if err := retireMeshEndpointPending(nodeState, plan.pending, final); err != nil {
			return meshEndpoint{}, managedDaemonMeshError("retire pending endpoint", err)
		}
	}
	if err := verifyManagedDaemonFinalEndpoint(nodeState, final); err != nil {
		return meshEndpoint{}, err
	}
	return final, nil
}

func verifyManagedDaemonMeshReady(ctx context.Context, nodeState string,
	final meshEndpoint, snapshot peer.MeshEndpointSnapshot, runtime *peer.MeshRuntime,
) error {
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return managedDaemonMeshError("verify readiness", err)
	}
	if err := verifyManagedDaemonFinalEndpoint(nodeState, final); err != nil {
		return managedDaemonMeshBoundaryError(ctx, err)
	}
	observed, err := newMeshEndpoint(meshEndpointSpec{PeerID: snapshot.PeerID(),
		ListenAddrs: snapshot.ListenAddrs(), AdvertisedAddrs: snapshot.AdvertisedAddrs()})
	if err != nil || !bytes.Equal(observed.canonicalJSON(), final.canonicalJSON()) {
		return managedDaemonMeshError("verify Host snapshot",
			managedDaemonMeshBoundaryError(ctx, errors.Join(err, errMeshEndpointConflict)))
	}
	advertised, err := runtime.LocalEnrollmentMultiaddrs()
	if err != nil || !equalManagedDaemonStrings(advertised, final.advertisedAddresses()) {
		return managedDaemonMeshError("verify runtime advertisement",
			managedDaemonMeshBoundaryError(ctx, errors.Join(err, errMeshEndpointConflict)))
	}
	if err := managedDaemonMeshContextError(ctx); err != nil {
		return managedDaemonMeshError("verify runtime advertisement", err)
	}
	return nil
}

func verifyManagedDaemonFinalEndpoint(nodeState string, final meshEndpoint) error {
	current, err := inspectMeshEndpointState(nodeState, final.peerIDValue())
	if err != nil {
		return managedDaemonMeshError("reread final endpoint", err)
	}
	got, ok := current.finalAuthority()
	if !ok || current.stateKind() != meshEndpointStateFinal ||
		!bytes.Equal(got.canonicalJSON(), final.canonicalJSON()) {
		return managedDaemonMeshError("reread final endpoint", errMeshEndpointConflict)
	}
	return nil
}

func parseManagedDaemonMeshAddrs(values []string) ([]ma.Multiaddr, error) {
	result := make([]ma.Multiaddr, len(values))
	for index, raw := range values {
		address, err := ma.NewMultiaddr(raw)
		if err != nil || address.String() != raw {
			return nil, errors.Join(err, errors.New("endpoint address is not canonical"))
		}
		result[index] = address
	}
	return result, nil
}

func equalManagedDaemonStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func managedDaemonMeshError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", errManagedDaemonMesh, operation, err)
}

func managedDaemonMeshBoundaryError(ctx context.Context, err error) error {
	return errors.Join(err, managedDaemonMeshContextError(ctx))
}

func managedDaemonMeshContextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is unavailable")
	}
	cause := context.Cause(ctx)
	if cause == nil {
		return nil
	}
	return errors.Join(ctx.Err(), cause)
}
