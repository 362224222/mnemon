package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/event/semantic"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type daemonDataPlaneRuntime struct {
	plane                *daemonDataPlane
	eventServer          *peer.EventServer
	artifactServer       *peer.ArtifactServer
	artifactReceiver     artifactReceiverSnapshotter
	maximumArtifactPulls int
}

type artifactReceiverSnapshotter interface {
	Snapshot() peer.ArtifactReceiverSnapshot
}

type daemonDataPlaneWorkerComposition struct {
	workers              []daemonDataPlaneWorkerSpec
	artifactReceiver     artifactReceiverSnapshotter
	maximumArtifactPulls int
}

func openDaemonDataPlane(ctx, lifetime context.Context, st *store.Store, identity *Identity,
	clock Clock, mesh *peer.MeshRuntime, manager *ChannelManager,
) (*daemonDataPlaneRuntime, error) {
	if ctx == nil || ctx.Err() != nil || lifetime == nil || lifetime.Err() != nil || st == nil ||
		identity == nil || identity.PublicationSigner() == nil || clock == nil || mesh == nil ||
		manager == nil || mesh.Host() == nil {
		return nil, errors.New("mnemond data plane authority is unavailable")
	}
	cas, cleanup, err := openDaemonArtifactRuntime(st, clock)
	if err != nil {
		return nil, err
	}
	eventServer, artifactServer, err := openDaemonProtocolServers(lifetime, st, clock, mesh, cas)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*daemonDataPlaneRuntime, error) {
		return nil, errors.Join(cause, artifactServer.Close(), eventServer.Close())
	}
	workers, err := composeDaemonDataPlaneWorkers(st, identity, clock, mesh, manager, cas, cleanup)
	if err != nil {
		return fail(err)
	}
	plane, err := newDaemonDataPlane(workers.workers)
	if err != nil {
		return fail(err)
	}
	return &daemonDataPlaneRuntime{plane: plane, eventServer: eventServer,
		artifactServer: artifactServer, artifactReceiver: workers.artifactReceiver,
		maximumArtifactPulls: workers.maximumArtifactPulls}, nil
}

func openDaemonArtifactRuntime(st *store.Store, clock Clock,
) (*artifact.CAS, *artifactStageCleanupWorker, error) {
	cas, err := artifact.NewCAS(filepath.Join(filepath.Dir(st.Path()), "objects", "sha256"))
	if err != nil {
		return nil, nil, fmt.Errorf("open mnemond Artifact CAS: %w", err)
	}
	cleanup, err := newArtifactStageCleanupWorker(st, cas, clock, artifactStageCleanupOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("compose mnemond Artifact stage cleanup: %w", err)
	}
	return cas, cleanup, nil
}

func openDaemonProtocolServers(lifetime context.Context, st *store.Store, clock Clock,
	mesh *peer.MeshRuntime, cas *artifact.CAS,
) (*peer.EventServer, *peer.ArtifactServer, error) {
	eventServer, err := peer.NewEventServer(lifetime, peer.EventServerOptions{
		Host: mesh.Host(), Source: st, Clock: clock})
	if err != nil {
		return nil, nil, fmt.Errorf("compose mnemond Event server: %w", err)
	}
	artifactSource, err := peer.NewArtifactServerStoreSource(st)
	if err != nil {
		return nil, nil, errors.Join(err, eventServer.Close())
	}
	artifactServer, err := peer.NewArtifactServer(lifetime, peer.ArtifactServerOptions{
		Host: mesh.Host(), Source: artifactSource, CAS: cas})
	if err != nil {
		return nil, nil, errors.Join(fmt.Errorf("compose mnemond Artifact server: %w", err),
			eventServer.Close())
	}
	return eventServer, artifactServer, nil
}

func composeDaemonDataPlaneWorkers(st *store.Store, identity *Identity, clock Clock,
	mesh *peer.MeshRuntime, manager *ChannelManager, cas *artifact.CAS,
	cleanup *artifactStageCleanupWorker,
) (daemonDataPlaneWorkerComposition, error) {
	eventClient, err := peer.NewEventClient(peer.EventClientOptions{Host: mesh.Host()})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	artifactClient, err := peer.NewArtifactClient(peer.ArtifactClientOptions{Host: mesh.Host()})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	memberClient, err := peer.NewChannelMemberClient(peer.ChannelMemberClientOptions{Host: mesh.Host()})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	members, err := NewChannelMemberReconciler(ChannelMemberReconcilerOptions{
		Store: st, Client: memberClient, Controller: manager, Clock: clock})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	manager.members = members
	publisher, err := peer.NewGossipPublicationWorker(peer.GossipPublicationWorkerOptions{
		Store: st, Runtime: mesh, Clock: clock})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	repair, err := peer.NewEventRepair(peer.EventRepairOptions{Store: st, Client: eventClient,
		Reconciler: members, Clock: clock})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	limits := peer.HermeticLimits()
	receiver, err := peer.NewArtifactReceiver(peer.ArtifactReceiverOptions{Store: st,
		Client: artifactClient, CAS: cas, Reconciler: members, Clock: clock, Period: time.Second})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	semanticWorker, err := semantic.NewPeerInboxSemanticWorker(semantic.PeerInboxSemanticWorkerOptions{
		Store: st, Signer: identity.PublicationSigner(), Clock: clock,
		PublicationTrigger: publisher})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	inbox := daemonInboxTrigger{receiver, semanticWorker}
	ingress, err := peer.NewRuntimeIngress(peer.RuntimeIngressOptions{Store: st, Runtime: mesh,
		Clock: clock, RepairTrigger: repair, InboxTrigger: inbox})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	deadlines, err := NewWorkDeadlineWorker(WorkDeadlineWorkerOptions{Store: st,
		Signer: identity.PublicationSigner(), Clock: clock, PublicationTrigger: publisher})
	if err != nil {
		return daemonDataPlaneWorkerComposition{}, err
	}
	workers := []daemonDataPlaneWorkerSpec{
		{name: "artifact-staging-cleanup", worker: cleanup, maxConcurrent: 1},
		{name: "channel-members", worker: members, maxConcurrent: 1},
		{name: "gossip-publication", worker: publisher, maxConcurrent: 1},
		{name: "event-repair", worker: repair,
			maxConcurrent: uint32(limits.ApplicationProtocolStreams)},
		{name: "artifact-receiver", worker: receiver,
			maxConcurrent: uint32(limits.InboxWorkers + limits.NodeArtifactPulls + 1)},
		{name: "peer-inbox-semantic", worker: semanticWorker, maxConcurrent: 1},
		{name: "gossip-ingress", worker: ingress,
			maxConcurrent: uint32(model.MaxChannelsPerNode)},
		{name: "work-deadlines", worker: deadlines, readiness: deadlines, maxConcurrent: 1},
	}
	return daemonDataPlaneWorkerComposition{workers: workers, artifactReceiver: receiver,
		maximumArtifactPulls: limits.NodeArtifactPulls}, nil
}

func (runtime *daemonDataPlaneRuntime) artifactTransferObservation() (StatusArtifactTransferSnapshot, bool) {
	if runtime == nil || runtime.artifactReceiver == nil ||
		runtime.maximumArtifactPulls <= 0 ||
		runtime.maximumArtifactPulls > StatusArtifactTransferPullLimit() {
		return StatusArtifactTransferSnapshot{}, false
	}
	snapshot := runtime.artifactReceiver.Snapshot()
	switch snapshot.State {
	case peer.ArtifactReceiverIdle, peer.ArtifactReceiverRunning,
		peer.ArtifactReceiverStopped, peer.ArtifactReceiverFailed:
	default:
		return StatusArtifactTransferSnapshot{}, false
	}
	if snapshot.InFlightPulls < 0 || snapshot.InFlightPulls > runtime.maximumArtifactPulls ||
		snapshot.State != peer.ArtifactReceiverRunning && snapshot.InFlightPulls != 0 {
		return StatusArtifactTransferSnapshot{}, false
	}
	return StatusArtifactTransferSnapshot{ActivePulls: snapshot.InFlightPulls,
		MaximumPulls: runtime.maximumArtifactPulls}, true
}

func (runtime *daemonDataPlaneRuntime) CloseContext(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("mnemond data-plane shutdown context is unavailable")
	}
	var result error
	if runtime.artifactServer != nil {
		result = errors.Join(result, runtime.artifactServer.CloseContext(ctx))
	}
	if runtime.eventServer != nil {
		result = errors.Join(result, runtime.eventServer.CloseContext(ctx))
	}
	return errors.Join(result,
		gracefulShutdownDeadlineError(ctx, "close mnemond data-plane runtime"))
}

type daemonInboxTrigger []interface{ Trigger() }

func (triggers daemonInboxTrigger) Trigger() {
	for _, trigger := range triggers {
		if trigger != nil {
			trigger.Trigger()
		}
	}
}
