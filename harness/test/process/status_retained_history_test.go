//go:build darwin || linux

package process_test

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	op01RetainedPublications = uint64(1024)
	op01PublicationPageSize  = uint64(32)
	op01FixturePrepareLimit  = 5 * time.Minute
	op01StatusLatencyLimit   = 5 * time.Second
	op01DoctorLatencyLimit   = 10 * time.Second
	op01ResponseGrowthLimit  = 128
	op01DoctorResponseLimit  = localapi.MaxStatusResponseBytes + 4096
)

type op01PublicObservation struct {
	status        localapi.StatusResponse
	doctor        setupProcessDoctorReport
	statusBytes   int
	doctorBytes   int
	statusLatency time.Duration
	doctorLatency time.Duration
}

func TestPublicStatusAndDoctorRemainBoundedAfter1024RetainedPublications(t *testing.T) {
	fixture := channelProcessSetupFixture(t)
	nodes := fixture.nodes
	created := channelProcessCreate(t, fixture.harnessExecutable, nodes["A"], "History", "history")
	channelProcessJoinWithToken(t, fixture.harnessExecutable, nodes["B"], "history",
		created.InviteToken)
	channelProcessJoinWithToken(t, fixture.harnessExecutable, nodes["C"], "history",
		created.InviteToken)
	peers := []string{nodes["A"].peerID, nodes["B"].peerID, nodes["C"].peerID}
	views := channelProcessWaitReadyViews(t, fixture.harnessExecutable, nodes,
		[]string{"A", "B", "C"}, "A", "history", "History", peers)
	channelProcessAssertSharedAuthority(t, "OP-01 retained history",
		views["A"], views["B"], views["C"])

	before := op01ObservePublicStatusAndDoctor(t, fixture.harnessExecutable, nodes["B"])
	if channel := op01RequireStatusChannel(t, before.status, "history"); channel.Inbox.Durable != 0 {
		t.Fatalf("precondition retained publications = %d, want 0", channel.Inbox.Durable)
	}

	op01StopProcessNode(t, nodes["B"])
	op01RetainIgnoredPublications(t, nodes["A"], nodes["B"], nodes["C"], "history")
	op01RestartProcessNode(t, fixture.harnessExecutable, nodes["B"],
		before.status.AssetRevision)
	channelProcessWaitChannel(t, fixture.harnessExecutable, nodes["B"], "history",
		func(view localapi.ChannelView) error {
			return channelProcessAssertReady(view, nodes["B"].peerID, nodes["A"].peerID,
				"history", "History", peers)
		})

	after := op01ObservePublicStatusAndDoctor(t, fixture.harnessExecutable, nodes["B"])
	channel := op01RequireStatusChannel(t, after.status, "history")
	if channel.Inbox.Durable != op01RetainedPublications ||
		channel.Inbox.Ignored != op01RetainedPublications ||
		channel.Semantic.Ignored != op01RetainedPublications {
		t.Fatalf("public retained-history projection = inbox %#v semantic %#v",
			channel.Inbox, channel.Semantic)
	}
	if after.statusBytes > before.statusBytes+op01ResponseGrowthLimit ||
		after.doctorBytes > before.doctorBytes+op01ResponseGrowthLimit {
		t.Fatalf("retained history expanded public responses: status %d->%d doctor %d->%d",
			before.statusBytes, after.statusBytes, before.doctorBytes, after.doctorBytes)
	}
	t.Logf("OP-01 retained=%d status_bytes=%d/%d status_latency=%s/%s "+
		"doctor_bytes=%d/%d doctor_latency=%s/%s",
		op01RetainedPublications, after.statusBytes, localapi.MaxStatusResponseBytes,
		after.statusLatency, op01StatusLatencyLimit, after.doctorBytes, op01DoctorResponseLimit,
		after.doctorLatency, op01DoctorLatencyLimit)
}

func op01StopProcessNode(t *testing.T, process *channelProcessNode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := setupProcessShutdown(ctx, process.client, process.nodeState, process.offline)
	cancel()
	if err != nil {
		t.Fatalf("stop OP-01 fixture Node: %v", err)
	}
	process.autoMayRun = false
}

func op01RestartProcessNode(t *testing.T, executable string, process *channelProcessNode,
	assetRevision string,
) {
	t.Helper()
	process.autoMayRun = true
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	result := setupProcessRunHarness(ctx, executable, process.workspace, process.environment, "status")
	contextErr := ctx.Err()
	cancel()
	if contextErr != nil || result.overflow || len(result.stdout) == 0 {
		t.Fatalf("restart OP-01 fixture Node: context=%v exit=%v stdout=%s stderr=%s",
			contextErr, result.err, setupProcessFingerprint(result.stdout),
			setupProcessFingerprint(result.stderr))
	}
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := setupProcessWaitReady(readyCtx, process.client, assetRevision)
	readyCancel()
	if err != nil {
		t.Fatalf("restarted OP-01 fixture Node is not ready: %v", err)
	}
}

func op01RetainIgnoredPublications(t *testing.T, origin, target,
	audience *channelProcessNode, alias string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), op01FixturePrepareLimit)
	defer cancel()
	st, err := store.OpenExisting(ctx, filepath.Join(target.nodeState, "node.db"))
	if err != nil {
		t.Fatalf("open offline OP-01 Store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	}()
	observation, err := st.ReadChannelObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	channel := op01ObservationChannel(t, observation, alias)
	originPeer, err := model.ParsePeerID(origin.peerID)
	if err != nil {
		t.Fatal(err)
	}
	audiencePeer, err := model.ParsePeerID(audience.peerID)
	if err != nil {
		t.Fatal(err)
	}
	originMember := op01RosterMember(t, channel, originPeer)
	identity, err := node.LoadIdentity(origin.nodeState)
	if err != nil {
		t.Fatalf("load retained-publication signer: %v", err)
	}
	if identity.PeerID() != originPeer {
		t.Fatal("retained-publication signer Peer differs from the origin")
	}
	at := time.Now().Round(0).UTC()
	repair := op01RepairTarget(t, ctx, st, channel.Channel().ID(), originPeer, at)
	op01PutPublicationPages(t, ctx, st, identity, channel, originMember,
		observation.LocalPeerID(), audiencePeer, at)
	committed, err := st.CommitPeerRepair(ctx, store.CommitPeerRepairSpec{
		Target: repair, Status: store.PeerRepairCaughtUp,
		ContiguousChannelSequence: op01RetainedPublications,
		SourceFloor:               1,
		SourceHead:                op01RetainedPublications,
		NextAttemptAt:             at.Add(24 * time.Hour),
		At:                        at,
	})
	if err != nil || !committed.Changed ||
		committed.Target.ContiguousChannelSequence() != op01RetainedPublications {
		t.Fatalf("commit retained-publication repair = (%#v, %v)", committed, err)
	}
	after, err := st.ReadChannelObservation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	progress := op01ObservationChannel(t, after, alias).Progress().Inbox()
	if progress.Durable != op01RetainedPublications ||
		progress.Ignored != op01RetainedPublications {
		t.Fatalf("offline retained-publication oracle = %#v", progress)
	}
}

func op01PutPublicationPages(t *testing.T, ctx context.Context, st *store.Store,
	identity *node.Identity, channel store.ChannelObservationChannel, origin model.Member,
	home, audience model.PeerID, at time.Time,
) {
	t.Helper()
	for after := uint64(0); after < op01RetainedPublications; after += op01PublicationPageSize {
		scanned := after + op01PublicationPageSize
		publications := make([]model.PublicationEvidence, 0, op01PublicationPageSize)
		for sequence := after + 1; sequence <= scanned; sequence++ {
			publications = append(publications,
				op01PublicationEvidence(t, ctx, identity, channel, origin,
					home, audience, sequence, at))
		}
		result, err := st.PutPeerInboxPage(ctx, store.PutPeerInboxPageSpec{
			ChannelID: channel.Channel().ID(), OriginPeerID: origin.PeerID(),
			OriginEpoch: origin.OriginEpoch(), TransportPeerID: origin.PeerID(),
			AfterChannelSequence: after, ScannedChannelSeq: scanned,
			SourceFloor: 1, SourceHead: op01RetainedPublications,
			Publications: publications, ReceivedAt: at,
		})
		if err != nil || result.Quarantined || len(result.Items) != int(op01PublicationPageSize) ||
			result.Cursor.ContiguousChannelSequence != scanned {
			t.Fatalf("retain publication page ending %d = (%#v, %v)", scanned, result, err)
		}
		for index, item := range result.Items {
			if item.Disposition != store.PeerInboxIgnored {
				t.Fatalf("retained publication %d disposition = %q",
					after+uint64(index)+1, item.Disposition)
			}
		}
	}
}

func op01PublicationEvidence(t *testing.T, ctx context.Context, identity *node.Identity,
	channel store.ChannelObservationChannel, origin model.Member, home, audience model.PeerID,
	sequence uint64, at time.Time,
) model.PublicationEvidence {
	t.Helper()
	workID, err := model.ParseWorkID(fmt.Sprintf("work-op01-%d", sequence))
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewWorkRef(home, workID)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := model.NewEventScope(channel.Channel().ID(), origin.PeerID(),
		origin.OriginEpoch(), sequence, sequence, origin.Head(), channel.Roster().Head(), work)
	if err != nil {
		t.Fatal(err)
	}
	eventAudience, err := model.NewAudience([]model.PeerID{audience})
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := model.ParseEventID(fmt.Sprintf("event-op01-%d", sequence))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewJSON(
		[]byte(`{"content":"retained history","iteration":1,"work_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-op01-origin",
		Type: model.EventReviewOutcome, Audience: eventAudience,
		Summary: "OP-01 retained publication", Payload: payload,
		CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.PublicationSigningMessage(channel.Channel().ID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	signature, err := identity.PublicationSigner().Sign(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := model.AttachSignature(body, signature)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := model.ParsePublicationEvidence(publication.WireJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func op01RepairTarget(t *testing.T, ctx context.Context, st *store.Store,
	channelID model.ChannelID,
	origin model.PeerID, at time.Time,
) store.PeerRepairTarget {
	t.Helper()
	targets, err := st.ReadPeerRepairTargets(ctx, at.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if target.ChannelID() == channelID && target.OriginPeerID() == origin {
			return target
		}
	}
	t.Fatalf("origin %s has no retained-publication repair target", origin.String())
	return store.PeerRepairTarget{}
}

func op01ObservationChannel(t *testing.T, observation store.ChannelObservation,
	alias string,
) store.ChannelObservationChannel {
	t.Helper()
	for _, channel := range observation.Channels() {
		if channel.Channel().LocalAlias() == alias {
			return channel
		}
	}
	t.Fatalf("offline observation has no Channel alias %q", alias)
	return store.ChannelObservationChannel{}
}

func op01RosterMember(t *testing.T, channel store.ChannelObservationChannel,
	peerID model.PeerID,
) model.Member {
	t.Helper()
	for _, member := range channel.Roster().Members() {
		if member.PeerID() == peerID {
			return member
		}
	}
	t.Fatalf("Channel roster has no Peer %s", peerID.String())
	return model.Member{}
}

func op01ObservePublicStatusAndDoctor(t *testing.T, executable string,
	process *channelProcessNode,
) op01PublicObservation {
	t.Helper()
	status, statusBytes, statusLatency := op01WaitPublicStatus(t, executable, process)
	doctor, doctorBytes, doctorLatency := op01WaitPublicDoctor(t, executable, process, status)
	return op01PublicObservation{status: status, doctor: doctor,
		statusBytes: statusBytes, doctorBytes: doctorBytes,
		statusLatency: statusLatency, doctorLatency: doctorLatency}
}

func op01WaitPublicStatus(t *testing.T, executable string,
	process *channelProcessNode,
) (localapi.StatusResponse, int, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	var last localapi.StatusResponse
	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, op01StatusLatencyLimit)
		started := time.Now()
		result := setupProcessRunHarness(attemptCtx, executable, process.workspace,
			process.environment, "status")
		latency := time.Since(started)
		attemptCancel()
		status, err := setupProcessParseStatus(result)
		if err == nil {
			last = status
		} else {
			diagnostic := result
			diagnostic.err = nil
			if decoded, decodeErr := setupProcessParseStatus(diagnostic); decodeErr == nil {
				last = decoded
			}
		}
		if result.overflow || len(result.stdout) > localapi.MaxStatusResponseBytes ||
			latency > op01StatusLatencyLimit {
			t.Fatalf("public status exceeded its bound: bytes=%d latency=%s overflow=%t",
				len(result.stdout), latency, result.overflow)
		}
		if err == nil && status.Status == "ready" {
			return status, len(result.stdout), latency
		}
		lastErr = err
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("public status did not become ready: status=%q activation=%#v "+
				"runtime=%#v error=%v", last.Status, last.Activation, last.Runtime, lastErr)
		}
	}
}

func op01WaitPublicDoctor(t *testing.T, executable string, process *channelProcessNode,
	status localapi.StatusResponse,
) (setupProcessDoctorReport, int, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	var last setupProcessDoctorReport
	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, op01DoctorLatencyLimit)
		started := time.Now()
		result := setupProcessRunHarness(attemptCtx, executable, process.workspace,
			process.environment, "doctor")
		latency := time.Since(started)
		attemptCancel()
		doctor, err := setupProcessParseDoctor(result)
		if err == nil {
			last = doctor
		} else {
			diagnostic := result
			diagnostic.err = nil
			if decoded, decodeErr := setupProcessParseDoctor(diagnostic); decodeErr == nil {
				last = decoded
			}
		}
		if result.overflow || len(result.stdout) > op01DoctorResponseLimit ||
			latency > op01DoctorLatencyLimit {
			t.Fatalf("public doctor exceeded its bound: bytes=%d latency=%s overflow=%t",
				len(result.stdout), latency, result.overflow)
		}
		if err == nil && doctor.Mode == "online" && doctor.Status == "healthy" &&
			reflect.DeepEqual(doctor.Channels, status.Channels) {
			return doctor, len(result.stdout), latency
		}
		lastErr = err
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("public doctor did not become healthy: mode=%q status=%q checks=%#v error=%v",
				last.Mode, last.Status, last.Checks, lastErr)
		}
	}
}

func op01RequireStatusChannel(t *testing.T, response localapi.StatusResponse,
	alias string,
) localapi.StatusChannel {
	t.Helper()
	for _, channel := range response.Channels {
		if channel.Alias == alias {
			return channel
		}
	}
	t.Fatalf("public status has no Channel alias %q", alias)
	return localapi.StatusChannel{}
}
