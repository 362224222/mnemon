package node

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestOpenManagedDaemonComposesOneSharedArtifactAndChannelAuthority(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	options := daemonTestManagedOptions(fixture)
	var casCalls int
	var sharedCAS *artifact.CAS
	options.artifactCASFactory = func(root string) (*artifact.CAS, error) {
		casCalls++
		if root != filepath.Join(fixture.nodeState, "objects", "sha256") {
			t.Fatalf("CAS root = %q", root)
		}
		var err error
		sharedCAS, err = artifact.NewCAS(root)
		return sharedCAS, err
	}
	var capturedRuntime *peer.MeshRuntime
	var captured peer.MeshTransportOptions
	var capturedTransport managedChannelTransport
	options.meshTransportFactory = func(runtime *peer.MeshRuntime,
		transportOptions peer.MeshTransportOptions,
	) (managedChannelTransport, error) {
		capturedRuntime, captured = runtime, transportOptions
		transport, err := peer.NewMeshTransport(runtime, transportOptions)
		capturedTransport = transport
		return transport, err
	}
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if casCalls != 1 || sharedCAS == nil || daemon.artifactCAS != sharedCAS ||
		daemon.controller.artifactCAS != sharedCAS || captured.ArtifactCAS != sharedCAS {
		t.Fatalf("shared CAS = calls %d daemon=%p controller=%p transport=%T",
			casCalls, daemon.artifactCAS, daemon.controller.artifactCAS, captured.ArtifactCAS)
	}
	if daemon.channelAuthority == nil || capturedRuntime != daemon.mesh ||
		captured.Enrollment.Controller != daemon.channelAuthority ||
		captured.Member.Controller != daemon.channelAuthority ||
		captured.EventSource != daemon.store || captured.ArtifactStore != daemon.store {
		t.Fatalf("transport composition = runtime %p authority=%p options=%#v",
			capturedRuntime, daemon.channelAuthority, captured)
	}
	if daemon.channelRuntime == nil || daemon.controller.channelRuntime != daemon.channelRuntime ||
		daemon.channelRuntime.store != daemon.store ||
		daemon.channelRuntime.transport != capturedTransport ||
		daemon.channelRuntime.authority != daemon.channelAuthority {
		t.Fatalf("Channel runtime composition = daemon=%p controller=%T Store=%p transport=%T authority=%T",
			daemon.channelRuntime, daemon.controller.channelRuntime, daemon.channelRuntime.store,
			daemon.channelRuntime.transport, daemon.channelRuntime.authority)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	assertClosedDescriptor(t, childFD)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonRealMeshRejoinsDurableChannelBeforeLocalAdmission(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	endpoint := publishDaemonTestMeshFinal(t, fixture, true)
	seedAt := fixture.profile.UpdatedAt().Add(time.Second)
	runtimeAt := seedAt.Add(3 * time.Second)
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	pristine, err := st.ReadMeshPristineAuthority(context.Background())
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	create := daemonTestOwnerChannelSpec(t, fixture.identity, pristine.Node(),
		endpoint.advertisedAddresses(), seedAt)
	created, err := st.CreateChannel(context.Background(), create)
	if err != nil || !created.Created {
		_ = st.Close()
		t.Fatalf("create restart Channel = (%#v,%v)", created, err)
	}
	oldJoinedAt := seedAt.Add(2 * time.Second)
	setDaemonTestChannelJoined(t, st, created.Channel, seedAt.Add(time.Second), oldJoinedAt)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.Clock = controllerTestClock{runtimeAt}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	before := readDaemonTestChannelTopic(t, daemon.store, created.Channel.ID())
	if before.TopicState != model.TopicJoined || !before.UpdatedAt.Equal(oldJoinedAt) ||
		daemon.channelRuntime.Snapshot().State != ChannelRuntimeIdle ||
		!daemon.mesh.HasCurrentSession(created.Channel.ID()) {
		t.Fatalf("pre-Serve Channel authority = topic %#v runtime %#v session=%t", before,
			daemon.channelRuntime.Snapshot(), daemon.mesh.HasCurrentSession(created.Channel.ID()))
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	waitControllerCompositionSignal(t, control.runStarted)
	assertClosedDescriptor(t, childFD)
	after := readDaemonTestChannelTopic(t, daemon.store, created.Channel.ID())
	snapshot := daemon.channelRuntime.Snapshot()
	if after.TopicState != model.TopicJoined || !after.UpdatedAt.Equal(runtimeAt) ||
		snapshot.State != ChannelRuntimeRunning || !snapshot.LocalTopicsReady ||
		snapshot.ActiveTopics != 1 || !daemon.mesh.HasCurrentSession(created.Channel.ID()) {
		t.Fatalf("admitted Channel authority = topic %#v runtime %#v session=%t", after,
			snapshot, daemon.mesh.HasCurrentSession(created.Channel.ID()))
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitControllerCompositionError(t, served); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenManagedDaemonTransportFactoryFailureRollsBackAuthority(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	options := daemonTestManagedOptions(fixture)
	var casCalls atomic.Int32
	options.artifactCASFactory = func(root string) (*artifact.CAS, error) {
		casCalls.Add(1)
		return artifact.NewCAS(root)
	}
	factoryErr := errors.New("injected mesh transport construction failure")
	partial := newDaemonTransportStub()
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedChannelTransport, error) {
		return partial, factoryErr
	}
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) || !errors.Is(err, factoryErr) ||
		casCalls.Load() != 1 || partial.closeCalls.Load() != 1 {
		t.Fatalf("OpenManagedDaemon() = (%v,%v), CAS calls=%d partial closes=%d",
			daemon, err, casCalls.Load(), partial.closeCalls.Load())
	}
	assertClosedDescriptor(t, childFD)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	final, ok := state.finalAuthority()
	if inspectErr != nil || !ok {
		t.Fatalf("rollback endpoint = (%#v,%v)", state, inspectErr)
	}
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonMeshAndLocalTopicStartupPrecedePermitAndLocalAccept(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	mesh := newDaemonTransportStub()
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedChannelTransport, error) {
		return mesh, nil
	}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	waitControllerCompositionSignal(t, mesh.runStarted)
	assertOpenDescriptor(t, childFD)
	if channelClosed(control.runStarted) {
		t.Fatal("local control accepted before mesh readiness")
	}
	mesh.ready <- nil
	waitControllerCompositionSignal(t, control.runStarted)
	assertClosedDescriptor(t, childFD)
	snapshot := daemon.channelRuntime.Snapshot()
	if snapshot.State != ChannelRuntimeRunning || !snapshot.LocalTopicsReady {
		t.Fatalf("Channel runtime at local admission = %#v", snapshot)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitControllerCompositionError(t, served); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonDrainRetainsHostAndStoreUntilTransportClose(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, _ := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	mesh := newDaemonTransportStub()
	mesh.blockClose = true
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedChannelTransport, error) {
		return mesh, nil
	}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	state, err := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	final, ok := state.finalAuthority()
	if err != nil || !ok {
		t.Fatalf("managed endpoint = (%#v,%v)", state, err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	mesh.ready <- nil
	waitControllerCompositionSignal(t, control.runStarted)
	closed := make(chan error, 1)
	go func() { closed <- daemon.Close() }()
	waitControllerCompositionSignal(t, mesh.closeStarted)
	assertDaemonTestMeshPort(t, final, false)
	if reopened, openErr := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db")); openErr == nil {
		_ = reopened.Close()
		t.Fatal("Store writer authority was released before transport drain")
	}
	close(mesh.releaseClose)
	if err := waitControllerCompositionError(t, closed); err != nil {
		t.Fatal(err)
	}
	if err := waitControllerCompositionError(t, served); err != nil {
		t.Fatal(err)
	}
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonPropagatesMeshTransportTerminalFailure(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, _ := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	mesh := newDaemonTransportStub()
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedChannelTransport, error) {
		return mesh, nil
	}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	mesh.ready <- nil
	waitControllerCompositionSignal(t, control.runStarted)
	terminalErr := errors.New("injected managed mesh terminal failure")
	mesh.terminal <- terminalErr
	err = waitControllerCompositionError(t, served)
	if !errors.Is(err, terminalErr) {
		t.Fatalf("Serve() = %v", err)
	}
	if reopened, openErr := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db")); openErr == nil {
		_ = reopened.Close()
		t.Fatal("terminal component exit released Daemon Store ownership")
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func daemonTestOwnerChannelSpec(t *testing.T, identity *Identity, node model.Node,
	addresses []string, at time.Time,
) store.CreateChannelSpec {
	t.Helper()
	channelID, err := model.ParseChannelID("channel-daemon-runtime-restart")
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := model.NewChannelDescriptor(model.ChannelDescriptorSpec{
		ID: channelID, Name: "Daemon runtime restart", OwnerPeerID: identity.PeerID(),
		OwnerPublicKey: identity.PublicKey(), MemberLimit: model.MaxMembersPerChannel, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	descriptorMessage, err := model.ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	if err != nil {
		t.Fatal(err)
	}
	signedDescriptor, err := model.AttachChannelDescriptorSignature(descriptor,
		daemonTestChannelSignature(t, identity, descriptorMessage))
	if err != nil {
		t.Fatal(err)
	}
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Digest(), Revision: 1, PeerID: identity.PeerID(),
		OriginEpoch: node.OriginEpoch(), DisplayLabel: "daemon-owner", PublicKey: identity.PublicKey(),
		Multiaddrs: addresses, Protocols: model.RequiredMemberProtocols(),
		Limits: model.DefaultMemberLimits(), Status: model.MemberActive, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	recordMessage, err := model.MemberRecordSigningMessage(channelID, record.Digest())
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := model.AttachMemberSignature(record,
		daemonTestChannelSignature(t, identity, recordMessage))
	if err != nil {
		t.Fatal(err)
	}
	roster, err := model.NewVerifiedRoster(signedDescriptor, []model.Member{genesis})
	if err != nil {
		t.Fatal(err)
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: signedDescriptor,
		LocalAlias: "daemon-runtime", RosterHead: roster.Head(), Status: model.ChannelActive,
		TopicState: model.TopicNotJoined, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return store.CreateChannelSpec{Channel: channel, Genesis: genesis,
		Token: daemonTestEnrollmentToken(t, identity, signedDescriptor, addresses, at)}
}

func daemonTestEnrollmentToken(t *testing.T, identity *Identity,
	descriptor model.SignedChannelDescriptor, addresses []string, at time.Time,
) model.EnrollmentToken {
	t.Helper()
	grantID, err := model.ParseGrantID("grant-daemon-runtime-restart")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: descriptor, OwnerMultiaddrs: addresses, GrantID: grantID,
		BearerSecret: model.Sum([]byte("daemon-runtime-restart-secret")).Bytes(),
		ExpiresAt:    at.Add(time.Hour), MaxUses: model.MaxMembersPerChannel - 1,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentTokenSigningMessage(
		descriptor.Descriptor().ID(), payload.Digest())
	if err != nil {
		t.Fatal(err)
	}
	token, err := model.AttachEnrollmentTokenSignature(payload,
		daemonTestChannelSignature(t, identity, message))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func daemonTestChannelSignature(t *testing.T, identity *Identity, message []byte) []byte {
	t.Helper()
	signature, err := identity.PublicationSigner().Sign(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	return signature
}

func setDaemonTestChannelJoined(t *testing.T, st *store.Store, channel model.Channel,
	joiningAt, joinedAt time.Time,
) {
	t.Helper()
	joining, err := st.CompareAndSetChannelTopicState(context.Background(),
		store.CompareAndSetChannelTopicStateSpec{ChannelID: channel.ID(),
			ExpectedStatus: model.ChannelActive, ExpectedRosterHead: channel.RosterHead(),
			ExpectedTopicState: model.TopicNotJoined, TopicState: model.TopicJoining, At: joiningAt})
	if err != nil || !joining.Changed {
		t.Fatalf("set restart Channel joining = (%#v,%v)", joining, err)
	}
	joined, err := st.CompareAndSetChannelTopicState(context.Background(),
		store.CompareAndSetChannelTopicStateSpec{ChannelID: channel.ID(),
			ExpectedStatus: model.ChannelActive, ExpectedRosterHead: channel.RosterHead(),
			ExpectedTopicState: model.TopicJoining, TopicState: model.TopicJoined, At: joinedAt})
	if err != nil || !joined.Changed {
		t.Fatalf("set restart Channel joined = (%#v,%v)", joined, err)
	}
}

func readDaemonTestChannelTopic(t *testing.T, st *store.Store,
	channelID model.ChannelID,
) store.ChannelTopicProjection {
	t.Helper()
	topics, err := st.ReadChannelTopicRuntime(context.Background())
	if err != nil || len(topics) != 1 || topics[0].ChannelID != channelID {
		t.Fatalf("read restart Channel topic = (%#v,%v)", topics, err)
	}
	return topics[0]
}

type daemonTransportStub struct {
	ready        chan error
	terminal     chan error
	runStarted   chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
	stop         chan struct{}
	blockClose   bool
	runOnce      sync.Once
	closeOnce    sync.Once
	stopOnce     sync.Once
	closeCalls   atomic.Int32
}

func newDaemonTransportStub() *daemonTransportStub {
	return &daemonTransportStub{ready: make(chan error, 1), terminal: make(chan error, 1),
		runStarted: make(chan struct{}), closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}), stop: make(chan struct{})}
}

func (transport *daemonTransportStub) Run(ctx context.Context) error {
	transport.runOnce.Do(func() { close(transport.runStarted) })
	select {
	case err := <-transport.terminal:
		return err
	case <-transport.stop:
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (transport *daemonTransportStub) Readiness(ctx context.Context) error {
	select {
	case err := <-transport.ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (transport *daemonTransportStub) Close() error {
	transport.closeCalls.Add(1)
	transport.closeOnce.Do(func() { close(transport.closeStarted) })
	if transport.blockClose {
		select {
		case <-transport.releaseClose:
		case <-time.After(5 * time.Second):
			return errors.New("timed out waiting to release test mesh transport")
		}
	}
	transport.stopOnce.Do(func() { close(transport.stop) })
	return nil
}

func (*daemonTransportStub) EnsureChannelTopic(context.Context, model.ChannelID) error {
	return nil
}

func (*daemonTransportStub) HasCurrentChannelTopic(model.ChannelID) bool { return true }

func (*daemonTransportStub) Hello(context.Context, model.PeerID,
	peer.MemberHello,
) (peer.MemberHelloAck, error) {
	return peer.MemberHelloAck{}, context.DeadlineExceeded
}

func (*daemonTransportStub) Sync(context.Context, model.PeerID,
	peer.SyncRequest,
) (peer.ChannelMemberSyncResult, error) {
	return peer.ChannelMemberSyncResult{}, context.DeadlineExceeded
}

func (*daemonTransportStub) Baseline(context.Context, model.PeerID,
	peer.DataBaseline,
) (peer.DataBaselineAck, error) {
	return peer.DataBaselineAck{}, context.DeadlineExceeded
}
