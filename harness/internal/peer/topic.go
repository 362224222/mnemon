package peer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const publicationMessageIDDomain = "mnemon/r5/gossipsub-message-id/1"

var ErrGossipTopic = errors.New("Mnemon Channel Gossip topic")

// NewGossipSub mutates a Host-wide protocol mux. Serializing constructors and
// checking the mux itself makes the one-router invariant Host-scoped rather
// than accidentally depending on callers sharing one Authority pointer.
var gossipConstructorMu sync.Mutex

// PublicationMessageID implements the frozen valid-header tuple plus the
// PubSub original author. The latter prevents another active member from
// re-publishing identical application bytes under its own transport signature
// and poisoning the seen cache before validation rejects it. ReceivedFrom and
// transport seqno are deliberately excluded so relays and retries are stable.
//
// A structurally invalid frame uses a separate topic+raw fallback. Parsing is
// bounded by the publication cap, performs no I/O and is total for every frame
// admitted by GossipSub's outer wire limit.
func PublicationMessageID(message *pb.Message) string {
	hash := sha256.New()
	if message == nil {
		writeMessageIDFields(hash, []byte(publicationMessageIDDomain), []byte("invalid"), nil, nil)
		return "r5:" + hex.EncodeToString(hash.Sum(nil))
	}
	publication, err := model.ParseSignedPublication(message.GetData())
	if err != nil {
		writeMessageIDFields(hash, []byte(publicationMessageIDDomain), []byte("invalid"),
			[]byte(message.GetTopic()), message.GetData())
		return "r5:" + hex.EncodeToString(hash.Sum(nil))
	}
	scope := publication.Event().Scope()
	sequence := make([]byte, 8)
	binary.BigEndian.PutUint64(sequence, scope.ChannelSequence())
	writeMessageIDFields(hash,
		[]byte(publicationMessageIDDomain),
		[]byte("valid"),
		[]byte(message.GetTopic()),
		message.GetFrom(),
		[]byte(scope.ChannelID().String()),
		[]byte(scope.OriginPeerID().String()),
		[]byte(scope.OriginEpoch().String()),
		sequence,
		[]byte(publication.Event().ID().String()),
		publication.Event().Digest().Bytes(),
		publication.Digest().Bytes(),
	)
	return "r5:" + hex.EncodeToString(hash.Sum(nil))
}

func writeMessageIDFields(digest hash.Hash, fields ...[]byte) {
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write(field)
	}
}

type authoritySubscriptionFilter struct{ authority *Authority }

var _ pubsub.SubscriptionFilter = authoritySubscriptionFilter{}

func (filter authoritySubscriptionFilter) CanSubscribe(topic string) bool {
	return filter.authority != nil && filter.authority.CanSubscribe(topic)
}

func (filter authoritySubscriptionFilter) FilterIncomingSubscriptions(from libp2ppeer.ID,
	subscriptions []*pb.RPC_SubOpts,
) ([]*pb.RPC_SubOpts, error) {
	if len(subscriptions) > model.MaxChannelsPerNode {
		return nil, pubsub.ErrTooManySubscriptions
	}
	accepted := make(map[string]*pb.RPC_SubOpts, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription == nil {
			continue
		}
		topic := subscription.GetTopicid()
		if !filter.CanSubscribe(topic) {
			continue
		}
		// Unsubscribe never grants access and remains admissible for cleanup
		// after a scoped revoke. Subscribe requires exact active authority.
		if subscription.GetSubscribe() && !filter.authority.CanUseTopic(from, topic) {
			continue
		}
		if previous, duplicate := accepted[topic]; duplicate {
			if previous.GetSubscribe() != subscription.GetSubscribe() {
				delete(accepted, topic)
			}
			continue
		}
		accepted[topic] = subscription
	}
	result := make([]*pb.RPC_SubOpts, 0, len(accepted))
	for _, subscription := range accepted {
		result = append(result, subscription)
	}
	return result, nil
}

// Gossip owns one Node-wide GossipSub router and an independent session for
// each active Channel topic. It has no Store or Agent dependency.
type Gossip struct {
	ctx         context.Context
	cancel      context.CancelFunc
	host        host.Host
	pubsub      *pubsub.PubSub
	authority   *Authority
	mu          sync.Mutex
	sessions    map[model.ChannelID]*TopicSession
	gates       map[model.ChannelID]*channelGate
	refresh     map[libp2ppeer.ID]*peerRefresh
	refreshWake chan struct{}
	refreshDone chan struct{}
	refreshWG   sync.WaitGroup
	closed      bool
}

type peerRefresh struct {
	info    libp2ppeer.AddrInfo
	running bool
	attempt uint
	next    time.Time
	cancel  context.CancelFunc
}

// channelGate is stable for one Channel over the full Gossip runtime. The
// deliverable bit opens only after a current subscription is completely
// installed, preventing validators from accepted older queued messages into a
// seen cache while no successor can receive them.
type channelGate struct {
	sync.RWMutex
	deliverable atomic.Bool
}

func NewGossip(ctx context.Context, nodeHost host.Host, authority *Authority) (*Gossip, error) {
	if ctx == nil || ctx.Err() != nil || nodeHost == nil || authority == nil ||
		nodeHost.ID() != authority.LocalPeerID() {
		return nil, fmt.Errorf("%w: Host and authority identity are required", ErrGossipTopic)
	}
	gossipConstructorMu.Lock()
	defer gossipConstructorMu.Unlock()
	for _, protocolID := range nodeHost.Mux().Protocols() {
		if protocolID == GossipProtocol {
			return nil, fmt.Errorf("%w: Host already owns a GossipSub v1.2 handler", ErrGossipTopic)
		}
	}
	// Reserve before NewGossipSub installs a Host stream handler. A rejected
	// duplicate constructor must not overwrite the live router's handler.
	if !authority.bindRuntime() {
		return nil, fmt.Errorf("%w: authority already has a Gossip runtime", ErrGossipTopic)
	}
	limits := HermeticLimits()
	ownedContext, cancel := context.WithCancel(ctx)
	// PubSub receives a stream-scoped Host even when tests or future callers
	// provide a raw libp2p Host. Pending/unknown enrollment connections cannot
	// open or service the Node-wide data-plane stream.
	scopedHost := &managedHost{Host: nodeHost, gater: &ConnectionGater{authority: authority}}
	router, err := pubsub.NewGossipSub(ownedContext, scopedHost,
		pubsub.WithGossipSubProtocols([]protocol.ID{GossipProtocol},
			pubsub.GossipSubDefaultFeatures),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
		pubsub.WithMessageIdFn(PublicationMessageID),
		pubsub.WithPeerFilter(func(peerID libp2ppeer.ID, topic string) bool {
			return authority.CanUseTopic(peerID, topic)
		}),
		pubsub.WithSubscriptionFilter(authoritySubscriptionFilter{authority: authority}),
		pubsub.WithAppSpecificRpcInspector(authorityRPCInspector(authority)),
		pubsub.WithPeerExchange(false),
		pubsub.WithMaxMessageSize(limits.GossipWireBytes),
		pubsub.WithValidateWorkers(limits.ValidateWorkers),
		pubsub.WithValidateThrottle(limits.ValidateThrottle),
		pubsub.WithValidateQueueSize(limits.ValidateQueue),
		pubsub.WithPeerOutboundQueueSize(limits.PeerOutboundQueue),
	)
	if err != nil {
		cancel()
		// The reservation proves no earlier Gossip runtime owns this protocol
		// on the Host. Remove a partially installed handler defensively before
		// permitting a later constructor attempt.
		nodeHost.RemoveStreamHandler(GossipProtocol)
		authority.releaseRuntimeReservation()
		return nil, fmt.Errorf("%w: create GossipSub: %v", ErrGossipTopic, err)
	}
	gossip := &Gossip{ctx: ownedContext, cancel: cancel, host: nodeHost, pubsub: router, authority: authority,
		sessions:    make(map[model.ChannelID]*TopicSession),
		gates:       make(map[model.ChannelID]*channelGate),
		refresh:     make(map[libp2ppeer.ID]*peerRefresh),
		refreshWake: make(chan struct{}, 1), refreshDone: make(chan struct{})}
	go gossip.runRefreshWorker()
	return gossip, nil
}

func authorityRPCInspector(authority *Authority) func(libp2ppeer.ID, *pubsub.RPC) error {
	return func(from libp2ppeer.ID, rpc *pubsub.RPC) error {
		if authority == nil || from == "" || rpc == nil {
			return fmt.Errorf("%w: invalid incoming RPC", ErrGossipTopic)
		}
		// This runs before transport signature verification, MessageID parsing and
		// the global validation queue. Inspect only authenticated transport Peer +
		// exact raw topic; full publication authority remains in the validator.
		for _, message := range rpc.GetPublish() {
			if message == nil || !authority.CanUseTopic(from, message.GetTopic()) {
				return fmt.Errorf("%w: incoming publish is outside Peer Channel authority", ErrGossipTopic)
			}
		}
		return nil
	}
}

// Join registers the exact validator before joining/subscribing. Repeated
// reconciliation returns the one existing Channel session.
func (gossip *Gossip) Join(channelID model.ChannelID) (*TopicSession, error) {
	if gossip == nil || gossip.pubsub == nil {
		return nil, fmt.Errorf("%w: router is unavailable", ErrGossipTopic)
	}
	gossip.mu.Lock()
	defer gossip.mu.Unlock()
	topicName, err := TopicName(channelID)
	if err != nil || !gossip.authority.CanSubscribe(topicName) {
		return nil, fmt.Errorf("%w: Channel is not locally active", ErrGossipTopic)
	}
	return gossip.joinLocked(channelID, topicName)
}

func (gossip *Gossip) joinLocked(channelID model.ChannelID,
	topicName string,
) (*TopicSession, error) {
	if gossip.closed {
		return nil, fmt.Errorf("%w: router is closed", ErrGossipTopic)
	}
	if current := gossip.sessions[channelID]; current != nil {
		return current, nil
	}
	if gossip.gates == nil {
		gossip.gates = make(map[model.ChannelID]*channelGate)
	}
	gate := gossip.gates[channelID]
	if gate == nil {
		gate = &channelGate{}
		gossip.gates[channelID] = gate
	}
	session := &TopicSession{gossip: gossip, channelID: channelID, name: topicName, gate: gate}
	validator := gossip.validator(session)
	if err := gossip.pubsub.RegisterTopicValidator(topicName, validator,
		pubsub.WithValidatorInline(true)); err != nil {
		return nil, fmt.Errorf("%w: register validator: %v", ErrGossipTopic, err)
	}
	topic, err := gossip.pubsub.Join(topicName)
	if err != nil {
		_ = gossip.pubsub.UnregisterTopicValidator(topicName)
		return nil, fmt.Errorf("%w: join: %v", ErrGossipTopic, err)
	}
	subscription, err := topic.Subscribe(pubsub.WithBufferSize(HermeticLimits().SubscriptionBuffer))
	if err != nil {
		_ = topic.Close()
		_ = gossip.pubsub.UnregisterTopicValidator(topicName)
		return nil, fmt.Errorf("%w: subscribe: %v", ErrGossipTopic, err)
	}
	session.topic = topic
	session.subscription = subscription
	gossip.sessions[channelID] = session
	gate.deliverable.Store(true)
	return session, nil
}

// Reconcile validates a whole authority candidate, drains only its affected
// Channel gates, atomically installs the revision, and rotates those sessions
// before their gates reopen. Rotation is required because
// go-libp2p-pubsub's peer filter does not re-check the full-message send path
// for an existing mesh peer. Unchanged Channels keep publishing and receiving.
// Callers reacquire rotated sessions with Join; blocked Next calls on an old
// session are cancelled and must be restarted by the owning worker.
func (gossip *Gossip) Reconcile(snapshot NetworkAuthoritySnapshot) error {
	if gossip == nil || gossip.pubsub == nil || gossip.authority == nil {
		return fmt.Errorf("%w: router is unavailable", ErrGossipTopic)
	}
	gossip.mu.Lock()
	defer gossip.mu.Unlock()
	if gossip.closed {
		return fmt.Errorf("%w: router is closed", ErrGossipTopic)
	}
	candidate, err := gossip.authority.prepare(snapshot)
	if err != nil {
		return fmt.Errorf("%w: prepare authority: %v", ErrGossipTopic, err)
	}
	gossip.authority.beginUpdate()
	current := gossip.authority.state.Load()
	promotedPeers := dataPlanePromotions(current, candidate)
	// Every Channel ever joined keeps one stable gate for this router's full
	// lifetime. A PubSub validator closure can outlive its TopicSession, so an
	// authority revision must also drain gates whose current session was closed
	// or temporarily inactive.
	channelIDs := make([]model.ChannelID, 0, len(gossip.gates))
	previous := make(map[model.ChannelID]topicAuthorityGeneration, len(gossip.gates))
	for channelID, gate := range gossip.gates {
		if gate == nil {
			continue
		}
		channelIDs = append(channelIDs, channelID)
		previous[channelID] = topicGenerationFromState(current, channelID)
	}
	sort.Slice(channelIDs, func(left, right int) bool {
		return channelIDs[left].String() < channelIDs[right].String()
	})
	affected := channelIDs[:0]
	for _, channelID := range channelIDs {
		if previous[channelID] != topicGenerationFromState(candidate, channelID) {
			affected = append(affected, channelID)
		}
	}
	locked := make([]*channelGate, 0, len(affected))
	lockedSet := make(map[*channelGate]struct{}, len(affected))
	oldSessions := make(map[model.ChannelID]*TopicSession, len(affected))
	for _, channelID := range affected {
		gate := gossip.gates[channelID]
		gate.Lock()
		locked = append(locked, gate)
		lockedSet[gate] = struct{}{}
		if session := gossip.sessions[channelID]; session != nil {
			oldSessions[channelID] = session
		}
	}
	// Every affected Channel is now drained at the application boundary. The
	// immutable authority pointer swap is the revision linearization point;
	// unrelated Channel sessions never take these gates and keep running.
	gossip.authority.install(candidate)
	gossip.authority.finishUpdate()
	unlockGates := func() {
		for index := len(locked) - 1; index >= 0; index-- {
			locked[index].Unlock()
		}
		locked = nil
	}
	defer unlockGates()

	var reconcileErrors []error
	for _, channelID := range affected {
		session := gossip.sessions[channelID]
		if session == nil {
			continue
		}
		if err := gossip.closeSessionLocked(session); err != nil {
			reconcileErrors = append(reconcileErrors,
				fmt.Errorf("rotate Channel %s: %w", channelID.String(), err))
		}
	}
	if len(reconcileErrors) > 0 {
		// A partially closed old mesh cannot safely coexist with a newly
		// installed authority. Keep the new fail-closed policy and stop the
		// whole router; daemon restart reconstructs clean sessions.
		gossip.closed = true
		if gossip.cancel != nil {
			gossip.cancel()
		}
		for _, session := range gossip.sortedSessionsLocked() {
			_, alreadyLocked := lockedSet[session.gate]
			if !alreadyLocked {
				session.gate.Lock()
			}
			if err := gossip.closeSessionLocked(session); err != nil {
				reconcileErrors = append(reconcileErrors, err)
			}
			if !alreadyLocked {
				session.gate.Unlock()
			}
		}
		return errors.Join(reconcileErrors...)
	}
	for _, channelID := range affected {
		oldSession := oldSessions[channelID]
		if oldSession == nil {
			continue
		}
		topicName, err := TopicName(channelID)
		if err != nil || !gossip.authority.CanSubscribe(topicName) {
			continue
		}
		if _, err := gossip.joinLocked(channelID, topicName); err != nil {
			reconcileErrors = append(reconcileErrors,
				fmt.Errorf("rejoin Channel %s: %w", channelID.String(), err))
		} else {
			oldSession.handoff.Store(true)
		}
	}
	gossip.resetUnauthorizedDataPlaneStreams()
	if len(reconcileErrors) > 0 {
		return errors.Join(reconcileErrors...)
	}
	desiredPeers := dataPlanePeers(candidate)
	gossip.pruneRefreshPeers(desiredPeers)
	gossip.recordPromotedPeers(promotedPeers)
	unlockGates()
	gossip.signalRefresh()
	return nil
}

func dataPlanePromotions(current, candidate *networkAuthorityState) []libp2ppeer.ID {
	currentPeers := dataPlanePeers(current)
	candidatePeers := dataPlanePeers(candidate)
	promoted := make([]libp2ppeer.ID, 0, len(candidatePeers))
	for peerID := range candidatePeers {
		if _, alreadyActive := currentPeers[peerID]; !alreadyActive {
			promoted = append(promoted, peerID)
		}
	}
	sort.Slice(promoted, func(left, right int) bool {
		return promoted[left].String() < promoted[right].String()
	})
	return promoted
}

func dataPlanePeers(state *networkAuthorityState) map[libp2ppeer.ID]struct{} {
	peers := make(map[libp2ppeer.ID]struct{})
	if state == nil {
		return peers
	}
	for _, channel := range state.channels {
		if channel.status != model.ChannelActive {
			continue
		}
		for peerID, binding := range channel.bindings {
			if binding == model.BindingActive {
				peers[peerID] = struct{}{}
			}
		}
	}
	return peers
}

func (gossip *Gossip) pruneRefreshPeers(desired map[libp2ppeer.ID]struct{}) {
	if gossip == nil {
		return
	}
	for peerID := range gossip.refresh {
		if _, keep := desired[peerID]; !keep {
			if refresh := gossip.refresh[peerID]; refresh != nil && refresh.cancel != nil {
				refresh.cancel()
			}
			delete(gossip.refresh, peerID)
		}
	}
}

func (gossip *Gossip) recordPromotedPeers(promoted []libp2ppeer.ID) {
	if gossip == nil || gossip.host == nil {
		return
	}
	if gossip.refresh == nil {
		gossip.refresh = make(map[libp2ppeer.ID]*peerRefresh)
	}
	for _, peerID := range promoted {
		connections := gossip.host.Network().ConnsToPeer(peerID)
		if len(connections) == 0 {
			continue
		}
		addresses := gossip.host.Peerstore().Addrs(peerID)
		if len(addresses) == 0 {
			for _, connection := range connections {
				addresses = append(addresses, connection.RemoteMultiaddr())
			}
		}
		if previous := gossip.refresh[peerID]; previous != nil && previous.cancel != nil {
			previous.cancel()
		}
		gossip.refresh[peerID] = &peerRefresh{info: libp2ppeer.AddrInfo{ID: peerID, Addrs: addresses}}
	}
}

func (gossip *Gossip) signalRefresh() {
	if gossip == nil || gossip.refreshWake == nil {
		return
	}
	select {
	case gossip.refreshWake <- struct{}{}:
	default:
	}
}

func (gossip *Gossip) runRefreshWorker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer func() {
		gossip.refreshWG.Wait()
		close(gossip.refreshDone)
	}()
	for {
		select {
		case <-gossip.ctx.Done():
			return
		case <-gossip.refreshWake:
		case <-ticker.C:
		}
		gossip.startRefreshAttempts(time.Now())
	}
}

func (gossip *Gossip) startRefreshAttempts(now time.Time) {
	gossip.mu.Lock()
	defer gossip.mu.Unlock()
	if gossip.closed {
		return
	}
	for peerID, refresh := range gossip.refresh {
		if !gossip.authority.CanOpenDataPlane(peerID) {
			if refresh != nil && refresh.cancel != nil {
				refresh.cancel()
			}
			delete(gossip.refresh, peerID)
			continue
		}
		if gossip.hasHealthyDataPlaneStream(peerID) {
			if refresh != nil && refresh.cancel != nil {
				refresh.cancel()
			}
			delete(gossip.refresh, peerID)
			continue
		}
		if refresh == nil || refresh.running || now.Before(refresh.next) {
			continue
		}
		refresh.running = true
		info := refresh.info
		attemptContext, cancel := context.WithCancel(gossip.ctx)
		refresh.cancel = cancel
		gossip.refreshWG.Add(1)
		go gossip.refreshPeer(attemptContext, info, refresh)
	}
}

func (gossip *Gossip) refreshPeer(attemptContext context.Context,
	info libp2ppeer.AddrInfo, expected *peerRefresh,
) {
	defer gossip.refreshWG.Done()
	defer expected.cancel()
	updated := info
	if addresses := gossip.host.Peerstore().Addrs(info.ID); len(addresses) > 0 {
		updated.Addrs = addresses
	}
	if !gossip.hasHealthyDataPlaneStream(info.ID) {
		if connections := gossip.host.Network().ConnsToPeer(info.ID); len(connections) > 0 {
			if len(updated.Addrs) == 0 {
				for _, connection := range connections {
					updated.Addrs = append(updated.Addrs, connection.RemoteMultiaddr())
				}
			}
			_ = gossip.host.Network().ClosePeer(info.ID)
		}
		ctx, cancel := context.WithTimeout(attemptContext, HermeticLimits().ChannelRequestTimeout)
		_ = gossip.host.Connect(ctx, updated)
		cancel()
	}

	gossip.finishRefresh(info.ID, updated, expected)
	gossip.signalRefresh()
}

func (gossip *Gossip) finishRefresh(peerID libp2ppeer.ID,
	updated libp2ppeer.AddrInfo, expected *peerRefresh,
) {
	gossip.mu.Lock()
	defer gossip.mu.Unlock()
	refresh := gossip.refresh[peerID]
	if refresh == expected {
		refresh.running = false
		refresh.cancel = nil
		refresh.info = updated
		if !gossip.authority.CanOpenDataPlane(peerID) || gossip.hasHealthyDataPlaneStream(peerID) {
			delete(gossip.refresh, peerID)
		} else {
			refresh.next = time.Now().Add(refreshRetryDelay(refresh.attempt))
			refresh.attempt++
		}
	}
}

func refreshRetryDelay(attempt uint) time.Duration {
	if attempt > 4 {
		attempt = 4
	}
	return (100 * time.Millisecond) << attempt
}

func (gossip *Gossip) hasHealthyDataPlaneStream(peerID libp2ppeer.ID) bool {
	for _, connection := range gossip.host.Network().ConnsToPeer(peerID) {
		for _, stream := range connection.GetStreams() {
			if stream != nil && stream.Protocol() == GossipProtocol &&
				stream.Stat().Direction == network.DirOutbound {
				return true
			}
		}
	}
	return false
}

func (gossip *Gossip) resetUnauthorizedDataPlaneStreams() {
	if gossip == nil || gossip.host == nil || gossip.authority == nil {
		return
	}
	for _, connection := range gossip.host.Network().Conns() {
		if connection == nil || gossip.authority.CanOpenDataPlane(connection.RemotePeer()) {
			continue
		}
		for _, stream := range connection.GetStreams() {
			if stream != nil && stream.Protocol() == GossipProtocol {
				_ = stream.Reset()
			}
		}
	}
}

func (gossip *Gossip) validator(session *TopicSession) pubsub.ValidatorEx {
	return func(_ context.Context, transportPeer libp2ppeer.ID,
		message *pubsub.Message,
	) pubsub.ValidationResult {
		if session == nil || session.gate == nil {
			return pubsub.ValidationReject
		}
		// Topic.Publish validates synchronously and already holds the read side
		// of the publication gate. sync.RWMutex is not recursively readable
		// once a writer is pending, so only remote validation acquires here.
		// A remote transport can never authenticate as the local Host PeerID.
		if transportPeer != gossip.authority.LocalPeerID() {
			session.gate.RLock()
			defer session.gate.RUnlock()
		} else if session.localPublishes.Load() == 0 {
			return pubsub.ValidationReject
		}
		topic := session.name
		if !session.gate.deliverable.Load() ||
			(session.closed.Load() && !session.handoff.Load()) {
			return pubsub.ValidationReject
		}
		if message == nil || message.Message == nil || message.GetTopic() != topic {
			return pubsub.ValidationReject
		}
		channelID, err := ParseTopicName(topic)
		if err != nil {
			return pubsub.ValidationReject
		}
		publication, err := model.ParseSignedPublication(message.GetData())
		if err != nil || publication.Key().ChannelID() != channelID {
			return pubsub.ValidationReject
		}
		originalAuthor := message.GetFrom()
		scope := publication.Event().Scope()
		if originalAuthor == "" || originalAuthor.String() != scope.OriginPeerID().String() {
			return pubsub.ValidationReject
		}
		publicKey, authorized := gossip.authority.AuthorizePublication(channelID, transportPeer,
			originalAuthor, scope.OriginEpoch(), scope.OriginMember(), scope.PublicationRoster())
		if !authorized || model.VerifyPublication(publicKey, publication) != nil {
			return pubsub.ValidationReject
		}
		message.ValidatorData = publication
		return pubsub.ValidationAccept
	}
}

func (gossip *Gossip) Close() error {
	if gossip == nil {
		return nil
	}
	gossip.mu.Lock()
	if gossip.closed {
		done := gossip.refreshDone
		gossip.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	gossip.closed = true
	sessions := gossip.sortedSessionsLocked()
	var closeErrors []error
	for _, session := range sessions {
		session.gate.Lock()
		if err := gossip.closeSessionLocked(session); err != nil {
			closeErrors = append(closeErrors, err)
		}
		session.gate.Unlock()
	}
	gossip.mu.Unlock()
	if gossip.cancel != nil {
		gossip.cancel()
	}
	if gossip.refreshDone != nil {
		<-gossip.refreshDone
	}
	return errors.Join(closeErrors...)
}

func (gossip *Gossip) sortedSessionsLocked() []*TopicSession {
	sessions := make([]*TopicSession, 0, len(gossip.sessions))
	for _, session := range gossip.sessions {
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(left, right int) bool {
		return sessions[left].channelID.String() < sessions[right].channelID.String()
	})
	return sessions
}

type ReceivedPublication struct {
	Publication    model.SignedPublication
	ReceivedFrom   libp2ppeer.ID
	OriginalAuthor libp2ppeer.ID
	Local          bool
}

type TopicSession struct {
	gossip         *Gossip
	channelID      model.ChannelID
	name           string
	topic          *pubsub.Topic
	subscription   *pubsub.Subscription
	gate           *channelGate
	localPublishes atomic.Int64
	closed         atomic.Bool
	handoff        atomic.Bool
	closeOnce      sync.Once
	closeErr       error
}

func (session *TopicSession) ChannelID() model.ChannelID { return session.channelID }
func (session *TopicSession) Name() string               { return session.name }

func (session *TopicSession) Publish(ctx context.Context,
	publication model.SignedPublication,
) error {
	if session == nil || session.gossip == nil || session.gate == nil {
		return fmt.Errorf("%w: publication lacks local Channel authority", ErrGossipTopic)
	}
	session.gate.RLock()
	defer session.gate.RUnlock()
	return session.publishUnderGate(ctx, publication)
}

func (session *TopicSession) publishUnderGate(ctx context.Context,
	publication model.SignedPublication,
) error {
	if session.closed.Load() || ctx == nil || ctx.Err() != nil ||
		publication.Event().ID().IsZero() || publication.Key().ChannelID() != session.channelID ||
		publication.Event().Scope().OriginPeerID().String() != session.gossip.authority.LocalPeerID().String() ||
		!session.gossip.authority.CanUseTopic(session.gossip.authority.LocalPeerID(), session.name) {
		return fmt.Errorf("%w: publication lacks local Channel authority", ErrGossipTopic)
	}
	session.localPublishes.Add(1)
	defer session.localPublishes.Add(-1)
	if err := session.topic.Publish(ctx, publication.WireJSON().Bytes()); err != nil {
		return fmt.Errorf("%w: publish: %v", ErrGossipTopic, err)
	}
	return nil
}

func (session *TopicSession) Next(ctx context.Context) (ReceivedPublication, error) {
	if session == nil || session.gossip == nil || session.gate == nil || session.closed.Load() || ctx == nil {
		return ReceivedPublication{}, fmt.Errorf("%w: subscription is unavailable", ErrGossipTopic)
	}
	message, err := session.subscription.Next(ctx)
	if err != nil {
		return ReceivedPublication{}, fmt.Errorf("%w: receive: %v", ErrGossipTopic, err)
	}
	session.gate.RLock()
	defer session.gate.RUnlock()
	if session.closed.Load() || message == nil || message.Message == nil || message.GetTopic() != session.name {
		return ReceivedPublication{}, fmt.Errorf("%w: received publication belongs to a retired generation", ErrGossipTopic)
	}
	publication, ok := message.ValidatorData.(model.SignedPublication)
	if !ok {
		return ReceivedPublication{}, fmt.Errorf("%w: accepted message lost validator authority", ErrGossipTopic)
	}
	scope := publication.Event().Scope()
	publicKey, authorized := session.gossip.authority.AuthorizePublication(session.channelID,
		message.ReceivedFrom, message.GetFrom(), scope.OriginEpoch(), scope.OriginMember(),
		scope.PublicationRoster())
	if !authorized || model.VerifyPublication(publicKey, publication) != nil {
		return ReceivedPublication{}, fmt.Errorf("%w: received publication authority is no longer current", ErrGossipTopic)
	}
	return ReceivedPublication{Publication: publication, ReceivedFrom: message.ReceivedFrom,
		OriginalAuthor: message.GetFrom(), Local: message.Local}, nil
}

func (session *TopicSession) Peers() []libp2ppeer.ID {
	if session == nil || session.gossip == nil || session.gate == nil {
		return nil
	}
	session.gate.RLock()
	defer session.gate.RUnlock()
	if session.closed.Load() {
		return nil
	}
	peers := session.topic.ListPeers()
	result := make([]libp2ppeer.ID, 0, len(peers))
	for _, peerID := range peers {
		if session.gossip.authority.CanUseTopic(peerID, session.name) {
			result = append(result, peerID)
		}
	}
	return result
}

// Close always performs cancel -> topic close -> validator unregister and
// removes only this Channel session.
func (session *TopicSession) Close() error {
	if session == nil {
		return nil
	}
	if session.gossip == nil || session.gate == nil {
		return nil
	}
	session.gossip.mu.Lock()
	defer session.gossip.mu.Unlock()
	session.gate.Lock()
	defer session.gate.Unlock()
	return session.gossip.closeSessionLocked(session)
}

// closeSessionLocked requires Gossip.mu and the session write gate across the
// full cleanup sequence. A concurrent Join cannot observe a deleted map entry
// until the old validator is unregistered and the topic is safe to recreate.
func (gossip *Gossip) closeSessionLocked(session *TopicSession) error {
	if session == nil {
		return nil
	}
	session.closeOnce.Do(func() {
		session.closed.Store(true)
		if session.subscription != nil {
			session.subscription.Cancel()
		}
		var closeErrors []error
		if session.topic != nil {
			if err := session.topic.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("close topic: %w", err))
			}
		}
		if gossip.pubsub != nil {
			if err := gossip.pubsub.UnregisterTopicValidator(session.name); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("unregister validator: %w", err))
			}
		}
		if gossip.sessions[session.channelID] == session {
			delete(gossip.sessions, session.channelID)
			session.gate.deliverable.Store(false)
		}
		session.closeErr = errors.Join(closeErrors...)
	})
	return session.closeErr
}
