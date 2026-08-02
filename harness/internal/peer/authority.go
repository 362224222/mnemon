package peer

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrNetworkAuthority = errors.New("invalid peer network authority snapshot")

// NetworkAuthoritySnapshot is a complete immutable projection of the R5
// Channel and owner-provisioned R7 Agency authority needed in libp2p callbacks.
// Reconciliation replaces it atomically; callbacks never query durable state
// or combine two revisions.
type NetworkAuthoritySnapshot struct {
	LocalPeerID             model.PeerID
	Channels                []ChannelAuthoritySnapshot
	AgencyPeers             []model.PeerID
	OutboundEnrollmentPeers []model.PeerID
}

type ChannelAuthoritySnapshot struct {
	ChannelID           model.ChannelID
	Status              model.ChannelStatus
	RosterHead          model.RecordHead
	VerifiedRosterHeads []model.RecordHead
	Bindings            []BindingAuthoritySnapshot
	Members             []MemberAuthoritySnapshot
}

type BindingAuthoritySnapshot struct {
	PeerID model.PeerID
	State  model.BindingState
}

// MemberAuthoritySnapshot is one verified owner-signed active membership
// record. Multiple historical active heads for one Peer may be retained so a
// delayed, immutable publication can still prove its exact signing key/epoch.
type MemberAuthoritySnapshot struct {
	PeerID      model.PeerID
	OriginEpoch model.OriginEpoch
	Head        model.RecordHead
	PublicKey   []byte
}

type memberAuthorityKey struct {
	peerID libp2ppeer.ID
	head   model.RecordHead
}

type memberAuthorityValue struct {
	originEpoch model.OriginEpoch
	publicKey   string
}

type channelAuthorityState struct {
	status     model.ChannelStatus
	rosterHead model.RecordHead
	roster     map[model.RecordHead]struct{}
	bindings   map[libp2ppeer.ID]model.BindingState
	members    map[memberAuthorityKey]memberAuthorityValue
}

type topicAuthorityGeneration struct {
	exists         bool
	status         model.ChannelStatus
	rosterHead     model.RecordHead
	activeBindings string
}

// physical is the union used only for connections. channelPhysical and agency
// remain disjoint protocol grants even when they name the same Peer. Keeping
// those source sets in the installed immutable revision prevents a shared
// transport from becoming a transitive authorization path between R5 and R7.
// outboundEnrollment is a bounded dial-only exception that joins neither grant.
type networkAuthorityState struct {
	localPeerID        libp2ppeer.ID
	channels           map[model.ChannelID]channelAuthorityState
	channelPhysical    map[libp2ppeer.ID]struct{}
	agency             map[libp2ppeer.ID]struct{}
	physical           map[libp2ppeer.ID]struct{}
	outboundEnrollment map[libp2ppeer.ID]struct{}
}

// Authority is the lock-free read projection shared by connection, stream and
// Gossip policy. Its state is never mutated after publication.
type Authority struct {
	localPeerID libp2ppeer.ID
	state       atomic.Pointer[networkAuthorityState]
	updateMu    sync.Mutex
	managed     atomic.Bool
}

func NewAuthority(localPeerID model.PeerID) (*Authority, error) {
	local, err := canonicalLibp2pID(localPeerID)
	if err != nil {
		return nil, fmt.Errorf("%w: local PeerID: %v", ErrNetworkAuthority, err)
	}
	authority := &Authority{localPeerID: local}
	authority.state.Store(&networkAuthorityState{localPeerID: local,
		channels:           make(map[model.ChannelID]channelAuthorityState),
		channelPhysical:    make(map[libp2ppeer.ID]struct{}),
		agency:             make(map[libp2ppeer.ID]struct{}),
		physical:           make(map[libp2ppeer.ID]struct{}),
		outboundEnrollment: make(map[libp2ppeer.ID]struct{})})
	return authority, nil
}

// Replace validates and atomically installs one whole Store revision. Failure
// leaves the previously visible authority untouched.
func (authority *Authority) Replace(snapshot NetworkAuthoritySnapshot) error {
	if authority == nil {
		return fmt.Errorf("%w: projection is unavailable", ErrNetworkAuthority)
	}
	authority.beginUpdate()
	defer authority.finishUpdate()
	if authority.managed.Load() {
		return fmt.Errorf("%w: runtime authority must reconcile through Gossip", ErrNetworkAuthority)
	}
	state, err := authority.prepare(snapshot)
	if err != nil {
		return err
	}
	authority.install(state)
	return nil
}

func (authority *Authority) install(state *networkAuthorityState) {
	authority.state.Store(state)
}

// beginUpdate serializes whole-snapshot writers. Readers remain lock-free and
// observe exactly the old or new immutable pointer, never a partial revision.
func (authority *Authority) beginUpdate() {
	authority.updateMu.Lock()
}

func (authority *Authority) finishUpdate() {
	authority.updateMu.Unlock()
}

func (authority *Authority) bindRuntime() bool {
	if authority == nil {
		return false
	}
	authority.beginUpdate()
	defer authority.finishUpdate()
	return authority.managed.CompareAndSwap(false, true)
}

// releaseRuntimeReservation rolls back a constructor reservation before a
// Gossip runtime becomes observable. A live runtime never releases its
// reservation: recreating Gossip on the same Host can overwrite libp2p stream
// handlers owned by the first router.
func (authority *Authority) releaseRuntimeReservation() {
	if authority == nil {
		return
	}
	authority.beginUpdate()
	defer authority.finishUpdate()
	authority.managed.Store(false)
}

func buildChannelAuthority(local libp2ppeer.ID,
	snapshot ChannelAuthoritySnapshot,
) (channelAuthorityState, error) {
	if snapshot.ChannelID.IsZero() || !snapshot.Status.Valid() || snapshot.RosterHead.IsZero() {
		return channelAuthorityState{}, fmt.Errorf("%w: incomplete Channel authority", ErrNetworkAuthority)
	}
	if _, err := TopicName(snapshot.ChannelID); err != nil {
		return channelAuthorityState{}, fmt.Errorf("%w: %v", ErrNetworkAuthority, err)
	}
	if len(snapshot.Bindings) > model.MaxMembersPerChannel-1 {
		return channelAuthorityState{}, fmt.Errorf("%w: Channel binding limit exceeded", ErrNetworkAuthority)
	}
	result := channelAuthorityState{status: snapshot.Status, rosterHead: snapshot.RosterHead,
		roster:   make(map[model.RecordHead]struct{}, len(snapshot.VerifiedRosterHeads)),
		bindings: make(map[libp2ppeer.ID]model.BindingState, len(snapshot.Bindings)),
		members:  make(map[memberAuthorityKey]memberAuthorityValue, len(snapshot.Members))}
	rosterRevisions := make(map[uint64]model.Digest, len(snapshot.VerifiedRosterHeads))
	for _, head := range snapshot.VerifiedRosterHeads {
		if head.IsZero() || head.Revision() > snapshot.RosterHead.Revision() {
			return channelAuthorityState{}, fmt.Errorf("%w: zero verified roster head", ErrNetworkAuthority)
		}
		if _, duplicate := result.roster[head]; duplicate {
			return channelAuthorityState{}, fmt.Errorf("%w: duplicate verified roster head", ErrNetworkAuthority)
		}
		result.roster[head] = struct{}{}
		if digest, exists := rosterRevisions[head.Revision()]; exists && digest != head.Digest() &&
			snapshot.Status == model.ChannelActive {
			return channelAuthorityState{}, fmt.Errorf("%w: active Channel contains a roster fork", ErrNetworkAuthority)
		}
		rosterRevisions[head.Revision()] = head.Digest()
	}
	if _, currentVerified := result.roster[snapshot.RosterHead]; !currentVerified {
		return channelAuthorityState{}, fmt.Errorf("%w: current roster head is not verified", ErrNetworkAuthority)
	}
	if snapshot.Status == model.ChannelActive {
		if uint64(len(rosterRevisions)) != snapshot.RosterHead.Revision() {
			return channelAuthorityState{}, fmt.Errorf("%w: active Channel roster prefix contains a gap", ErrNetworkAuthority)
		}
		for revision := uint64(1); revision <= snapshot.RosterHead.Revision(); revision++ {
			if _, verified := rosterRevisions[revision]; !verified {
				return channelAuthorityState{}, fmt.Errorf("%w: active Channel roster prefix contains a gap", ErrNetworkAuthority)
			}
		}
	}
	for _, binding := range snapshot.Bindings {
		peerID, err := canonicalLibp2pID(binding.PeerID)
		if err != nil || peerID == local || !binding.State.Valid() {
			return channelAuthorityState{}, fmt.Errorf("%w: invalid PeerBinding", ErrNetworkAuthority)
		}
		if _, duplicate := result.bindings[peerID]; duplicate {
			return channelAuthorityState{}, fmt.Errorf("%w: duplicate PeerBinding", ErrNetworkAuthority)
		}
		result.bindings[peerID] = binding.State
	}
	memberPeers := make(map[libp2ppeer.ID]struct{})
	memberEpochs := make(map[libp2ppeer.ID]model.OriginEpoch)
	memberHeadOwners := make(map[model.RecordHead]libp2ppeer.ID)
	for _, member := range snapshot.Members {
		peerID, err := canonicalLibp2pID(member.PeerID)
		if err != nil || member.OriginEpoch.IsZero() || member.Head.IsZero() ||
			len(member.PublicKey) != ed25519.PublicKeySize {
			return channelAuthorityState{}, fmt.Errorf("%w: invalid member credential", ErrNetworkAuthority)
		}
		if _, verified := result.roster[member.Head]; !verified {
			return channelAuthorityState{}, fmt.Errorf("%w: member head is outside verified roster", ErrNetworkAuthority)
		}
		if err := verifyMemberPeerID(peerID, member.PublicKey); err != nil {
			return channelAuthorityState{}, err
		}
		key := memberAuthorityKey{peerID: peerID, head: member.Head}
		if _, duplicate := result.members[key]; duplicate {
			return channelAuthorityState{}, fmt.Errorf("%w: duplicate member credential", ErrNetworkAuthority)
		}
		if epoch, exists := memberEpochs[peerID]; exists && epoch != member.OriginEpoch {
			return channelAuthorityState{}, fmt.Errorf("%w: member origin epoch changed", ErrNetworkAuthority)
		}
		if owner, exists := memberHeadOwners[member.Head]; exists && owner != peerID {
			return channelAuthorityState{}, fmt.Errorf("%w: one member record head names multiple Peers", ErrNetworkAuthority)
		}
		result.members[key] = memberAuthorityValue{originEpoch: member.OriginEpoch,
			publicKey: string(append([]byte(nil), member.PublicKey...))}
		memberPeers[peerID] = struct{}{}
		memberEpochs[peerID] = member.OriginEpoch
		memberHeadOwners[member.Head] = peerID
	}
	if snapshot.Status == model.ChannelActive {
		if _, localMember := memberPeers[local]; !localMember {
			return channelAuthorityState{}, fmt.Errorf("%w: active Channel lacks local membership", ErrNetworkAuthority)
		}
	}
	for peerID, binding := range result.bindings {
		if binding != model.BindingRevoked {
			if _, exists := memberPeers[peerID]; !exists {
				return channelAuthorityState{}, fmt.Errorf("%w: live binding lacks verified membership", ErrNetworkAuthority)
			}
		}
	}
	return result, nil
}

func verifyMemberPeerID(peerID libp2ppeer.ID, publicKey []byte) error {
	key, err := libp2pcrypto.UnmarshalEd25519PublicKey(publicKey)
	if err != nil {
		return fmt.Errorf("%w: invalid Ed25519 member key", ErrNetworkAuthority)
	}
	derived, err := libp2ppeer.IDFromPublicKey(key)
	if err != nil || derived != peerID {
		return fmt.Errorf("%w: member key does not derive its PeerID", ErrNetworkAuthority)
	}
	return nil
}

func canonicalLibp2pID(value model.PeerID) (libp2ppeer.ID, error) {
	if _, err := model.CanonicalPeerIDBytes(value); err != nil {
		return "", err
	}
	return libp2ppeer.Decode(value.String())
}

func (authority *Authority) LocalPeerID() libp2ppeer.ID {
	if authority == nil {
		return ""
	}
	return authority.localPeerID
}

// CanOpenDataPlane is stricter than physical connection authority: a Peer
// must have an active binding in at least one active Channel. Pending
// enrollment Peers retain their connection but can only use the Channel
// control protocol until the owner-signed roster becomes active.
func (authority *Authority) CanOpenDataPlane(peerID libp2ppeer.ID) bool {
	if authority == nil || peerID == "" || peerID == authority.localPeerID {
		return false
	}
	state := authority.state.Load()
	for _, channel := range state.channels {
		if channel.status == model.ChannelActive && channel.bindings[peerID] == model.BindingActive {
			return true
		}
	}
	return false
}

// CanDial additionally admits the fixed owner of one bounded outbound
// enrollment attempt. That permit never grants inbound, topic or stream data
// authority.
func (authority *Authority) CanDial(peerID libp2ppeer.ID) bool {
	if authority == nil || peerID == "" || peerID == authority.localPeerID {
		return false
	}
	state := authority.state.Load()
	if _, allowed := state.physical[peerID]; allowed {
		return true
	}
	_, allowed := state.outboundEnrollment[peerID]
	return allowed
}

func (authority *Authority) CanSubscribe(topic string) bool {
	channelID, err := ParseTopicName(topic)
	if authority == nil || err != nil {
		return false
	}
	state := authority.state.Load()
	channel, exists := state.channels[channelID]
	return exists && channel.status == model.ChannelActive
}

// CanUseTopic requires exact-Channel active binding. Local self is admitted
// only while its local Channel replica remains active.
func (authority *Authority) CanUseTopic(peerID libp2ppeer.ID, topic string) bool {
	channelID, err := ParseTopicName(topic)
	if authority == nil || peerID == "" || err != nil {
		return false
	}
	state := authority.state.Load()
	channel, exists := state.channels[channelID]
	if !exists || channel.status != model.ChannelActive {
		return false
	}
	if peerID == authority.localPeerID {
		return true
	}
	return channel.bindings[peerID] == model.BindingActive
}

// topicGeneration is the exact authority subset that can change topic
// lifecycle or egress. Other revisions, such as a bounded enrollment dial
// permit, do not disturb an unrelated live Channel.
func (authority *Authority) topicGeneration(channelID model.ChannelID) topicAuthorityGeneration {
	if authority == nil || authority.state.Load() == nil {
		return topicAuthorityGeneration{}
	}
	return topicGenerationFromState(authority.state.Load(), channelID)
}

func topicGenerationFromState(state *networkAuthorityState,
	channelID model.ChannelID,
) topicAuthorityGeneration {
	if state == nil {
		return topicAuthorityGeneration{}
	}
	channel, exists := state.channels[channelID]
	if !exists {
		return topicAuthorityGeneration{}
	}
	active := make([]string, 0, len(channel.bindings))
	for peerID, binding := range channel.bindings {
		if binding == model.BindingActive {
			active = append(active, peerID.String())
		}
	}
	sort.Strings(active)
	return topicAuthorityGeneration{exists: true, status: channel.status,
		rosterHead: channel.rosterHead, activeBindings: strings.Join(active, "\x00")}
}

// AuthorizePublication proves from one immutable snapshot that the immediate
// transport Peer and original author are both active in this exact Channel,
// and that the embedded historical member/roster heads are verified. Loading
// state once prevents a roster update from combining authority that never
// existed together in any revision. The returned key is copied.
func (authority *Authority) AuthorizePublication(channelID model.ChannelID,
	transportPeer, author libp2ppeer.ID, epoch model.OriginEpoch,
	memberHead, rosterHead model.RecordHead,
) ([]byte, bool) {
	if authority == nil || transportPeer == "" || author == "" || epoch.IsZero() ||
		memberHead.IsZero() || rosterHead.IsZero() {
		return nil, false
	}
	state := authority.state.Load()
	channel, exists := state.channels[channelID]
	if !exists || channel.status != model.ChannelActive {
		return nil, false
	}
	active := func(peerID libp2ppeer.ID) bool {
		return peerID == state.localPeerID || channel.bindings[peerID] == model.BindingActive
	}
	if !active(transportPeer) || !active(author) {
		return nil, false
	}
	if _, verified := channel.roster[rosterHead]; !verified ||
		rosterHead.Revision() > channel.rosterHead.Revision() {
		return nil, false
	}
	member, exists := channel.members[memberAuthorityKey{peerID: author, head: memberHead}]
	if !exists || member.originEpoch != epoch || memberHead.Revision() > rosterHead.Revision() {
		return nil, false
	}
	return append([]byte(nil), member.publicKey...), true
}
