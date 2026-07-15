package model

import "encoding/json"

type identifier struct {
	value string
}

func newIdentifier(field, value string) (identifier, error) {
	if err := validateIdentifier(field, value); err != nil {
		return identifier{}, err
	}
	return identifier{value: value}, nil
}

func (i identifier) String() string { return i.value }
func (i identifier) IsZero() bool   { return i.value == "" }

func (i identifier) MarshalJSON() ([]byte, error) {
	if i.IsZero() {
		return nil, invalid("identifier", "zero identifier")
	}
	return json.Marshal(i.value)
}

type PeerID struct{ identifier }
type OriginEpoch struct{ identifier }
type ChannelID struct{ identifier }
type EventID struct{ identifier }
type WorkID struct{ identifier }
type ProfileID struct{ identifier }
type HandlingID struct{ identifier }
type OperationID struct{ identifier }
type RunID struct{ identifier }
type InboxID struct{ identifier }

func ParsePeerID(value string) (PeerID, error) {
	id, err := newIdentifier("peer_id", value)
	return PeerID{id}, err
}

func ParseOriginEpoch(value string) (OriginEpoch, error) {
	id, err := newIdentifier("origin_epoch", value)
	return OriginEpoch{id}, err
}

func ParseChannelID(value string) (ChannelID, error) {
	id, err := newIdentifier("channel_id", value)
	return ChannelID{id}, err
}

func ParseEventID(value string) (EventID, error) {
	id, err := newIdentifier("event_id", value)
	return EventID{id}, err
}

func ParseWorkID(value string) (WorkID, error) {
	id, err := newIdentifier("work_id", value)
	return WorkID{id}, err
}

func ParseProfileID(value string) (ProfileID, error) {
	id, err := newIdentifier("profile_id", value)
	return ProfileID{id}, err
}

func ParseHandlingID(value string) (HandlingID, error) {
	id, err := newIdentifier("handling_id", value)
	return HandlingID{id}, err
}

func ParseOperationID(value string) (OperationID, error) {
	id, err := newIdentifier("operation_id", value)
	return OperationID{id}, err
}

func ParseRunID(value string) (RunID, error) {
	id, err := newIdentifier("run_id", value)
	return RunID{id}, err
}

func ParseInboxID(value string) (InboxID, error) {
	id, err := newIdentifier("inbox_id", value)
	return InboxID{id}, err
}

func TeamworkProfileID() ProfileID {
	id, _ := ParseProfileID("teamwork-default")
	return id
}

type RecordHead struct {
	revision uint64
	digest   Digest
}

func NewRecordHead(revision uint64, digest Digest) (RecordHead, error) {
	if err := validateSQLitePositive("record revision", revision); err != nil {
		return RecordHead{}, err
	}
	if digest.IsZero() {
		return RecordHead{}, invalid("record digest", "must not be zero")
	}
	return RecordHead{revision: revision, digest: digest}, nil
}

func (h RecordHead) Revision() uint64 { return h.revision }
func (h RecordHead) Digest() Digest   { return h.digest }
func (h RecordHead) IsZero() bool     { return h.revision == 0 || h.digest.IsZero() }

func (h RecordHead) MarshalJSON() ([]byte, error) {
	if h.IsZero() {
		return nil, invalid("record head", "zero record head")
	}
	return CanonicalMarshal(struct {
		Revision uint64 `json:"revision"`
		Digest   Digest `json:"digest"`
	}{h.revision, h.digest})
}

type WorkRef struct {
	homePeerID PeerID
	workID     WorkID
}

func NewWorkRef(homePeerID PeerID, workID WorkID) (WorkRef, error) {
	if homePeerID.IsZero() || workID.IsZero() {
		return WorkRef{}, invalid("work_ref", "home peer and work ID are required")
	}
	return WorkRef{homePeerID: homePeerID, workID: workID}, nil
}

func (r WorkRef) HomePeerID() PeerID { return r.homePeerID }
func (r WorkRef) WorkID() WorkID     { return r.workID }
func (r WorkRef) IsZero() bool       { return r.homePeerID.IsZero() || r.workID.IsZero() }
func (r WorkRef) key() string        { return r.homePeerID.String() + "\x00" + r.workID.String() }

func (r WorkRef) MarshalJSON() ([]byte, error) {
	if r.IsZero() {
		return nil, invalid("work_ref", "zero work reference")
	}
	return CanonicalMarshal(struct {
		HomePeerID PeerID `json:"home_peer_id"`
		WorkID     WorkID `json:"work_id"`
	}{r.homePeerID, r.workID})
}

type EventKey struct {
	originPeerID PeerID
	originEpoch  OriginEpoch
	eventID      EventID
}

func NewEventKey(originPeerID PeerID, originEpoch OriginEpoch, eventID EventID) (EventKey, error) {
	if originPeerID.IsZero() || originEpoch.IsZero() || eventID.IsZero() {
		return EventKey{}, invalid("event_key", "origin peer, epoch and Event ID are required")
	}
	return EventKey{originPeerID: originPeerID, originEpoch: originEpoch, eventID: eventID}, nil
}

func (k EventKey) OriginPeerID() PeerID     { return k.originPeerID }
func (k EventKey) OriginEpoch() OriginEpoch { return k.originEpoch }
func (k EventKey) EventID() EventID         { return k.eventID }
func (k EventKey) IsZero() bool {
	return k.originPeerID.IsZero() || k.originEpoch.IsZero() || k.eventID.IsZero()
}
func (k EventKey) key() string {
	return k.originPeerID.String() + "\x00" + k.originEpoch.String() + "\x00" + k.eventID.String()
}

func (k EventKey) MarshalJSON() ([]byte, error) {
	if k.IsZero() {
		return nil, invalid("event_key", "zero Event key")
	}
	return CanonicalMarshal(struct {
		OriginPeerID PeerID      `json:"origin_peer_id"`
		OriginEpoch  OriginEpoch `json:"origin_epoch"`
		EventID      EventID     `json:"event_id"`
	}{k.originPeerID, k.originEpoch, k.eventID})
}

type OriginKey struct {
	originPeerID PeerID
	originEpoch  OriginEpoch
	originSeq    uint64
}

func NewOriginKey(peer PeerID, epoch OriginEpoch, sequence uint64) (OriginKey, error) {
	if peer.IsZero() || epoch.IsZero() {
		return OriginKey{}, invalid("origin_key", "peer, epoch and positive sequence are required")
	}
	if err := validateSQLitePositive("origin sequence", sequence); err != nil {
		return OriginKey{}, err
	}
	return OriginKey{peer, epoch, sequence}, nil
}

func (k OriginKey) OriginPeerID() PeerID     { return k.originPeerID }
func (k OriginKey) OriginEpoch() OriginEpoch { return k.originEpoch }
func (k OriginKey) OriginSequence() uint64   { return k.originSeq }

type PublicationKey struct {
	channelID    ChannelID
	originPeerID PeerID
	originEpoch  OriginEpoch
	channelSeq   uint64
}

func NewPublicationKey(channel ChannelID, peer PeerID, epoch OriginEpoch, sequence uint64) (PublicationKey, error) {
	if channel.IsZero() || peer.IsZero() || epoch.IsZero() {
		return PublicationKey{}, invalid("publication_key", "channel, origin and positive sequence are required")
	}
	if err := validateSQLitePositive("Channel sequence", sequence); err != nil {
		return PublicationKey{}, err
	}
	return PublicationKey{channel, peer, epoch, sequence}, nil
}

func (k PublicationKey) ChannelID() ChannelID     { return k.channelID }
func (k PublicationKey) OriginPeerID() PeerID     { return k.originPeerID }
func (k PublicationKey) OriginEpoch() OriginEpoch { return k.originEpoch }
func (k PublicationKey) ChannelSequence() uint64  { return k.channelSeq }
