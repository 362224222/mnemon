package store

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type gossipPublicationFenceWire struct {
	Attempt    uint32 `json:"attempt"`
	ChannelID  string `json:"channel_id"`
	EventID    string `json:"event_id"`
	LeaseOwner string `json:"lease_owner"`
	LeaseUntil string `json:"lease_until"`
	RosterHead struct {
		Digest   string `json:"digest"`
		Revision uint64 `json:"revision"`
	} `json:"roster_head"`
}

func canonicalGossipPublicationFence(fence GossipPublicationFence) (model.JSON, error) {
	if fence.EventID.IsZero() || fence.ChannelID.IsZero() || fence.Attempt == 0 ||
		!validPublicationIdentifier(fence.LeaseOwner) || fence.RosterHead.IsZero() {
		return model.JSON{}, ErrGossipPublicationInput
	}
	leaseUntil, err := canonicalStoreTime(fence.LeaseUntil)
	if err != nil {
		return model.JSON{}, fmt.Errorf("%w: fence lease time: %v", ErrGossipPublicationInput, err)
	}
	wire := gossipPublicationFenceWire{Attempt: fence.Attempt,
		ChannelID: fence.ChannelID.String(), EventID: fence.EventID.String(),
		LeaseOwner: fence.LeaseOwner, LeaseUntil: storeTime(leaseUntil)}
	wire.RosterHead.Digest = fence.RosterHead.Digest().String()
	wire.RosterHead.Revision = fence.RosterHead.Revision()
	value, err := model.JSONFrom(wire)
	if err != nil {
		return model.JSON{}, fmt.Errorf("%w: encode canonical fence: %v", ErrGossipPublicationInvariant, err)
	}
	return value, nil
}

func parseGossipPublicationFence(raw []byte) (GossipPublicationFence, model.JSON, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: noncanonical lease fence",
			ErrGossipPublicationInvariant)
	}
	var wire gossipPublicationFenceWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: decode lease fence: %v",
			ErrGossipPublicationInvariant, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: decode lease fence: %v",
			ErrGossipPublicationInvariant, err)
	}
	eventID, err := model.ParseEventID(wire.EventID)
	if err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: lease fence Event: %v",
			ErrGossipPublicationInvariant, err)
	}
	channelID, err := model.ParseChannelID(wire.ChannelID)
	if err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: lease fence Channel: %v",
			ErrGossipPublicationInvariant, err)
	}
	leaseUntil, err := parseCanonicalStoreTime(wire.LeaseUntil)
	if err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: lease fence time: %v",
			ErrGossipPublicationInvariant, err)
	}
	digest, err := model.ParseDigest(wire.RosterHead.Digest)
	if err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: lease fence roster digest: %v",
			ErrGossipPublicationInvariant, err)
	}
	rosterHead, err := model.NewRecordHead(wire.RosterHead.Revision, digest)
	if err != nil {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: lease fence roster head: %v",
			ErrGossipPublicationInvariant, err)
	}
	fence := GossipPublicationFence{EventID: eventID, ChannelID: channelID,
		LeaseOwner: wire.LeaseOwner, Attempt: wire.Attempt, LeaseUntil: leaseUntil,
		RosterHead: rosterHead}
	rebuilt, err := canonicalGossipPublicationFence(fence)
	if err != nil || !bytes.Equal(rebuilt.Bytes(), raw) {
		return GossipPublicationFence{}, model.JSON{}, fmt.Errorf("%w: lease fence projection mismatch: %v",
			ErrGossipPublicationInvariant, err)
	}
	return fence, rebuilt, nil
}
