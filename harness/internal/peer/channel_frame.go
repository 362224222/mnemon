package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// ChannelFrameVersion is the only envelope version admitted by the R5 T0
	// Channel protocol. Protocol negotiation is exact at ChannelProtocol; the
	// envelope version is still bound explicitly so bytes cannot be reinterpreted.
	ChannelFrameVersion uint8 = model.EnrollmentProtocolMinVersion

	channelFrameLengthBytes = 4
)

var ErrChannelFrame = errors.New("invalid Mnemon Channel frame")

type ChannelFrameType string

const (
	ChannelFrameEnrollInit      ChannelFrameType = "enroll_init"
	ChannelFrameEnrollChallenge ChannelFrameType = "enroll_challenge"
	ChannelFrameEnrollProof     ChannelFrameType = "enroll_proof"
	ChannelFrameEnrollAccepted  ChannelFrameType = "enroll_accepted"
	ChannelFrameProtocolError   ChannelFrameType = "protocol_error"
)

func (frameType ChannelFrameType) Valid() bool {
	switch frameType {
	case ChannelFrameEnrollInit, ChannelFrameEnrollChallenge, ChannelFrameEnrollProof,
		ChannelFrameEnrollAccepted, ChannelFrameProtocolError:
		return true
	default:
		return false
	}
}

// ChannelFramePayload is sealed to the exact payloads supported by the R5 T0
// enrollment exchange. In particular, there is no generic credential-bearing
// map or raw payload constructor.
type ChannelFramePayload interface {
	CanonicalJSON() model.JSON
	channelFrameType() ChannelFrameType
}

type ChannelFrame struct {
	requestID model.EnrollmentRequestID
	frameType ChannelFrameType
	payload   ChannelFramePayload
	canonical model.JSON
}

type channelFrameWire struct {
	Payload   json.RawMessage  `json:"payload"`
	RequestID string           `json:"request_id"`
	Type      ChannelFrameType `json:"type"`
	Version   uint8            `json:"version"`
}

func NewChannelFrame(requestID model.EnrollmentRequestID,
	payload ChannelFramePayload,
) (ChannelFrame, error) {
	if requestID.IsZero() || payload == nil {
		return ChannelFrame{}, channelFrameError("request ID and typed payload are required", nil)
	}
	frameType, canonical, err := canonicalChannelPayload(payload)
	if err != nil {
		return ChannelFrame{}, err
	}
	if accepted, ok := payload.(EnrollAccepted); ok && accepted.JoinReceipt().RequestID() != requestID {
		return ChannelFrame{}, channelFrameError("accepted receipt does not match envelope request ID", nil)
	}
	wire, err := model.JSONFrom(channelFrameWire{Payload: canonical.Bytes(),
		RequestID: requestID.String(), Type: frameType, Version: ChannelFrameVersion})
	if err != nil {
		return ChannelFrame{}, channelFrameError("encode canonical envelope", err)
	}
	if len(wire.Bytes()) > maxChannelFrameBytes() {
		return ChannelFrame{}, channelFrameError("canonical envelope exceeds direct frame limit", nil)
	}
	return ChannelFrame{requestID: requestID, frameType: frameType,
		payload: payload, canonical: wire}, nil
}

func canonicalChannelPayload(payload ChannelFramePayload) (ChannelFrameType, model.JSON, error) {
	var frameType ChannelFrameType
	switch value := payload.(type) {
	case EnrollInit:
		frameType = ChannelFrameEnrollInit
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero EnrollInit payload", nil)
		}
	case EnrollChallenge:
		frameType = ChannelFrameEnrollChallenge
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero EnrollChallenge payload", nil)
		}
	case EnrollProof:
		frameType = ChannelFrameEnrollProof
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero EnrollProof payload", nil)
		}
	case EnrollAccepted:
		frameType = ChannelFrameEnrollAccepted
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero EnrollAccepted payload", nil)
		}
	case ProtocolError:
		frameType = ChannelFrameProtocolError
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero ProtocolError payload", nil)
		}
	default:
		return "", model.JSON{}, channelFrameError("unknown Channel payload implementation", nil)
	}
	if payload.channelFrameType() != frameType || payload.CanonicalJSON().IsZero() {
		return "", model.JSON{}, channelFrameError("payload type or canonical bytes are inconsistent", nil)
	}
	return frameType, payload.CanonicalJSON(), nil
}

// ParseChannelFrame parses one unprefixed envelope. It admits only exact
// canonical JSON and rebuilds the selected typed payload before returning it.
func ParseChannelFrame(raw []byte) (ChannelFrame, error) {
	if len(raw) == 0 || len(raw) > maxChannelFrameBytes() {
		return ChannelFrame{}, channelFrameError("empty or oversized envelope", nil)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return ChannelFrame{}, channelFrameError("envelope must be exact canonical JSON", err)
	}
	var wire channelFrameWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return ChannelFrame{}, err
	}
	if wire.Version != ChannelFrameVersion || !wire.Type.Valid() {
		return ChannelFrame{}, channelFrameError("unsupported version or frame type", nil)
	}
	requestID, err := model.ParseEnrollmentRequestID(wire.RequestID)
	if err != nil {
		return ChannelFrame{}, channelFrameError("invalid request ID", err)
	}
	payload, err := parseChannelPayload(wire.Type, wire.Payload)
	if err != nil {
		return ChannelFrame{}, err
	}
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		return ChannelFrame{}, err
	}
	if frame.frameType != wire.Type || !bytes.Equal(frame.canonical.Bytes(), raw) {
		return ChannelFrame{}, channelFrameError("envelope bytes are not canonical", nil)
	}
	return frame, nil
}

func parseChannelPayload(frameType ChannelFrameType, raw []byte) (ChannelFramePayload, error) {
	switch frameType {
	case ChannelFrameEnrollInit:
		return parseEnrollInit(raw)
	case ChannelFrameEnrollChallenge:
		return parseEnrollChallenge(raw)
	case ChannelFrameEnrollProof:
		return parseEnrollProof(raw)
	case ChannelFrameEnrollAccepted:
		return parseEnrollAccepted(raw)
	case ChannelFrameProtocolError:
		return parseProtocolError(raw)
	default:
		return nil, channelFrameError("unknown frame type", nil)
	}
}

// ReadChannelFrame reads one uint32 big-endian length-prefixed envelope. The
// size fence is checked before payload allocation or reads.
func ReadChannelFrame(reader io.Reader) (ChannelFrame, error) {
	if reader == nil {
		return ChannelFrame{}, channelFrameError("reader is required", nil)
	}
	var prefix [channelFrameLengthBytes]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return ChannelFrame{}, channelFrameError("read length prefix", err)
	}
	length := uint64(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 || length > uint64(maxChannelFrameBytes()) {
		return ChannelFrame{}, channelFrameError("declared length is empty or exceeds direct frame limit", nil)
	}
	raw := make([]byte, int(length))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return ChannelFrame{}, channelFrameError("read declared envelope", err)
	}
	return ParseChannelFrame(raw)
}

// WriteChannelFrame writes one complete uint32 big-endian length-prefixed
// envelope, detecting short writers instead of emitting a partial success.
func WriteChannelFrame(writer io.Writer, frame ChannelFrame) error {
	if writer == nil || frame.IsZero() {
		return channelFrameError("writer and complete frame are required", nil)
	}
	raw := frame.canonical.Bytes()
	if len(raw) == 0 || len(raw) > maxChannelFrameBytes() || len(raw) > int(^uint32(0)) {
		return channelFrameError("canonical envelope exceeds length prefix or direct frame limit", nil)
	}
	var prefix [channelFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if err := writeFull(writer, prefix[:]); err != nil {
		return channelFrameError("write length prefix", err)
	}
	if err := writeFull(writer, raw); err != nil {
		return channelFrameError("write canonical envelope", err)
	}
	return nil
}

func writeFull(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if written < 0 || written > len(value) {
			return io.ErrShortWrite
		}
		value = value[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func maxChannelFrameBytes() int { return HermeticLimits().DirectFrameBytes }

func (frame ChannelFrame) Version() uint8                       { return ChannelFrameVersion }
func (frame ChannelFrame) Type() ChannelFrameType               { return frame.frameType }
func (frame ChannelFrame) RequestID() model.EnrollmentRequestID { return frame.requestID }
func (frame ChannelFrame) Payload() ChannelFramePayload         { return frame.payload }
func (frame ChannelFrame) CanonicalJSON() model.JSON            { return frame.canonical }
func (frame ChannelFrame) IsZero() bool {
	return frame.requestID.IsZero() || !frame.frameType.Valid() || frame.payload == nil ||
		frame.canonical.IsZero()
}

type EnrollInitSpec struct {
	ChannelID            model.ChannelID
	GrantID              model.GrantID
	JoinerNonce          []byte
	SupportedVersions    []uint8
	OriginEpoch          model.OriginEpoch
	DisplayLabel         string
	AdvertisedMultiaddrs []string
}

type EnrollInit struct {
	channelID            model.ChannelID
	grantID              model.GrantID
	joinerNonce          string
	supportedVersions    []uint8
	originEpoch          model.OriginEpoch
	displayLabel         string
	advertisedMultiaddrs []string
	canonical            model.JSON
}

type enrollInitWire struct {
	AdvertisedMultiaddrs []string `json:"advertised_addrs"`
	ChannelID            string   `json:"channel_id"`
	DisplayLabel         string   `json:"display_label"`
	GrantID              string   `json:"grant_id"`
	JoinerNonce          []byte   `json:"joiner_nonce"`
	OriginEpoch          string   `json:"origin_epoch"`
	SupportedVersions    []uint16 `json:"supported_versions"`
}

func NewEnrollInit(spec EnrollInitSpec) (EnrollInit, error) {
	if spec.ChannelID.IsZero() || spec.GrantID.IsZero() || spec.OriginEpoch.IsZero() ||
		len(spec.JoinerNonce) != model.EnrollmentNonceBytes {
		return EnrollInit{}, channelFrameError("EnrollInit requires Channel, grant, epoch and a 32-byte nonce", nil)
	}
	if len(spec.SupportedVersions) == 0 || len(spec.SupportedVersions) > 8 {
		return EnrollInit{}, channelFrameError("EnrollInit supported_versions is outside the protocol bound", nil)
	}
	versions := append([]uint8(nil), spec.SupportedVersions...)
	for index, version := range versions {
		if version == 0 || (index != 0 && versions[index-1] >= version) {
			return EnrollInit{}, channelFrameError("EnrollInit supported_versions must be sorted, unique and nonzero", nil)
		}
	}
	if err := validateFrameLabel(spec.DisplayLabel); err != nil {
		return EnrollInit{}, err
	}
	addresses, err := normalizeFrameMultiaddrs(spec.AdvertisedMultiaddrs)
	if err != nil {
		return EnrollInit{}, err
	}
	nonce := append([]byte(nil), spec.JoinerNonce...)
	wireVersions := make([]uint16, len(versions))
	for index, version := range versions {
		wireVersions[index] = uint16(version)
	}
	canonical, err := model.JSONFrom(enrollInitWire{AdvertisedMultiaddrs: addresses,
		ChannelID: spec.ChannelID.String(), DisplayLabel: spec.DisplayLabel,
		GrantID: spec.GrantID.String(), JoinerNonce: nonce,
		OriginEpoch: spec.OriginEpoch.String(), SupportedVersions: wireVersions})
	if err != nil {
		return EnrollInit{}, channelFrameError("encode EnrollInit", err)
	}
	return EnrollInit{channelID: spec.ChannelID, grantID: spec.GrantID, joinerNonce: string(nonce),
		supportedVersions: versions, originEpoch: spec.OriginEpoch, displayLabel: spec.DisplayLabel,
		advertisedMultiaddrs: addresses, canonical: canonical}, nil
}

func parseEnrollInit(raw []byte) (EnrollInit, error) {
	var wire enrollInitWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return EnrollInit{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	grantID, grantErr := model.ParseGrantID(wire.GrantID)
	epoch, epochErr := model.ParseOriginEpoch(wire.OriginEpoch)
	if channelErr != nil || grantErr != nil || epochErr != nil || len(wire.SupportedVersions) == 0 ||
		len(wire.SupportedVersions) > 8 {
		return EnrollInit{}, channelFrameError("invalid EnrollInit identifiers or versions",
			errors.Join(channelErr, grantErr, epochErr))
	}
	versions := make([]uint8, len(wire.SupportedVersions))
	for index, version := range wire.SupportedVersions {
		if version == 0 || version > 255 || (index != 0 && wire.SupportedVersions[index-1] >= version) {
			return EnrollInit{}, channelFrameError("invalid EnrollInit identifiers or versions", nil)
		}
		versions[index] = uint8(version)
	}
	payload, err := NewEnrollInit(EnrollInitSpec{ChannelID: channelID, GrantID: grantID,
		JoinerNonce:       wire.JoinerNonce,
		SupportedVersions: versions, OriginEpoch: epoch,
		DisplayLabel: wire.DisplayLabel, AdvertisedMultiaddrs: wire.AdvertisedMultiaddrs})
	if err != nil {
		return EnrollInit{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EnrollInit{}, channelFrameError("EnrollInit bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload EnrollInit) ChannelID() model.ChannelID { return payload.channelID }
func (payload EnrollInit) GrantID() model.GrantID     { return payload.grantID }
func (payload EnrollInit) JoinerNonce() []byte        { return []byte(payload.joinerNonce) }
func (payload EnrollInit) SupportedVersions() []uint8 {
	return append([]uint8(nil), payload.supportedVersions...)
}
func (payload EnrollInit) OriginEpoch() model.OriginEpoch { return payload.originEpoch }
func (payload EnrollInit) DisplayLabel() string           { return payload.displayLabel }
func (payload EnrollInit) AdvertisedMultiaddrs() []string {
	return append([]string(nil), payload.advertisedMultiaddrs...)
}
func (payload EnrollInit) CanonicalJSON() model.JSON { return payload.canonical }
func (payload EnrollInit) IsZero() bool {
	return payload.channelID.IsZero() || payload.grantID.IsZero() || payload.canonical.IsZero()
}
func (EnrollInit) channelFrameType() ChannelFrameType { return ChannelFrameEnrollInit }

type EnrollChallengeSpec struct {
	OwnerNonce      []byte
	SelectedVersion uint8
	Limits          model.JSON
	RosterHead      model.RecordHead
}

type EnrollChallenge struct {
	ownerNonce      string
	selectedVersion uint8
	limits          model.JSON
	rosterHead      model.RecordHead
	canonical       model.JSON
}

type enrollChallengeWire struct {
	Limits          json.RawMessage `json:"limits"`
	OwnerNonce      []byte          `json:"owner_nonce"`
	RosterHead      recordHeadWire  `json:"roster_head"`
	SelectedVersion uint8           `json:"selected_version"`
}

type recordHeadWire struct {
	Digest   string `json:"digest"`
	Revision uint64 `json:"revision"`
}

func NewEnrollChallenge(spec EnrollChallengeSpec) (EnrollChallenge, error) {
	if len(spec.OwnerNonce) != model.EnrollmentNonceBytes ||
		spec.SelectedVersion != ChannelFrameVersion || spec.RosterHead.IsZero() ||
		spec.Limits.String() != model.DefaultMemberLimits().String() {
		return EnrollChallenge{}, channelFrameError("EnrollChallenge requires exact nonce, version, limits and roster head", nil)
	}
	nonce := append([]byte(nil), spec.OwnerNonce...)
	canonical, err := model.JSONFrom(enrollChallengeWire{Limits: spec.Limits.Bytes(), OwnerNonce: nonce,
		RosterHead: recordHeadWire{Digest: spec.RosterHead.Digest().String(),
			Revision: spec.RosterHead.Revision()}, SelectedVersion: spec.SelectedVersion})
	if err != nil {
		return EnrollChallenge{}, channelFrameError("encode EnrollChallenge", err)
	}
	return EnrollChallenge{ownerNonce: string(nonce), selectedVersion: spec.SelectedVersion,
		limits: spec.Limits, rosterHead: spec.RosterHead, canonical: canonical}, nil
}

func parseEnrollChallenge(raw []byte) (EnrollChallenge, error) {
	var wire enrollChallengeWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return EnrollChallenge{}, err
	}
	limits, limitsErr := model.NewJSON(wire.Limits)
	digest, digestErr := model.ParseDigest(wire.RosterHead.Digest)
	head, headErr := model.NewRecordHead(wire.RosterHead.Revision, digest)
	if limitsErr != nil || digestErr != nil || headErr != nil {
		return EnrollChallenge{}, channelFrameError("invalid EnrollChallenge limits or roster head",
			errors.Join(limitsErr, digestErr, headErr))
	}
	payload, err := NewEnrollChallenge(EnrollChallengeSpec{OwnerNonce: wire.OwnerNonce,
		SelectedVersion: wire.SelectedVersion, Limits: limits, RosterHead: head})
	if err != nil {
		return EnrollChallenge{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EnrollChallenge{}, channelFrameError("EnrollChallenge bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload EnrollChallenge) OwnerNonce() []byte           { return []byte(payload.ownerNonce) }
func (payload EnrollChallenge) SelectedVersion() uint8       { return payload.selectedVersion }
func (payload EnrollChallenge) Limits() model.JSON           { return payload.limits }
func (payload EnrollChallenge) RosterHead() model.RecordHead { return payload.rosterHead }
func (payload EnrollChallenge) CanonicalJSON() model.JSON    { return payload.canonical }
func (payload EnrollChallenge) IsZero() bool {
	return payload.rosterHead.IsZero() || payload.canonical.IsZero()
}
func (EnrollChallenge) channelFrameType() ChannelFrameType { return ChannelFrameEnrollChallenge }

type EnrollProof struct {
	proof     model.Digest
	canonical model.JSON
}

type enrollProofWire struct {
	Proof string `json:"proof"`
}

func NewEnrollProof(proof model.Digest) (EnrollProof, error) {
	if proof.IsZero() {
		return EnrollProof{}, channelFrameError("EnrollProof requires a nonzero transcript proof", nil)
	}
	canonical, err := model.JSONFrom(enrollProofWire{Proof: proof.String()})
	if err != nil {
		return EnrollProof{}, channelFrameError("encode EnrollProof", err)
	}
	return EnrollProof{proof: proof, canonical: canonical}, nil
}

func parseEnrollProof(raw []byte) (EnrollProof, error) {
	var wire enrollProofWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return EnrollProof{}, err
	}
	proof, err := model.ParseDigest(wire.Proof)
	if err != nil {
		return EnrollProof{}, channelFrameError("invalid EnrollProof digest", err)
	}
	payload, err := NewEnrollProof(proof)
	if err != nil {
		return EnrollProof{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EnrollProof{}, channelFrameError("EnrollProof bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload EnrollProof) Proof() model.Digest        { return payload.proof }
func (payload EnrollProof) CanonicalJSON() model.JSON  { return payload.canonical }
func (payload EnrollProof) IsZero() bool               { return payload.proof.IsZero() || payload.canonical.IsZero() }
func (EnrollProof) channelFrameType() ChannelFrameType { return ChannelFrameEnrollProof }

type EnrollAccepted struct {
	status    ChannelEnrollmentStatus
	member    model.Member
	roster    []model.Member
	receipt   model.EnrollmentReceipt
	canonical model.JSON
}

type enrollAcceptedWire struct {
	JoinReceipt    json.RawMessage         `json:"join_receipt"`
	MemberRecord   json.RawMessage         `json:"member_record"`
	RosterSnapshot []json.RawMessage       `json:"roster_snapshot"`
	Status         ChannelEnrollmentStatus `json:"status"`
}

type ChannelEnrollmentStatus string

const (
	ChannelEnrollmentAccepted      ChannelEnrollmentStatus = "accepted"
	ChannelEnrollmentReplayed      ChannelEnrollmentStatus = "replayed"
	ChannelEnrollmentMemberRevoked ChannelEnrollmentStatus = "member_revoked"
	ChannelEnrollmentChannelClosed ChannelEnrollmentStatus = "channel_closed"
)

func (status ChannelEnrollmentStatus) Valid() bool {
	switch status {
	case ChannelEnrollmentAccepted, ChannelEnrollmentReplayed,
		ChannelEnrollmentMemberRevoked, ChannelEnrollmentChannelClosed:
		return true
	default:
		return false
	}
}

// NewEnrollAccepted carries the historical joining MemberRecord bound by the
// receipt plus the latest complete roster. Terminal status never substitutes a
// later record for that immutable enrollment evidence.
func NewEnrollAccepted(status ChannelEnrollmentStatus, member model.Member, roster []model.Member,
	receipt model.EnrollmentReceipt,
) (EnrollAccepted, error) {
	if !status.Valid() || member.IsZero() || receipt.IsZero() || len(roster) == 0 ||
		len(roster) > model.MaxMemberRecordsPerChannel ||
		receipt.ChannelID() != member.ChannelID() || receipt.MemberPeerID() != member.PeerID() ||
		receipt.MemberHead() != member.Head() || member.Status() != model.MemberActive {
		return EnrollAccepted{}, channelFrameError("EnrollAccepted requires matching member, receipt and roster", nil)
	}
	copyRoster := append([]model.Member(nil), roster...)
	rosterWire := make([]json.RawMessage, len(copyRoster))
	foundMember := false
	var currentJoiningMember model.Member
	var currentOwner model.Member
	var previous model.Member
	ownerPeerID := copyRoster[0].PeerID()
	for index, item := range copyRoster {
		if item.IsZero() || item.Head().Revision() != uint64(index+1) {
			return EnrollAccepted{}, channelFrameError("EnrollAccepted roster must be a complete contiguous prefix", nil)
		}
		if item.ChannelID() != member.ChannelID() || item.DescriptorDigest() != member.DescriptorDigest() {
			return EnrollAccepted{}, channelFrameError("EnrollAccepted roster crosses Channel authority", nil)
		}
		previousDigest, hasPrevious := item.PreviousDigest()
		if index == 0 {
			if hasPrevious {
				return EnrollAccepted{}, channelFrameError("EnrollAccepted roster genesis has a predecessor", nil)
			}
		} else if !hasPrevious || previousDigest != previous.Head().Digest() {
			return EnrollAccepted{}, channelFrameError("EnrollAccepted roster digest chain is discontinuous", nil)
		}
		if item.Head() == member.Head() {
			if !bytes.Equal(item.WireJSON().Bytes(), member.WireJSON().Bytes()) {
				return EnrollAccepted{}, channelFrameError("EnrollAccepted member head has different roster bytes", nil)
			}
			foundMember = true
		}
		if item.PeerID() == member.PeerID() {
			currentJoiningMember = item
		}
		if item.PeerID() == ownerPeerID {
			currentOwner = item
		}
		rosterWire[index] = item.WireJSON().Bytes()
		previous = item
	}
	if !foundMember {
		return EnrollAccepted{}, channelFrameError("EnrollAccepted roster omits joining member", nil)
	}
	if currentJoiningMember.IsZero() || currentOwner.IsZero() ||
		currentOwner.Status() == model.MemberRevoked {
		return EnrollAccepted{}, channelFrameError("EnrollAccepted roster has no valid current owner or joining member", nil)
	}
	joinerTerminal := currentJoiningMember.Status().Terminal()
	ownerClosed := currentOwner.Status() == model.MemberLeft
	if (status == ChannelEnrollmentMemberRevoked) != joinerTerminal ||
		(status == ChannelEnrollmentChannelClosed) != (ownerClosed && !joinerTerminal) ||
		((status == ChannelEnrollmentAccepted || status == ChannelEnrollmentReplayed) && ownerClosed) {
		return EnrollAccepted{}, channelFrameError("EnrollAccepted status disagrees with joining member lifecycle", nil)
	}
	canonical, err := model.JSONFrom(enrollAcceptedWire{JoinReceipt: receipt.WireJSON().Bytes(),
		MemberRecord: member.WireJSON().Bytes(), RosterSnapshot: rosterWire, Status: status})
	if err != nil {
		return EnrollAccepted{}, channelFrameError("encode EnrollAccepted", err)
	}
	return EnrollAccepted{status: status, member: member, roster: copyRoster, receipt: receipt,
		canonical: canonical}, nil
}

func parseEnrollAccepted(raw []byte) (EnrollAccepted, error) {
	var wire enrollAcceptedWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return EnrollAccepted{}, err
	}
	member, memberErr := model.ParseMember(wire.MemberRecord)
	receipt, receiptErr := model.ParseEnrollmentReceipt(wire.JoinReceipt)
	if memberErr != nil || receiptErr != nil || len(wire.RosterSnapshot) == 0 {
		return EnrollAccepted{}, channelFrameError("invalid EnrollAccepted member, receipt or roster",
			errors.Join(memberErr, receiptErr))
	}
	roster := make([]model.Member, len(wire.RosterSnapshot))
	for index, encoded := range wire.RosterSnapshot {
		parsed, err := model.ParseMember(encoded)
		if err != nil {
			return EnrollAccepted{}, channelFrameError(fmt.Sprintf("invalid EnrollAccepted roster revision %d", index+1), err)
		}
		roster[index] = parsed
	}
	payload, err := NewEnrollAccepted(wire.Status, member, roster, receipt)
	if err != nil {
		return EnrollAccepted{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EnrollAccepted{}, channelFrameError("EnrollAccepted bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload EnrollAccepted) Status() ChannelEnrollmentStatus { return payload.status }
func (payload EnrollAccepted) MemberRecord() model.Member      { return payload.member }
func (payload EnrollAccepted) RosterSnapshot() []model.Member {
	return append([]model.Member(nil), payload.roster...)
}
func (payload EnrollAccepted) JoinReceipt() model.EnrollmentReceipt { return payload.receipt }
func (payload EnrollAccepted) CanonicalJSON() model.JSON            { return payload.canonical }
func (payload EnrollAccepted) IsZero() bool {
	return !payload.status.Valid() || payload.member.IsZero() || payload.receipt.IsZero() || len(payload.roster) == 0 ||
		payload.canonical.IsZero()
}
func (EnrollAccepted) channelFrameType() ChannelFrameType { return ChannelFrameEnrollAccepted }

type ChannelProtocolErrorCode string

const (
	ChannelErrorOwnerUnreachable     ChannelProtocolErrorCode = "owner_unreachable"
	ChannelErrorBusy                 ChannelProtocolErrorCode = "busy"
	ChannelErrorInvalidToken         ChannelProtocolErrorCode = "invalid_token"
	ChannelErrorWrongOwner           ChannelProtocolErrorCode = "wrong_owner"
	ChannelErrorTokenExpired         ChannelProtocolErrorCode = "token_expired"
	ChannelErrorTokenClosed          ChannelProtocolErrorCode = "token_closed"
	ChannelErrorTokenExhausted       ChannelProtocolErrorCode = "token_exhausted"
	ChannelErrorChannelFull          ChannelProtocolErrorCode = "channel_full"
	ChannelErrorNodeChannelLimit     ChannelProtocolErrorCode = "node_channel_limit"
	ChannelErrorBadProof             ChannelProtocolErrorCode = "bad_proof"
	ChannelErrorIncompatibleProtocol ChannelProtocolErrorCode = "incompatible_protocol"
	ChannelErrorNotMember            ChannelProtocolErrorCode = "not_member"
	ChannelErrorMemberRevoked        ChannelProtocolErrorCode = "member_revoked"
	ChannelErrorChannelClosed        ChannelProtocolErrorCode = "channel_closed"
	ChannelErrorBaselineConflict     ChannelProtocolErrorCode = "baseline_conflict"
	ChannelErrorOriginEpochMismatch  ChannelProtocolErrorCode = "origin_epoch_mismatch"
	ChannelErrorRosterGap            ChannelProtocolErrorCode = "roster_gap"
	ChannelErrorRosterConflict       ChannelProtocolErrorCode = "roster_conflict"
)

func (code ChannelProtocolErrorCode) Valid() bool {
	switch code {
	case ChannelErrorOwnerUnreachable, ChannelErrorBusy, ChannelErrorInvalidToken,
		ChannelErrorWrongOwner, ChannelErrorTokenExpired, ChannelErrorTokenClosed,
		ChannelErrorTokenExhausted, ChannelErrorChannelFull, ChannelErrorNodeChannelLimit,
		ChannelErrorBadProof, ChannelErrorIncompatibleProtocol, ChannelErrorNotMember,
		ChannelErrorMemberRevoked, ChannelErrorChannelClosed, ChannelErrorBaselineConflict,
		ChannelErrorOriginEpochMismatch, ChannelErrorRosterGap, ChannelErrorRosterConflict:
		return true
	default:
		return false
	}
}

func (code ChannelProtocolErrorCode) retryable() bool {
	return code == ChannelErrorOwnerUnreachable || code == ChannelErrorBusy || code == ChannelErrorRosterGap
}

type ProtocolErrorSpec struct {
	Code       ChannelProtocolErrorCode
	Retryable  bool
	RetryAfter time.Duration
}

type ProtocolError struct {
	code       ChannelProtocolErrorCode
	retryable  bool
	retryAfter time.Duration
	canonical  model.JSON
}

type protocolErrorWire struct {
	Code       ChannelProtocolErrorCode `json:"code"`
	RetryAfter int64                    `json:"retry_after"`
	Retryable  bool                     `json:"retryable"`
}

// NewProtocolError uses integer milliseconds on the wire. Retry policy is
// part of each stable code: permanent errors cannot be relabeled retryable.
func NewProtocolError(spec ProtocolErrorSpec) (ProtocolError, error) {
	if !spec.Code.Valid() || spec.Retryable != spec.Code.retryable() || spec.RetryAfter < 0 ||
		spec.RetryAfter%time.Millisecond != 0 ||
		(spec.Retryable && spec.RetryAfter == 0) || (!spec.Retryable && spec.RetryAfter != 0) {
		return ProtocolError{}, channelFrameError("ProtocolError code, retry policy or retry_after is invalid", nil)
	}
	milliseconds := spec.RetryAfter.Milliseconds()
	canonical, err := model.JSONFrom(protocolErrorWire{Code: spec.Code,
		RetryAfter: milliseconds, Retryable: spec.Retryable})
	if err != nil {
		return ProtocolError{}, channelFrameError("encode ProtocolError", err)
	}
	return ProtocolError{code: spec.Code, retryable: spec.Retryable,
		retryAfter: spec.RetryAfter, canonical: canonical}, nil
}

func parseProtocolError(raw []byte) (ProtocolError, error) {
	var wire protocolErrorWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return ProtocolError{}, err
	}
	if wire.RetryAfter < 0 || wire.RetryAfter > int64((time.Duration(1<<63-1))/time.Millisecond) {
		return ProtocolError{}, channelFrameError("ProtocolError retry_after is out of range", nil)
	}
	payload, err := NewProtocolError(ProtocolErrorSpec{Code: wire.Code, Retryable: wire.Retryable,
		RetryAfter: time.Duration(wire.RetryAfter) * time.Millisecond})
	if err != nil {
		return ProtocolError{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return ProtocolError{}, channelFrameError("ProtocolError bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload ProtocolError) Code() ChannelProtocolErrorCode { return payload.code }
func (payload ProtocolError) Retryable() bool                { return payload.retryable }
func (payload ProtocolError) RetryAfter() time.Duration      { return payload.retryAfter }
func (payload ProtocolError) CanonicalJSON() model.JSON      { return payload.canonical }
func (payload ProtocolError) IsZero() bool {
	return !payload.code.Valid() || payload.canonical.IsZero()
}
func (ProtocolError) channelFrameType() ChannelFrameType { return ChannelFrameProtocolError }

func decodeExactFrameJSON(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > maxChannelFrameBytes() {
		return channelFrameError("empty or oversized canonical value", nil)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return channelFrameError("wire value must be exact canonical JSON", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return channelFrameError("decode exact wire value", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return channelFrameError("wire value contains a trailing value", err)
	}
	return nil
}

func validateFrameLabel(value string) error {
	if !utf8.ValidString(value) || value == "" || len(value) > model.MaxLabelBytes ||
		strings.TrimSpace(value) != value {
		return channelFrameError("display label must be a bounded canonical single line", nil)
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return channelFrameError("display label contains a forbidden control character", nil)
		}
	}
	return nil
}

func normalizeFrameMultiaddrs(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > model.MaxMemberMultiaddrs {
		return nil, channelFrameError("advertised_addrs count is outside the Channel bound", nil)
	}
	result := append([]string(nil), values...)
	for _, value := range result {
		if value == "" || len(value) > model.MaxIdentifierBytes || strings.IndexFunc(value, func(character rune) bool {
			return character <= 0x20 || character == 0x7f
		}) >= 0 {
			return nil, channelFrameError("advertised_addrs contains an invalid value", nil)
		}
		address, err := ma.NewMultiaddr(value)
		if err != nil || address.String() != value {
			return nil, channelFrameError("advertised_addrs must contain canonical libp2p multiaddrs", err)
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, channelFrameError("advertised_addrs contains a duplicate", nil)
		}
	}
	return result, nil
}

func channelFrameError(detail string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrChannelFrame, detail)
	}
	return fmt.Errorf("%w: %s: %w", ErrChannelFrame, detail, cause)
}
