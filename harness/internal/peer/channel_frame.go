package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
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

	channelFrameLengthBytes    = 4
	channelRequestIDBytes      = 16
	channelRequestIDPrefix     = "channel-request-"
	channelSmallFrameBytes     = model.MaxChannelRecordBytes
	channelSyncPageFrameBytes  = 1 << 20
	channelRosterFrameBytes    = model.MaxCanonicalJSONBytes
	channelSyncPageRecordLimit = 16
)

var ErrChannelFrame = errors.New("invalid Mnemon Channel frame")

type ChannelFrameType string

const (
	ChannelFrameEnrollInit      ChannelFrameType = "enroll_init"
	ChannelFrameEnrollChallenge ChannelFrameType = "enroll_challenge"
	ChannelFrameEnrollProof     ChannelFrameType = "enroll_proof"
	ChannelFrameEnrollAccepted  ChannelFrameType = "enroll_accepted"
	ChannelFrameMemberHello     ChannelFrameType = "member_hello"
	ChannelFrameMemberHelloAck  ChannelFrameType = "member_hello_ack"
	ChannelFrameSyncRequest     ChannelFrameType = "sync_request"
	ChannelFrameSyncPage        ChannelFrameType = "sync_page"
	ChannelFrameDataBaseline    ChannelFrameType = "data_baseline"
	ChannelFrameDataBaselineAck ChannelFrameType = "data_baseline_ack"
	ChannelFrameProtocolError   ChannelFrameType = "protocol_error"
)

func (frameType ChannelFrameType) Valid() bool {
	switch frameType {
	case ChannelFrameEnrollInit, ChannelFrameEnrollChallenge, ChannelFrameEnrollProof,
		ChannelFrameEnrollAccepted, ChannelFrameMemberHello, ChannelFrameMemberHelloAck,
		ChannelFrameSyncRequest, ChannelFrameSyncPage, ChannelFrameDataBaseline,
		ChannelFrameDataBaselineAck, ChannelFrameProtocolError:
		return true
	default:
		return false
	}
}

// ChannelFramePayload is sealed to the exact control messages supported by the
// R5 T0 Channel protocol. There is no generic credential-bearing map or raw
// payload constructor.
type ChannelFramePayload interface {
	CanonicalJSON() model.JSON
	channelFrameType() ChannelFrameType
}

type ChannelFrame struct {
	requestID ChannelRequestID
	frameType ChannelFrameType
	payload   ChannelFramePayload
	canonical model.JSON
}

// ChannelRequestID correlates frames on one transport stream. It is deliberately
// transient: durable operation identity belongs to the typed payload, such as
// EnrollInit.EnrollmentRequestID, and a retry uses a fresh ChannelRequestID.
type ChannelRequestID struct{ value string }

func NewChannelRequestID(random io.Reader) (ChannelRequestID, error) {
	if random == nil {
		return ChannelRequestID{}, channelFrameError("Channel request ID entropy is required", nil)
	}
	value := make([]byte, channelRequestIDBytes)
	if _, err := io.ReadFull(random, value); err != nil {
		return ChannelRequestID{}, channelFrameError("read Channel request ID entropy", err)
	}
	return ParseChannelRequestID(channelRequestIDPrefix + hex.EncodeToString(value))
}

func ParseChannelRequestID(value string) (ChannelRequestID, error) {
	hexValue := strings.TrimPrefix(value, channelRequestIDPrefix)
	if len(value) != len(channelRequestIDPrefix)+hex.EncodedLen(channelRequestIDBytes) ||
		len(hexValue) != hex.EncodedLen(channelRequestIDBytes) || strings.ToLower(hexValue) != hexValue {
		return ChannelRequestID{}, channelFrameError("invalid Channel request ID", nil)
	}
	decoded, err := hex.DecodeString(hexValue)
	if err != nil || len(decoded) != channelRequestIDBytes {
		return ChannelRequestID{}, channelFrameError("invalid Channel request ID", err)
	}
	return ChannelRequestID{value: value}, nil
}

func (requestID ChannelRequestID) String() string { return requestID.value }
func (requestID ChannelRequestID) IsZero() bool   { return requestID.value == "" }

type channelFrameWire struct {
	Payload   json.RawMessage  `json:"payload"`
	RequestID string           `json:"request_id"`
	Type      ChannelFrameType `json:"type"`
	Version   uint8            `json:"version"`
}

func NewChannelFrame(requestID ChannelRequestID,
	payload ChannelFramePayload,
) (ChannelFrame, error) {
	if requestID.IsZero() || payload == nil {
		return ChannelFrame{}, channelFrameError("request ID and typed payload are required", nil)
	}
	frameType, canonical, err := canonicalChannelPayload(payload)
	if err != nil {
		return ChannelFrame{}, err
	}
	wire, err := model.JSONFrom(channelFrameWire{Payload: canonical.Bytes(),
		RequestID: requestID.String(), Type: frameType, Version: ChannelFrameVersion})
	if err != nil {
		return ChannelFrame{}, channelFrameError("encode canonical envelope", err)
	}
	if len(wire.Bytes()) > channelFrameMaximum(frameType) {
		return ChannelFrame{}, channelFrameError("canonical envelope exceeds typed frame limit", nil)
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
	case MemberHello:
		frameType = ChannelFrameMemberHello
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero MemberHello payload", nil)
		}
	case MemberHelloAck:
		frameType = ChannelFrameMemberHelloAck
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero MemberHelloAck payload", nil)
		}
	case SyncRequest:
		frameType = ChannelFrameSyncRequest
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero SyncRequest payload", nil)
		}
	case SyncPage:
		frameType = ChannelFrameSyncPage
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero SyncPage payload", nil)
		}
	case DataBaseline:
		frameType = ChannelFrameDataBaseline
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero DataBaseline payload", nil)
		}
	case DataBaselineAck:
		frameType = ChannelFrameDataBaselineAck
		if value.IsZero() {
			return "", model.JSON{}, channelFrameError("zero DataBaselineAck payload", nil)
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
	if len(raw) > channelFrameMaximum(wire.Type) {
		return ChannelFrame{}, channelFrameError("envelope exceeds typed frame limit", nil)
	}
	requestID, err := ParseChannelRequestID(wire.RequestID)
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
	case ChannelFrameMemberHello:
		return parseMemberHello(raw)
	case ChannelFrameMemberHelloAck:
		return parseMemberHelloAck(raw)
	case ChannelFrameSyncRequest:
		return parseSyncRequest(raw)
	case ChannelFrameSyncPage:
		return parseSyncPage(raw)
	case ChannelFrameDataBaseline:
		return parseDataBaseline(raw)
	case ChannelFrameDataBaselineAck:
		return parseDataBaselineAck(raw)
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
	if len(raw) == 0 || len(raw) > channelFrameMaximum(frame.frameType) || len(raw) > int(^uint32(0)) {
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

func maxChannelFrameBytes() int {
	if HermeticLimits().DirectFrameBytes < model.MaxCanonicalJSONBytes {
		return HermeticLimits().DirectFrameBytes
	}
	return model.MaxCanonicalJSONBytes
}

func channelFrameMaximum(frameType ChannelFrameType) int {
	switch frameType {
	case ChannelFrameSyncPage:
		return channelSyncPageFrameBytes
	case ChannelFrameEnrollAccepted, ChannelFrameMemberHello, ChannelFrameMemberHelloAck:
		return channelRosterFrameBytes
	case ChannelFrameEnrollInit, ChannelFrameEnrollChallenge, ChannelFrameEnrollProof,
		ChannelFrameSyncRequest, ChannelFrameDataBaseline, ChannelFrameDataBaselineAck,
		ChannelFrameProtocolError:
		return channelSmallFrameBytes
	default:
		return 0
	}
}

func (frame ChannelFrame) Version() uint8               { return ChannelFrameVersion }
func (frame ChannelFrame) Type() ChannelFrameType       { return frame.frameType }
func (frame ChannelFrame) RequestID() ChannelRequestID  { return frame.requestID }
func (frame ChannelFrame) Payload() ChannelFramePayload { return frame.payload }
func (frame ChannelFrame) CanonicalJSON() model.JSON    { return frame.canonical }
func (frame ChannelFrame) IsZero() bool {
	return frame.requestID.IsZero() || !frame.frameType.Valid() || frame.payload == nil ||
		frame.canonical.IsZero()
}

type EnrollInitSpec struct {
	ChannelID            model.ChannelID
	GrantID              model.GrantID
	EnrollmentRequestID  model.EnrollmentRequestID
	JoinerNonce          []byte
	SupportedVersions    []uint8
	OriginEpoch          model.OriginEpoch
	DisplayLabel         string
	AdvertisedMultiaddrs []string
}

type EnrollInit struct {
	channelID            model.ChannelID
	grantID              model.GrantID
	enrollmentRequestID  model.EnrollmentRequestID
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
	EnrollmentRequestID  string   `json:"enrollment_request_id"`
	GrantID              string   `json:"grant_id"`
	JoinerNonce          []byte   `json:"joiner_nonce"`
	OriginEpoch          string   `json:"origin_epoch"`
	SupportedVersions    []uint16 `json:"supported_versions"`
}

func NewEnrollInit(spec EnrollInitSpec) (EnrollInit, error) {
	if spec.ChannelID.IsZero() || spec.GrantID.IsZero() || spec.EnrollmentRequestID.IsZero() ||
		spec.OriginEpoch.IsZero() ||
		len(spec.JoinerNonce) != model.EnrollmentNonceBytes {
		return EnrollInit{}, channelFrameError("EnrollInit requires Channel, grant, stable enrollment request, epoch and a 32-byte nonce", nil)
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
		EnrollmentRequestID: spec.EnrollmentRequestID.String(), GrantID: spec.GrantID.String(), JoinerNonce: nonce,
		OriginEpoch: spec.OriginEpoch.String(), SupportedVersions: wireVersions})
	if err != nil {
		return EnrollInit{}, channelFrameError("encode EnrollInit", err)
	}
	return EnrollInit{channelID: spec.ChannelID, grantID: spec.GrantID,
		enrollmentRequestID: spec.EnrollmentRequestID, joinerNonce: string(nonce),
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
	enrollmentRequestID, requestErr := model.ParseEnrollmentRequestID(wire.EnrollmentRequestID)
	epoch, epochErr := model.ParseOriginEpoch(wire.OriginEpoch)
	if channelErr != nil || grantErr != nil || requestErr != nil || epochErr != nil || len(wire.SupportedVersions) == 0 ||
		len(wire.SupportedVersions) > 8 {
		return EnrollInit{}, channelFrameError("invalid EnrollInit identifiers or versions",
			errors.Join(channelErr, grantErr, requestErr, epochErr))
	}
	versions := make([]uint8, len(wire.SupportedVersions))
	for index, version := range wire.SupportedVersions {
		if version == 0 || version > 255 || (index != 0 && wire.SupportedVersions[index-1] >= version) {
			return EnrollInit{}, channelFrameError("invalid EnrollInit identifiers or versions", nil)
		}
		versions[index] = uint8(version)
	}
	payload, err := NewEnrollInit(EnrollInitSpec{ChannelID: channelID, GrantID: grantID,
		EnrollmentRequestID: enrollmentRequestID,
		JoinerNonce:         wire.JoinerNonce,
		SupportedVersions:   versions, OriginEpoch: epoch,
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
func (payload EnrollInit) EnrollmentRequestID() model.EnrollmentRequestID {
	return payload.enrollmentRequestID
}
func (payload EnrollInit) JoinerNonce() []byte { return []byte(payload.joinerNonce) }
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
	return payload.channelID.IsZero() || payload.grantID.IsZero() || payload.enrollmentRequestID.IsZero() ||
		payload.canonical.IsZero()
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

type MemberHelloSpec struct {
	ChannelID             model.ChannelID
	ActiveMemberRecord    model.Member
	KnownRosterHead       model.RecordHead
	OwnerSignedProofChain []model.Member
}

type MemberHello struct {
	channelID             model.ChannelID
	activeMemberRecord    model.Member
	knownRosterHead       model.RecordHead
	ownerSignedProofChain []model.Member
	canonical             model.JSON
}

type memberHelloWire struct {
	ActiveMemberRecord    json.RawMessage   `json:"active_member_record"`
	ChannelID             string            `json:"channel_id"`
	KnownHeadDigest       string            `json:"known_head_digest"`
	KnownRevision         uint64            `json:"known_revision"`
	OwnerSignedProofChain []json.RawMessage `json:"owner_signed_proof_chain"`
}

func NewMemberHello(spec MemberHelloSpec) (MemberHello, error) {
	if spec.ChannelID.IsZero() || spec.ActiveMemberRecord.IsZero() || spec.KnownRosterHead.IsZero() ||
		spec.KnownRosterHead.Revision() > model.MaxMemberRecordsPerChannel ||
		spec.ActiveMemberRecord.ChannelID() != spec.ChannelID ||
		spec.ActiveMemberRecord.Status() != model.MemberActive ||
		spec.ActiveMemberRecord.Head().Revision() > spec.KnownRosterHead.Revision() ||
		(spec.ActiveMemberRecord.Head().Revision() == spec.KnownRosterHead.Revision() &&
			spec.ActiveMemberRecord.Head() != spec.KnownRosterHead) {
		return MemberHello{}, channelFrameError("MemberHello requires an active member under the known Channel roster head", nil)
	}
	proof, proofWire, err := canonicalChannelMemberSequence(spec.ChannelID,
		spec.OwnerSignedProofChain, model.MaxMemberRecordsPerChannel, "MemberHello proof chain")
	if err != nil {
		return MemberHello{}, err
	}
	if len(proof) > 0 && proof[len(proof)-1].Head() != spec.KnownRosterHead {
		return MemberHello{}, channelFrameError("MemberHello proof chain does not reach the known roster head", nil)
	}
	var proofMember model.Member
	for _, member := range proof {
		if member.PeerID() == spec.ActiveMemberRecord.PeerID() {
			proofMember = member
		}
	}
	if !proofMember.IsZero() && !sameMember(proofMember, spec.ActiveMemberRecord) {
		return MemberHello{}, channelFrameError("MemberHello active member is not current in its proof chain", nil)
	}
	canonical, err := model.JSONFrom(memberHelloWire{
		ActiveMemberRecord: spec.ActiveMemberRecord.WireJSON().Bytes(), ChannelID: spec.ChannelID.String(),
		KnownHeadDigest: spec.KnownRosterHead.Digest().String(), KnownRevision: spec.KnownRosterHead.Revision(),
		OwnerSignedProofChain: proofWire,
	})
	if err != nil {
		return MemberHello{}, channelFrameError("encode MemberHello", err)
	}
	return MemberHello{channelID: spec.ChannelID, activeMemberRecord: spec.ActiveMemberRecord,
		knownRosterHead: spec.KnownRosterHead, ownerSignedProofChain: proof, canonical: canonical}, nil
}

func parseMemberHello(raw []byte) (MemberHello, error) {
	var wire memberHelloWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return MemberHello{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	activeMember, memberErr := model.ParseMember(wire.ActiveMemberRecord)
	knownHead, headErr := parseChannelRecordHead(wire.KnownRevision, wire.KnownHeadDigest)
	proof, proofErr := parseChannelMembers(wire.OwnerSignedProofChain)
	if channelErr != nil || memberErr != nil || headErr != nil || proofErr != nil {
		return MemberHello{}, channelFrameError("invalid MemberHello authority evidence",
			errors.Join(channelErr, memberErr, headErr, proofErr))
	}
	payload, err := NewMemberHello(MemberHelloSpec{ChannelID: channelID,
		ActiveMemberRecord: activeMember, KnownRosterHead: knownHead, OwnerSignedProofChain: proof})
	if err != nil {
		return MemberHello{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return MemberHello{}, channelFrameError("MemberHello bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload MemberHello) ChannelID() model.ChannelID        { return payload.channelID }
func (payload MemberHello) ActiveMemberRecord() model.Member  { return payload.activeMemberRecord }
func (payload MemberHello) KnownRosterHead() model.RecordHead { return payload.knownRosterHead }
func (payload MemberHello) OwnerSignedProofChain() []model.Member {
	return append([]model.Member(nil), payload.ownerSignedProofChain...)
}
func (payload MemberHello) CanonicalJSON() model.JSON { return payload.canonical }
func (payload MemberHello) IsZero() bool {
	return payload.channelID.IsZero() || payload.activeMemberRecord.IsZero() ||
		payload.knownRosterHead.IsZero() || payload.canonical.IsZero()
}
func (MemberHello) channelFrameType() ChannelFrameType { return ChannelFrameMemberHello }

type MemberHelloAckSpec struct {
	ChannelID      model.ChannelID
	MissingRecords []model.Member
	RosterHead     model.RecordHead
}

type MemberHelloAck struct {
	channelID      model.ChannelID
	missingRecords []model.Member
	rosterHead     model.RecordHead
	canonical      model.JSON
}

type memberHelloAckWire struct {
	ChannelID      string            `json:"channel_id"`
	MissingRecords []json.RawMessage `json:"missing_records"`
	RosterHead     recordHeadWire    `json:"roster_head"`
}

func NewMemberHelloAck(spec MemberHelloAckSpec) (MemberHelloAck, error) {
	if spec.ChannelID.IsZero() || spec.RosterHead.IsZero() ||
		spec.RosterHead.Revision() > model.MaxMemberRecordsPerChannel {
		return MemberHelloAck{}, channelFrameError("MemberHelloAck requires Channel and roster head", nil)
	}
	records, recordsWire, err := canonicalChannelMemberSequence(spec.ChannelID,
		spec.MissingRecords, model.MaxMemberRecordsPerChannel, "MemberHelloAck missing records")
	if err != nil {
		return MemberHelloAck{}, err
	}
	if len(records) > 0 && records[len(records)-1].Head() != spec.RosterHead {
		return MemberHelloAck{}, channelFrameError("MemberHelloAck missing records do not reach roster head", nil)
	}
	canonical, err := model.JSONFrom(memberHelloAckWire{ChannelID: spec.ChannelID.String(),
		MissingRecords: recordsWire, RosterHead: wireRecordHead(spec.RosterHead)})
	if err != nil {
		return MemberHelloAck{}, channelFrameError("encode MemberHelloAck", err)
	}
	return MemberHelloAck{channelID: spec.ChannelID, missingRecords: records,
		rosterHead: spec.RosterHead, canonical: canonical}, nil
}

func parseMemberHelloAck(raw []byte) (MemberHelloAck, error) {
	var wire memberHelloAckWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return MemberHelloAck{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	records, recordsErr := parseChannelMembers(wire.MissingRecords)
	rosterHead, headErr := parseChannelRecordHead(wire.RosterHead.Revision, wire.RosterHead.Digest)
	if channelErr != nil || recordsErr != nil || headErr != nil {
		return MemberHelloAck{}, channelFrameError("invalid MemberHelloAck roster evidence",
			errors.Join(channelErr, recordsErr, headErr))
	}
	payload, err := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: channelID,
		MissingRecords: records, RosterHead: rosterHead})
	if err != nil {
		return MemberHelloAck{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return MemberHelloAck{}, channelFrameError("MemberHelloAck bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload MemberHelloAck) ChannelID() model.ChannelID { return payload.channelID }
func (payload MemberHelloAck) MissingRecords() []model.Member {
	return append([]model.Member(nil), payload.missingRecords...)
}
func (payload MemberHelloAck) RosterHead() model.RecordHead { return payload.rosterHead }
func (payload MemberHelloAck) CanonicalJSON() model.JSON    { return payload.canonical }
func (payload MemberHelloAck) IsZero() bool {
	return payload.channelID.IsZero() || payload.rosterHead.IsZero() || payload.canonical.IsZero()
}
func (MemberHelloAck) channelFrameType() ChannelFrameType { return ChannelFrameMemberHelloAck }

type SyncRequestSpec struct {
	ChannelID model.ChannelID
	AfterHead model.RecordHead
}

type SyncRequest struct {
	channelID model.ChannelID
	afterHead model.RecordHead
	canonical model.JSON
}

type syncRequestWire struct {
	AfterRevision   uint64 `json:"after_revision"`
	ChannelID       string `json:"channel_id"`
	KnownHeadDigest string `json:"known_head_digest"`
}

func NewSyncRequest(spec SyncRequestSpec) (SyncRequest, error) {
	if spec.ChannelID.IsZero() || spec.AfterHead.IsZero() ||
		spec.AfterHead.Revision() > model.MaxMemberRecordsPerChannel {
		return SyncRequest{}, channelFrameError("SyncRequest requires Channel and exact after head", nil)
	}
	canonical, err := model.JSONFrom(syncRequestWire{AfterRevision: spec.AfterHead.Revision(),
		ChannelID: spec.ChannelID.String(), KnownHeadDigest: spec.AfterHead.Digest().String()})
	if err != nil {
		return SyncRequest{}, channelFrameError("encode SyncRequest", err)
	}
	return SyncRequest{channelID: spec.ChannelID, afterHead: spec.AfterHead, canonical: canonical}, nil
}

func parseSyncRequest(raw []byte) (SyncRequest, error) {
	var wire syncRequestWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return SyncRequest{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	afterHead, headErr := parseChannelRecordHead(wire.AfterRevision, wire.KnownHeadDigest)
	if channelErr != nil || headErr != nil {
		return SyncRequest{}, channelFrameError("invalid SyncRequest head", errors.Join(channelErr, headErr))
	}
	payload, err := NewSyncRequest(SyncRequestSpec{ChannelID: channelID, AfterHead: afterHead})
	if err != nil {
		return SyncRequest{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return SyncRequest{}, channelFrameError("SyncRequest bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload SyncRequest) ChannelID() model.ChannelID    { return payload.channelID }
func (payload SyncRequest) AfterHead() model.RecordHead   { return payload.afterHead }
func (payload SyncRequest) AfterRevision() uint64         { return payload.afterHead.Revision() }
func (payload SyncRequest) KnownHeadDigest() model.Digest { return payload.afterHead.Digest() }
func (payload SyncRequest) CanonicalJSON() model.JSON     { return payload.canonical }
func (payload SyncRequest) IsZero() bool {
	return payload.channelID.IsZero() || payload.afterHead.IsZero() || payload.canonical.IsZero()
}
func (SyncRequest) channelFrameType() ChannelFrameType { return ChannelFrameSyncRequest }

type SyncPageSpec struct {
	ChannelID          model.ChannelID
	More               bool
	OwnerSignedRecords []model.Member
	RosterHead         model.RecordHead
}

type SyncPage struct {
	channelID          model.ChannelID
	more               bool
	ownerSignedRecords []model.Member
	rosterHead         model.RecordHead
	canonical          model.JSON
}

type syncPageWire struct {
	ChannelID          string            `json:"channel_id"`
	More               bool              `json:"more"`
	OwnerSignedRecords []json.RawMessage `json:"owner_signed_records"`
	RosterHead         recordHeadWire    `json:"roster_head"`
}

func NewSyncPage(spec SyncPageSpec) (SyncPage, error) {
	if spec.ChannelID.IsZero() || spec.RosterHead.IsZero() ||
		spec.RosterHead.Revision() > model.MaxMemberRecordsPerChannel {
		return SyncPage{}, channelFrameError("SyncPage requires Channel and roster head", nil)
	}
	records, recordsWire, err := canonicalChannelMemberSequence(spec.ChannelID,
		spec.OwnerSignedRecords, channelSyncPageRecordLimit, "SyncPage records")
	if err != nil {
		return SyncPage{}, err
	}
	if len(records) == 0 && spec.More {
		return SyncPage{}, channelFrameError("SyncPage cannot advertise more without records", nil)
	}
	if len(records) > 0 {
		last := records[len(records)-1].Head()
		if (!spec.More && last != spec.RosterHead) ||
			(spec.More && last.Revision() >= spec.RosterHead.Revision()) {
			return SyncPage{}, channelFrameError("SyncPage records disagree with roster head and more", nil)
		}
	}
	canonical, err := model.JSONFrom(syncPageWire{ChannelID: spec.ChannelID.String(), More: spec.More,
		OwnerSignedRecords: recordsWire, RosterHead: wireRecordHead(spec.RosterHead)})
	if err != nil {
		return SyncPage{}, channelFrameError("encode SyncPage", err)
	}
	return SyncPage{channelID: spec.ChannelID, more: spec.More, ownerSignedRecords: records,
		rosterHead: spec.RosterHead, canonical: canonical}, nil
}

func parseSyncPage(raw []byte) (SyncPage, error) {
	var wire syncPageWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return SyncPage{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	records, recordsErr := parseChannelMembers(wire.OwnerSignedRecords)
	rosterHead, headErr := parseChannelRecordHead(wire.RosterHead.Revision, wire.RosterHead.Digest)
	if channelErr != nil || recordsErr != nil || headErr != nil {
		return SyncPage{}, channelFrameError("invalid SyncPage roster evidence",
			errors.Join(channelErr, recordsErr, headErr))
	}
	payload, err := NewSyncPage(SyncPageSpec{ChannelID: channelID, More: wire.More,
		OwnerSignedRecords: records, RosterHead: rosterHead})
	if err != nil {
		return SyncPage{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return SyncPage{}, channelFrameError("SyncPage bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload SyncPage) ChannelID() model.ChannelID { return payload.channelID }
func (payload SyncPage) More() bool                 { return payload.more }
func (payload SyncPage) OwnerSignedRecords() []model.Member {
	return append([]model.Member(nil), payload.ownerSignedRecords...)
}
func (payload SyncPage) RosterHead() model.RecordHead { return payload.rosterHead }
func (payload SyncPage) CanonicalJSON() model.JSON    { return payload.canonical }
func (payload SyncPage) IsZero() bool {
	return payload.channelID.IsZero() || payload.rosterHead.IsZero() || payload.canonical.IsZero()
}
func (SyncPage) channelFrameType() ChannelFrameType { return ChannelFrameSyncPage }

type DataBaselineSpec struct {
	ChannelID               model.ChannelID
	OriginPeerID            model.PeerID
	OriginEpoch             model.OriginEpoch
	BaselineChannelSequence uint64
}

type DataBaseline struct {
	channelID               model.ChannelID
	originPeerID            model.PeerID
	originEpoch             model.OriginEpoch
	baselineChannelSequence uint64
	canonical               model.JSON
}

type dataBaselineWire struct {
	BaselineChannelSequence uint64 `json:"baseline_channel_seq"`
	ChannelID               string `json:"channel_id"`
	OriginEpoch             string `json:"origin_epoch"`
	OriginPeerID            string `json:"origin_peer_id"`
}

func NewDataBaseline(spec DataBaselineSpec) (DataBaseline, error) {
	canonical, err := newDataBaselineCanonical(spec)
	if err != nil {
		return DataBaseline{}, err
	}
	return DataBaseline{channelID: spec.ChannelID, originPeerID: spec.OriginPeerID,
		originEpoch: spec.OriginEpoch, baselineChannelSequence: spec.BaselineChannelSequence,
		canonical: canonical}, nil
}

func parseDataBaseline(raw []byte) (DataBaseline, error) {
	spec, err := parseDataBaselineSpec(raw)
	if err != nil {
		return DataBaseline{}, err
	}
	payload, err := NewDataBaseline(spec)
	if err != nil {
		return DataBaseline{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return DataBaseline{}, channelFrameError("DataBaseline bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload DataBaseline) ChannelID() model.ChannelID     { return payload.channelID }
func (payload DataBaseline) OriginPeerID() model.PeerID     { return payload.originPeerID }
func (payload DataBaseline) OriginEpoch() model.OriginEpoch { return payload.originEpoch }
func (payload DataBaseline) BaselineChannelSequence() uint64 {
	return payload.baselineChannelSequence
}
func (payload DataBaseline) CanonicalJSON() model.JSON { return payload.canonical }
func (payload DataBaseline) IsZero() bool {
	return payload.channelID.IsZero() || payload.originPeerID.IsZero() || payload.originEpoch.IsZero() ||
		payload.canonical.IsZero()
}
func (DataBaseline) channelFrameType() ChannelFrameType { return ChannelFrameDataBaseline }

type DataBaselineAck struct {
	channelID               model.ChannelID
	originPeerID            model.PeerID
	originEpoch             model.OriginEpoch
	baselineChannelSequence uint64
	canonical               model.JSON
}

func NewDataBaselineAck(spec DataBaselineSpec) (DataBaselineAck, error) {
	canonical, err := newDataBaselineCanonical(spec)
	if err != nil {
		return DataBaselineAck{}, err
	}
	return DataBaselineAck{channelID: spec.ChannelID, originPeerID: spec.OriginPeerID,
		originEpoch: spec.OriginEpoch, baselineChannelSequence: spec.BaselineChannelSequence,
		canonical: canonical}, nil
}

func parseDataBaselineAck(raw []byte) (DataBaselineAck, error) {
	spec, err := parseDataBaselineSpec(raw)
	if err != nil {
		return DataBaselineAck{}, err
	}
	payload, err := NewDataBaselineAck(spec)
	if err != nil {
		return DataBaselineAck{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return DataBaselineAck{}, channelFrameError("DataBaselineAck bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload DataBaselineAck) ChannelID() model.ChannelID     { return payload.channelID }
func (payload DataBaselineAck) OriginPeerID() model.PeerID     { return payload.originPeerID }
func (payload DataBaselineAck) OriginEpoch() model.OriginEpoch { return payload.originEpoch }
func (payload DataBaselineAck) BaselineChannelSequence() uint64 {
	return payload.baselineChannelSequence
}
func (payload DataBaselineAck) CanonicalJSON() model.JSON { return payload.canonical }
func (payload DataBaselineAck) IsZero() bool {
	return payload.channelID.IsZero() || payload.originPeerID.IsZero() || payload.originEpoch.IsZero() ||
		payload.canonical.IsZero()
}
func (DataBaselineAck) channelFrameType() ChannelFrameType { return ChannelFrameDataBaselineAck }

func newDataBaselineCanonical(spec DataBaselineSpec) (model.JSON, error) {
	if spec.ChannelID.IsZero() || spec.OriginPeerID.IsZero() || spec.OriginEpoch.IsZero() ||
		spec.BaselineChannelSequence > model.MaxSQLiteInteger {
		return model.JSON{}, channelFrameError("DataBaseline requires a bounded origin tuple", nil)
	}
	canonical, err := model.JSONFrom(dataBaselineWire{BaselineChannelSequence: spec.BaselineChannelSequence,
		ChannelID: spec.ChannelID.String(), OriginEpoch: spec.OriginEpoch.String(),
		OriginPeerID: spec.OriginPeerID.String()})
	if err != nil {
		return model.JSON{}, channelFrameError("encode DataBaseline tuple", err)
	}
	return canonical, nil
}

func parseDataBaselineSpec(raw []byte) (DataBaselineSpec, error) {
	var wire dataBaselineWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return DataBaselineSpec{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	originPeerID, peerErr := model.ParsePeerID(wire.OriginPeerID)
	originEpoch, epochErr := model.ParseOriginEpoch(wire.OriginEpoch)
	if channelErr != nil || peerErr != nil || epochErr != nil ||
		wire.BaselineChannelSequence > model.MaxSQLiteInteger {
		return DataBaselineSpec{}, channelFrameError("invalid DataBaseline origin tuple",
			errors.Join(channelErr, peerErr, epochErr))
	}
	return DataBaselineSpec{ChannelID: channelID, OriginPeerID: originPeerID,
		OriginEpoch: originEpoch, BaselineChannelSequence: wire.BaselineChannelSequence}, nil
}

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

func wireRecordHead(head model.RecordHead) recordHeadWire {
	return recordHeadWire{Digest: head.Digest().String(), Revision: head.Revision()}
}

func parseChannelRecordHead(revision uint64, encodedDigest string) (model.RecordHead, error) {
	digest, digestErr := model.ParseDigest(encodedDigest)
	head, headErr := model.NewRecordHead(revision, digest)
	if digestErr != nil || headErr != nil {
		return model.RecordHead{}, channelFrameError("invalid Channel roster head",
			errors.Join(digestErr, headErr))
	}
	return head, nil
}

func parseChannelMembers(values []json.RawMessage) ([]model.Member, error) {
	if len(values) > model.MaxMemberRecordsPerChannel {
		return nil, channelFrameError("Channel member sequence exceeds roster bound", nil)
	}
	members := make([]model.Member, len(values))
	for index, value := range values {
		member, err := model.ParseMember(value)
		if err != nil {
			return nil, channelFrameError(fmt.Sprintf("invalid Channel member record %d", index+1), err)
		}
		members[index] = member
	}
	return members, nil
}

func canonicalChannelMemberSequence(channelID model.ChannelID, values []model.Member,
	maximum int, field string,
) ([]model.Member, []json.RawMessage, error) {
	if channelID.IsZero() || maximum <= 0 || maximum > model.MaxMemberRecordsPerChannel ||
		len(values) > maximum {
		return nil, nil, channelFrameError(field+" exceeds its record bound", nil)
	}
	members := append([]model.Member{}, values...)
	wire := make([]json.RawMessage, len(members))
	var previous model.Member
	for index, member := range members {
		if member.IsZero() || member.ChannelID() != channelID ||
			member.Head().Revision() > model.MaxMemberRecordsPerChannel ||
			len(member.WireJSON().Bytes()) > model.MaxChannelRecordBytes {
			return nil, nil, channelFrameError(field+" crosses Channel authority or record bound", nil)
		}
		if index > 0 {
			previousDigest, ok := member.PreviousDigest()
			if member.Head().Revision() != previous.Head().Revision()+1 || !ok ||
				previousDigest != previous.Head().Digest() {
				return nil, nil, channelFrameError(field+" is not a contiguous owner-signed sequence", nil)
			}
		}
		wire[index] = member.WireJSON().Bytes()
		previous = member
	}
	return members, wire, nil
}

func sameMember(left, right model.Member) bool {
	return !left.IsZero() && !right.IsZero() && left.Head() == right.Head() &&
		bytes.Equal(left.WireJSON().Bytes(), right.WireJSON().Bytes())
}

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
