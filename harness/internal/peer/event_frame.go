package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	// EventFrameVersion is the exact envelope version for /mnemon/events/1.
	// Protocol negotiation happens at the libp2p protocol ID; the explicit
	// version prevents otherwise-valid JSON from being reinterpreted.
	EventFrameVersion uint8 = model.SchemaVersion

	eventFrameLengthBytes   = 4
	eventSmallFrameBytes    = 32 << 10
	eventPullPageFrameBytes = 1 << 20
	eventPullPageLimit      = 32
)

var ErrEventFrame = errors.New("invalid Mnemon Events frame")

type EventFrameType string

const (
	EventFramePullRequest   EventFrameType = "pull_request"
	EventFramePullPage      EventFrameType = "pull_page"
	EventFrameCursorAck     EventFrameType = "cursor_ack"
	EventFrameAck           EventFrameType = "ack"
	EventFrameProtocolError EventFrameType = "protocol_error"
)

// EventFramePayload is sealed to the five /mnemon/events/1 repair messages;
// their one-request stream is the correlation scope.
type EventFramePayload interface {
	CanonicalJSON() model.JSON
	IsZero() bool
	eventFramePayload()
}
type eventFrameCodec interface {
	accepts(EventFramePayload) bool
	parse([]byte) (EventFramePayload, error)
}
type typedEventFrameCodec[T EventFramePayload] func([]byte) (T, error)

func (typedEventFrameCodec[T]) accepts(payload EventFramePayload) bool {
	_, ok := payload.(T)
	return ok
}
func (codec typedEventFrameCodec[T]) parse(raw []byte) (EventFramePayload, error) { return codec(raw) }

// eventFrameDescriptors is the immutable ordered authority for Events frames.
type eventFrameDescriptor struct {
	frameType EventFrameType
	maximum   int
	request   bool
	codec     eventFrameCodec
}

var eventFrameDescriptors = [...]eventFrameDescriptor{
	{EventFramePullRequest, eventSmallFrameBytes, true, typedEventFrameCodec[PullRequest](parsePullRequest)},
	{EventFramePullPage, eventPullPageFrameBytes, false, typedEventFrameCodec[PullPage](parsePullPage)},
	{EventFrameCursorAck, eventSmallFrameBytes, true, typedEventFrameCodec[CursorAck](parseCursorAck)},
	{EventFrameAck, eventSmallFrameBytes, false, typedEventFrameCodec[EventAck](parseEventAck)},
	{EventFrameProtocolError, eventSmallFrameBytes, false, typedEventFrameCodec[EventProtocolError](parseEventProtocolError)},
}

func eventFrameDescriptorFor(frameType EventFrameType) (eventFrameDescriptor, bool) {
	for _, descriptor := range eventFrameDescriptors {
		if descriptor.frameType == frameType {
			return descriptor, true
		}
	}
	return eventFrameDescriptor{}, false
}
func (frameType EventFrameType) Valid() bool {
	_, valid := eventFrameDescriptorFor(frameType)
	return valid
}
func (frameType EventFrameType) IsRequest() bool {
	descriptor, valid := eventFrameDescriptorFor(frameType)
	return valid && descriptor.request
}
func (frameType EventFrameType) IsResponse() bool {
	descriptor, valid := eventFrameDescriptorFor(frameType)
	return valid && !descriptor.request
}

type EventFrame struct {
	frameType EventFrameType
	payload   EventFramePayload
	canonical model.JSON
}

type eventFrameWire struct {
	Payload json.RawMessage `json:"payload"`
	Type    EventFrameType  `json:"type"`
	Version uint8           `json:"version"`
}

func NewEventFrame(payload EventFramePayload) (EventFrame, error) {
	if payload == nil {
		return EventFrame{}, eventFrameError("typed payload is required", nil)
	}
	frameType, canonicalPayload, err := canonicalEventPayload(payload)
	if err != nil {
		return EventFrame{}, err
	}
	canonical, err := model.JSONFrom(eventFrameWire{Payload: canonicalPayload.Bytes(),
		Type: frameType, Version: EventFrameVersion})
	if err != nil {
		return EventFrame{}, eventFrameError("encode canonical envelope", err)
	}
	if len(canonical.Bytes()) > eventFrameMaximum(frameType) {
		return EventFrame{}, eventFrameError("canonical envelope exceeds typed frame limit", nil)
	}
	return EventFrame{frameType: frameType, payload: payload, canonical: canonical}, nil
}

func canonicalEventPayload(payload EventFramePayload) (EventFrameType, model.JSON, error) {
	for _, descriptor := range eventFrameDescriptors {
		if !descriptor.codec.accepts(payload) {
			continue
		}
		if payload.IsZero() {
			return "", model.JSON{}, eventFrameError("zero typed Events payload", nil)
		}
		canonical := payload.CanonicalJSON()
		if canonical.IsZero() {
			return "", model.JSON{}, eventFrameError("canonical payload bytes are required", nil)
		}
		return descriptor.frameType, canonical, nil
	}
	return "", model.JSON{}, eventFrameError("unknown Events payload implementation", nil)
}

// ParseEventFrame admits exact canonical JSON and rebuilds the selected payload.
func ParseEventFrame(raw []byte) (EventFrame, error) {
	if len(raw) == 0 || len(raw) > maxEventFrameBytes() {
		return EventFrame{}, eventFrameError("empty or oversized envelope", nil)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return EventFrame{}, eventFrameError("envelope must be exact canonical JSON", err)
	}
	var wire eventFrameWire
	if err := decodeExactEventJSON(raw, &wire); err != nil {
		return EventFrame{}, err
	}
	if wire.Version != EventFrameVersion || !wire.Type.Valid() {
		return EventFrame{}, eventFrameError("unsupported version or frame type", nil)
	}
	if len(raw) > eventFrameMaximum(wire.Type) {
		return EventFrame{}, eventFrameError("envelope exceeds typed frame limit", nil)
	}
	payload, err := parseEventPayload(wire.Type, wire.Payload)
	if err != nil {
		return EventFrame{}, err
	}
	frame, err := NewEventFrame(payload)
	if err != nil {
		return EventFrame{}, err
	}
	if frame.frameType != wire.Type || !bytes.Equal(frame.canonical.Bytes(), raw) {
		return EventFrame{}, eventFrameError("envelope bytes are not canonical", nil)
	}
	return frame, nil
}

func parseEventPayload(frameType EventFrameType, raw []byte) (EventFramePayload, error) {
	descriptor, valid := eventFrameDescriptorFor(frameType)
	if !valid {
		return nil, eventFrameError("unknown frame type", nil)
	}
	return descriptor.codec.parse(raw)
}

// ReadEventFrame fences and reads one uint32 big-endian length-prefixed envelope.
func ReadEventFrame(reader io.Reader) (EventFrame, error) {
	return readEventFrameWithScope(reader, nil, maxEventFrameBytes())
}

// readEventStreamFrame reserves declared bytes until the caller invokes the
// returned idempotent release function.
func readEventStreamFrame(stream network.Stream, maximum int) (EventFrame, func(), error) {
	if stream == nil || stream.Scope() == nil {
		return EventFrame{}, nil, eventFrameError("live stream scope is required", nil)
	}
	return readReservedEventFrame(stream, stream.Scope(), maximum)
}

func readReservedEventFrame(reader io.Reader, scope network.ResourceScope,
	maximum int,
) (EventFrame, func(), error) {
	if scope == nil {
		return EventFrame{}, nil, eventFrameError("stream resource scope is required", nil)
	}
	return readEventFrameReserved(reader, scope, maximum)
}

func readEventFrameWithScope(reader io.Reader, scope network.ResourceScope,
	maximum int,
) (EventFrame, error) {
	frame, release, err := readEventFrameReserved(reader, scope, maximum)
	if release != nil {
		release()
	}
	return frame, err
}

func readEventFrameReserved(reader io.Reader, scope network.ResourceScope,
	maximum int,
) (EventFrame, func(), error) {
	if reader == nil || maximum <= 0 || maximum > maxEventFrameBytes() {
		return EventFrame{}, nil, eventFrameError("reader and valid message bound are required", nil)
	}
	var prefix [eventFrameLengthBytes]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return EventFrame{}, nil, eventFrameError("read length prefix", err)
	}
	length := uint64(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 || length > uint64(maximum) {
		return EventFrame{}, nil, eventFrameError("declared length is empty or exceeds message bound", nil)
	}
	reserved := int(length)
	var release func()
	if scope != nil {
		if err := scope.ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
			return EventFrame{}, nil, eventFrameError("reserve stream frame memory", err)
		}
		var once sync.Once
		release = func() { once.Do(func() { scope.ReleaseMemory(reserved) }) }
	}
	raw := make([]byte, reserved)
	if _, err := io.ReadFull(reader, raw); err != nil {
		if release != nil {
			release()
		}
		return EventFrame{}, nil, eventFrameError("read declared envelope", err)
	}
	frame, err := ParseEventFrame(raw)
	if err != nil {
		if release != nil {
			release()
		}
		return EventFrame{}, nil, err
	}
	return frame, release, nil
}

// WriteEventFrame writes one frame and rejects a zero-progress writer.
func WriteEventFrame(writer io.Writer, frame EventFrame) error {
	if writer == nil || frame.IsZero() {
		return eventFrameError("writer and complete frame are required", nil)
	}
	raw := frame.canonical.Bytes()
	if len(raw) == 0 || len(raw) > eventFrameMaximum(frame.frameType) || len(raw) > int(^uint32(0)) {
		return eventFrameError("canonical envelope exceeds length prefix or typed frame limit", nil)
	}
	var prefix [eventFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if err := writeFullEventFrame(writer, prefix[:]); err != nil {
		return eventFrameError("write length prefix", err)
	}
	if err := writeFullEventFrame(writer, raw); err != nil {
		return eventFrameError("write canonical envelope", err)
	}
	return nil
}

func writeFullEventFrame(writer io.Writer, value []byte) error {
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

func maxEventFrameBytes() int {
	// DirectFrameBytes is the outer Artifact-capable stream ceiling. Events has
	// no frame larger than a PullPage, so reject its 1 MiB bound before payload
	// allocation instead of borrowing the unrelated 8 MiB outer allowance.
	if HermeticLimits().PullPageBytes < eventPullPageFrameBytes {
		return HermeticLimits().PullPageBytes
	}
	return eventPullPageFrameBytes
}

func eventFrameMaximum(frameType EventFrameType) int {
	descriptor, _ := eventFrameDescriptorFor(frameType)
	return descriptor.maximum
}

func (frame EventFrame) Version() uint8             { return EventFrameVersion }
func (frame EventFrame) Type() EventFrameType       { return frame.frameType }
func (frame EventFrame) Payload() EventFramePayload { return frame.payload }
func (frame EventFrame) CanonicalJSON() model.JSON  { return frame.canonical }
func (frame EventFrame) IsRequest() bool            { return frame.frameType.IsRequest() }
func (frame EventFrame) IsResponse() bool           { return frame.frameType.IsResponse() }
func (frame EventFrame) IsZero() bool {
	return !frame.frameType.Valid() || frame.payload == nil || frame.canonical.IsZero()
}

type PullRequestSpec struct {
	ChannelID            model.ChannelID
	OriginEpoch          model.OriginEpoch
	AfterChannelSequence uint64
	Limit                uint8
}

type PullRequest struct {
	channelID            model.ChannelID
	originEpoch          model.OriginEpoch
	afterChannelSequence uint64
	limit                uint8
	canonical            model.JSON
}

type pullRequestWire struct {
	AfterChannelSequence uint64 `json:"after_channel_seq"`
	ChannelID            string `json:"channel_id"`
	Limit                uint8  `json:"limit"`
	OriginEpoch          string `json:"origin_epoch"`
}

func NewPullRequest(spec PullRequestSpec) (PullRequest, error) {
	if spec.ChannelID.IsZero() || spec.OriginEpoch.IsZero() ||
		spec.AfterChannelSequence > model.MaxSQLiteInteger || spec.Limit == 0 ||
		int(spec.Limit) > eventPullPageLimit {
		return PullRequest{}, eventFrameError("PullRequest requires a bounded Channel cursor and limit", nil)
	}
	canonical, err := model.JSONFrom(pullRequestWire{AfterChannelSequence: spec.AfterChannelSequence,
		ChannelID: spec.ChannelID.String(), Limit: spec.Limit,
		OriginEpoch: spec.OriginEpoch.String()})
	if err != nil {
		return PullRequest{}, eventFrameError("encode PullRequest", err)
	}
	return PullRequest{channelID: spec.ChannelID, originEpoch: spec.OriginEpoch,
		afterChannelSequence: spec.AfterChannelSequence, limit: spec.Limit,
		canonical: canonical}, nil
}

func parsePullRequest(raw []byte) (PullRequest, error) {
	var wire pullRequestWire
	if err := decodeExactEventJSON(raw, &wire); err != nil {
		return PullRequest{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	originEpoch, epochErr := model.ParseOriginEpoch(wire.OriginEpoch)
	if channelErr != nil || epochErr != nil {
		return PullRequest{}, eventFrameError("invalid PullRequest identifiers",
			errors.Join(channelErr, epochErr))
	}
	payload, err := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, AfterChannelSequence: wire.AfterChannelSequence,
		Limit: wire.Limit})
	if err != nil {
		return PullRequest{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return PullRequest{}, eventFrameError("PullRequest bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload PullRequest) ChannelID() model.ChannelID { return payload.channelID }
func (payload PullRequest) OriginEpoch() model.OriginEpoch {
	return payload.originEpoch
}
func (payload PullRequest) AfterChannelSequence() uint64 {
	return payload.afterChannelSequence
}
func (payload PullRequest) Limit() uint8              { return payload.limit }
func (payload PullRequest) CanonicalJSON() model.JSON { return payload.canonical }
func (payload PullRequest) IsZero() bool {
	return payload.channelID.IsZero() || payload.originEpoch.IsZero() || payload.limit == 0 || payload.canonical.IsZero()
}
func (PullRequest) eventFramePayload() {}

type PullPageSpec struct {
	Publications           []model.SignedPublication
	ScannedChannelSequence uint64
	SourceFloor            uint64
	SourceHead             uint64
	OriginEpoch            model.OriginEpoch
}

type PullPage struct {
	publications           []model.PublicationEvidence
	scannedChannelSequence uint64
	sourceFloor            uint64
	sourceHead             uint64
	originEpoch            model.OriginEpoch
	canonical              model.JSON
}

type pullPageWire struct {
	OriginEpoch            string            `json:"origin_epoch"`
	Publications           []json.RawMessage `json:"publications"`
	ScannedChannelSequence uint64            `json:"scanned_channel_seq"`
	SourceFloor            uint64            `json:"source_floor"`
	SourceHead             uint64            `json:"source_head"`
}

func NewPullPage(spec PullPageSpec) (PullPage, error) {
	publications := make([]model.PublicationEvidence, len(spec.Publications))
	for index, candidate := range spec.Publications {
		raw := candidate.WireJSON().Bytes()
		if len(raw) == 0 || len(raw) > model.MaxPublicationBytes {
			return PullPage{}, eventFrameError(fmt.Sprintf("PullPage publication %d exceeds its wire bound", index+1), nil)
		}
		publication, err := model.ParsePublicationEvidence(raw)
		if err != nil || !publication.IsSupported() {
			return PullPage{}, eventFrameError(fmt.Sprintf("parse supported PullPage publication %d", index+1), err)
		}
		publications[index] = publication
	}
	return newPullPageFromEvidence(publications, spec.ScannedChannelSequence, spec.SourceFloor,
		spec.SourceHead, spec.OriginEpoch)
}

func newPullPageFromEvidence(publications []model.PublicationEvidence, scannedChannelSequence,
	sourceFloor, sourceHead uint64, originEpoch model.OriginEpoch,
) (PullPage, error) {
	if originEpoch.IsZero() || sourceFloor == 0 ||
		sourceFloor > model.MaxSQLiteInteger || sourceHead > model.MaxSQLiteInteger ||
		scannedChannelSequence > model.MaxSQLiteInteger ||
		sourceFloor-1 > sourceHead ||
		scannedChannelSequence < sourceFloor-1 ||
		scannedChannelSequence > sourceHead ||
		len(publications) > eventPullPageLimit {
		return PullPage{}, eventFrameError("PullPage floor, scanned head or publication count is invalid", nil)
	}
	retained := make([]model.PublicationEvidence, len(publications))
	wirePublications := make([]json.RawMessage, len(publications))
	var channelID model.ChannelID
	var originPeerID model.PeerID
	var previousSequence uint64
	totalPublicationBytes := 0
	for index, publication := range publications {
		raw := publication.WireJSON().Bytes()
		if len(raw) == 0 || len(raw) > model.MaxPublicationBytes {
			return PullPage{}, eventFrameError(fmt.Sprintf("PullPage publication %d exceeds its wire bound", index+1), nil)
		}
		if publication.IsZero() {
			return PullPage{}, eventFrameError(fmt.Sprintf("PullPage publication %d is incomplete", index+1), nil)
		}
		if index == 0 {
			channelID, originPeerID = publication.ChannelID(), publication.OriginPeerID()
		} else if publication.ChannelID() != channelID || publication.OriginPeerID() != originPeerID {
			return PullPage{}, eventFrameError("PullPage publications cross Channel or origin", nil)
		}
		sequence := publication.ChannelSequence()
		if publication.OriginEpoch() != originEpoch || sequence < sourceFloor ||
			(index > 0 && sequence != previousSequence+1) || sequence > scannedChannelSequence ||
			sequence > sourceHead {
			return PullPage{}, eventFrameError("PullPage publication tuple or sequence is inconsistent", nil)
		}
		if totalPublicationBytes > eventPullPageFrameBytes-len(raw) {
			return PullPage{}, eventFrameError("PullPage publication bytes exceed page bound", nil)
		}
		totalPublicationBytes += len(raw)
		retained[index] = publication
		wirePublications[index] = raw
		previousSequence = sequence
	}
	if len(retained) > 0 && previousSequence != scannedChannelSequence {
		return PullPage{}, eventFrameError("PullPage publications do not reach the scanned cursor", nil)
	}
	canonical, err := model.JSONFrom(pullPageWire{OriginEpoch: originEpoch.String(),
		Publications: wirePublications, ScannedChannelSequence: scannedChannelSequence,
		SourceFloor: sourceFloor, SourceHead: sourceHead})
	if err != nil {
		return PullPage{}, eventFrameError("encode PullPage", err)
	}
	envelope, err := model.JSONFrom(eventFrameWire{Payload: canonical.Bytes(),
		Type: EventFramePullPage, Version: EventFrameVersion})
	if err != nil {
		return PullPage{}, eventFrameError("encode PullPage envelope", err)
	}
	if len(envelope.Bytes()) > eventPullPageFrameBytes {
		return PullPage{}, eventFrameError("PullPage canonical envelope exceeds page bound", nil)
	}
	return PullPage{publications: retained,
		scannedChannelSequence: scannedChannelSequence, sourceFloor: sourceFloor,
		sourceHead: sourceHead, originEpoch: originEpoch, canonical: canonical}, nil
}

func parsePullPage(raw []byte) (PullPage, error) {
	var wire pullPageWire
	if err := decodeExactEventJSON(raw, &wire); err != nil {
		return PullPage{}, err
	}
	if len(wire.Publications) > eventPullPageLimit {
		return PullPage{}, eventFrameError("PullPage publication count exceeds page bound", nil)
	}
	originEpoch, err := model.ParseOriginEpoch(wire.OriginEpoch)
	if err != nil {
		return PullPage{}, eventFrameError("invalid PullPage origin epoch", err)
	}
	publications := make([]model.PublicationEvidence, len(wire.Publications))
	totalPublicationBytes := 0
	for index, publicationWire := range wire.Publications {
		if len(publicationWire) == 0 || len(publicationWire) > model.MaxPublicationBytes ||
			totalPublicationBytes > eventPullPageFrameBytes-len(publicationWire) {
			return PullPage{}, eventFrameError(fmt.Sprintf("PullPage publication %d exceeds page bounds", index+1), nil)
		}
		totalPublicationBytes += len(publicationWire)
		publication, parseErr := model.ParsePublicationEvidence(publicationWire)
		if parseErr != nil {
			return PullPage{}, eventFrameError(fmt.Sprintf("parse PullPage publication %d", index+1), parseErr)
		}
		publications[index] = publication
	}
	payload, err := newPullPageFromEvidence(publications, wire.ScannedChannelSequence, wire.SourceFloor,
		wire.SourceHead, originEpoch)
	if err != nil {
		return PullPage{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return PullPage{}, eventFrameError("PullPage bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload PullPage) Publications() []model.PublicationEvidence {
	return append([]model.PublicationEvidence(nil), payload.publications...)
}
func (payload PullPage) ScannedChannelSequence() uint64 {
	return payload.scannedChannelSequence
}
func (payload PullPage) SourceFloor() uint64            { return payload.sourceFloor }
func (payload PullPage) SourceHead() uint64             { return payload.sourceHead }
func (payload PullPage) OriginEpoch() model.OriginEpoch { return payload.originEpoch }
func (payload PullPage) CanonicalJSON() model.JSON      { return payload.canonical }
func (payload PullPage) IsZero() bool {
	return payload.originEpoch.IsZero() || payload.sourceFloor == 0 || payload.canonical.IsZero()
}
func (PullPage) eventFramePayload() {}

type CursorAckSpec struct {
	ChannelID                 model.ChannelID
	OriginEpoch               model.OriginEpoch
	ContiguousChannelSequence uint64
}

type CursorAck struct {
	channelID                 model.ChannelID
	originEpoch               model.OriginEpoch
	contiguousChannelSequence uint64
	canonical                 model.JSON
}

type cursorAckWire struct {
	ChannelID                 string `json:"channel_id"`
	ContiguousChannelSequence uint64 `json:"contiguous_channel_seq"`
	OriginEpoch               string `json:"origin_epoch"`
}

func NewCursorAck(spec CursorAckSpec) (CursorAck, error) {
	if spec.ChannelID.IsZero() || spec.OriginEpoch.IsZero() ||
		spec.ContiguousChannelSequence > model.MaxSQLiteInteger {
		return CursorAck{}, eventFrameError("CursorAck requires a bounded Channel cursor", nil)
	}
	canonical, err := model.JSONFrom(cursorAckWire{ChannelID: spec.ChannelID.String(),
		ContiguousChannelSequence: spec.ContiguousChannelSequence,
		OriginEpoch:               spec.OriginEpoch.String()})
	if err != nil {
		return CursorAck{}, eventFrameError("encode CursorAck", err)
	}
	return CursorAck{channelID: spec.ChannelID, originEpoch: spec.OriginEpoch,
		contiguousChannelSequence: spec.ContiguousChannelSequence, canonical: canonical}, nil
}

func parseCursorAck(raw []byte) (CursorAck, error) {
	var wire cursorAckWire
	if err := decodeExactEventJSON(raw, &wire); err != nil {
		return CursorAck{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	originEpoch, epochErr := model.ParseOriginEpoch(wire.OriginEpoch)
	if channelErr != nil || epochErr != nil {
		return CursorAck{}, eventFrameError("invalid CursorAck identifiers",
			errors.Join(channelErr, epochErr))
	}
	payload, err := NewCursorAck(CursorAckSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, ContiguousChannelSequence: wire.ContiguousChannelSequence})
	if err != nil {
		return CursorAck{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return CursorAck{}, eventFrameError("CursorAck bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload CursorAck) ChannelID() model.ChannelID { return payload.channelID }
func (payload CursorAck) OriginEpoch() model.OriginEpoch {
	return payload.originEpoch
}
func (payload CursorAck) ContiguousChannelSequence() uint64 {
	return payload.contiguousChannelSequence
}
func (payload CursorAck) CanonicalJSON() model.JSON { return payload.canonical }
func (payload CursorAck) IsZero() bool {
	return payload.channelID.IsZero() || payload.originEpoch.IsZero() || payload.canonical.IsZero()
}
func (CursorAck) eventFramePayload() {}

// EventAck is the empty success response to a durable CursorAck commit; its
// request stream supplies correlation and Store retries remain idempotent.
type EventAck struct{ canonical model.JSON }

func NewEventAck() (EventAck, error) {
	canonical, err := model.JSONFrom(struct{}{})
	if err != nil {
		return EventAck{}, eventFrameError("encode Ack", err)
	}
	return EventAck{canonical: canonical}, nil
}

func parseEventAck(raw []byte) (EventAck, error) {
	var wire struct{}
	if err := decodeExactEventJSON(raw, &wire); err != nil {
		return EventAck{}, err
	}
	payload, err := NewEventAck()
	if err != nil {
		return EventAck{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EventAck{}, eventFrameError("Ack bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload EventAck) CanonicalJSON() model.JSON { return payload.canonical }
func (payload EventAck) IsZero() bool              { return payload.canonical.IsZero() }
func (EventAck) eventFramePayload()                {}

type EventProtocolErrorCode string

const (
	EventErrorBusy                EventProtocolErrorCode = "busy"
	EventErrorHistoryGap          EventProtocolErrorCode = "history_gap"
	EventErrorNotOrigin           EventProtocolErrorCode = "not_origin"
	EventErrorNotMember           EventProtocolErrorCode = "not_member"
	EventErrorMemberRevoked       EventProtocolErrorCode = "member_revoked"
	EventErrorChannelClosed       EventProtocolErrorCode = "channel_closed"
	EventErrorOriginEpochMismatch EventProtocolErrorCode = "origin_epoch_mismatch"
)

func (code EventProtocolErrorCode) Valid() bool {
	switch code {
	case EventErrorBusy, EventErrorHistoryGap, EventErrorNotOrigin, EventErrorNotMember,
		EventErrorMemberRevoked, EventErrorChannelClosed, EventErrorOriginEpochMismatch:
		return true
	default:
		return false
	}
}

func (code EventProtocolErrorCode) retryable() bool { return code == EventErrorBusy }

type EventProtocolErrorSpec struct {
	Code        EventProtocolErrorCode
	Retryable   bool
	RetryAfter  time.Duration
	SourceFloor uint64
}

type EventProtocolError struct {
	code        EventProtocolErrorCode
	retryable   bool
	retryAfter  time.Duration
	sourceFloor uint64
	canonical   model.JSON
}

type eventProtocolErrorWire struct {
	Code        EventProtocolErrorCode `json:"code"`
	RetryAfter  int64                  `json:"retry_after"`
	Retryable   bool                   `json:"retryable"`
	SourceFloor uint64                 `json:"source_floor"`
}

// NewEventProtocolError freezes retry policy into the stable error code.
// retry_after is integer milliseconds. history_gap alone carries source_floor.
func NewEventProtocolError(spec EventProtocolErrorSpec) (EventProtocolError, error) {
	validHistoryFloor := spec.Code == EventErrorHistoryGap && spec.SourceFloor > 0 &&
		spec.SourceFloor <= model.MaxSQLiteInteger
	if !spec.Code.Valid() || spec.Retryable != spec.Code.retryable() || spec.RetryAfter < 0 ||
		spec.RetryAfter%time.Millisecond != 0 ||
		(spec.Retryable && spec.RetryAfter == 0) || (!spec.Retryable && spec.RetryAfter != 0) ||
		(spec.Code == EventErrorHistoryGap && !validHistoryFloor) ||
		(spec.Code != EventErrorHistoryGap && spec.SourceFloor != 0) {
		return EventProtocolError{}, eventFrameError("ProtocolError code, retry policy or source floor is invalid", nil)
	}
	milliseconds := spec.RetryAfter.Milliseconds()
	canonical, err := model.JSONFrom(eventProtocolErrorWire{Code: spec.Code,
		RetryAfter: milliseconds, Retryable: spec.Retryable, SourceFloor: spec.SourceFloor})
	if err != nil {
		return EventProtocolError{}, eventFrameError("encode ProtocolError", err)
	}
	return EventProtocolError{code: spec.Code, retryable: spec.Retryable,
		retryAfter: spec.RetryAfter, sourceFloor: spec.SourceFloor, canonical: canonical}, nil
}

func parseEventProtocolError(raw []byte) (EventProtocolError, error) {
	var wire eventProtocolErrorWire
	if err := decodeExactEventJSON(raw, &wire); err != nil {
		return EventProtocolError{}, err
	}
	if wire.RetryAfter < 0 || wire.RetryAfter > int64((time.Duration(1<<63-1))/time.Millisecond) {
		return EventProtocolError{}, eventFrameError("ProtocolError retry_after is out of range", nil)
	}
	payload, err := NewEventProtocolError(EventProtocolErrorSpec{Code: wire.Code,
		Retryable: wire.Retryable, RetryAfter: time.Duration(wire.RetryAfter) * time.Millisecond,
		SourceFloor: wire.SourceFloor})
	if err != nil {
		return EventProtocolError{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EventProtocolError{}, eventFrameError("ProtocolError bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload EventProtocolError) Code() EventProtocolErrorCode { return payload.code }
func (payload EventProtocolError) Retryable() bool              { return payload.retryable }
func (payload EventProtocolError) RetryAfter() time.Duration    { return payload.retryAfter }
func (payload EventProtocolError) SourceFloor() uint64          { return payload.sourceFloor }
func (payload EventProtocolError) CanonicalJSON() model.JSON    { return payload.canonical }
func (payload EventProtocolError) IsZero() bool {
	return !payload.code.Valid() || payload.canonical.IsZero()
}
func (EventProtocolError) eventFramePayload() {}

func decodeExactEventJSON(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > maxEventFrameBytes() {
		return eventFrameError("empty or oversized canonical value", nil)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return eventFrameError("wire value must be exact canonical JSON", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return eventFrameError("decode exact wire value", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return eventFrameError("wire value contains a trailing value", err)
	}
	return nil
}

func eventFrameError(detail string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrEventFrame, detail)
	}
	return fmt.Errorf("%w: %s: %w", ErrEventFrame, detail, cause)
}
