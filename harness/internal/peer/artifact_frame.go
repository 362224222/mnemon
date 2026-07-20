package peer

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	// ArtifactFrameVersion is the exact envelope version admitted by
	// /mnemon/artifacts/1. Request correlation is the one-request/one-response
	// stream itself, so Artifact frames deliberately have no request ID.
	ArtifactFrameVersion uint8 = model.SchemaVersion

	artifactFrameLengthBytes = 4
	artifactSmallFrameBytes  = 32 << 10
	artifactManifestBytes    = artifactdomain.MaxManifestBytes
	artifactBlockBytes       = artifactdomain.BlockSize
	artifactDirectFrameBytes = 8 << 20
)

var (
	ErrArtifactFrame = errors.New("invalid Mnemon Artifact frame")

	// These package-private sentinels preserve the only content diagnostics the
	// Artifact client may act on after a single bounded frame parse. They carry
	// no remote bytes or parser detail and remain ordinary ErrArtifactFrame
	// failures to every other protocol consumer.
	errArtifactFrameManifestInvalid        = fmt.Errorf("%w: manifest invalid", ErrArtifactFrame)
	errArtifactFrameManifestDigestMismatch = fmt.Errorf("%w: manifest digest mismatch", ErrArtifactFrame)
	errArtifactFrameBlockDigestMismatch    = fmt.Errorf("%w: block digest mismatch", ErrArtifactFrame)
)

// ArtifactJSON is an immutable exact canonical JSON value bounded by the
// Artifact direct-frame limit. It is intentionally separate from model.JSON:
// a valid 4 MiB manifest is base64 encoded inside its response envelope and
// can therefore exceed model.JSON's 4 MiB domain-object ceiling while still
// remaining below the 8 MiB direct-frame ceiling.
type ArtifactJSON struct {
	raw string
}

func newArtifactJSON(raw []byte, maximum int, exact bool) (ArtifactJSON, error) {
	if len(raw) == 0 || maximum <= 0 || maximum > maxArtifactFrameBytes() || len(raw) > maximum {
		return ArtifactJSON{}, artifactFrameError("empty or oversized canonical JSON", nil)
	}
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil {
		return ArtifactJSON{}, artifactFrameError("canonicalize JSON", err)
	}
	if len(canonical) > maximum || (exact && !bytes.Equal(canonical, raw)) {
		return ArtifactJSON{}, artifactFrameError("JSON must be exact and within its typed limit", nil)
	}
	return ArtifactJSON{raw: string(canonical)}, nil
}

func artifactJSONFrom(value any, maximum int) (ArtifactJSON, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return ArtifactJSON{}, artifactFrameError("marshal canonical JSON", err)
	}
	return newArtifactJSON(raw, maximum, false)
}

func (value ArtifactJSON) Bytes() []byte  { return append([]byte(nil), value.raw...) }
func (value ArtifactJSON) String() string { return value.raw }
func (value ArtifactJSON) IsZero() bool   { return value.raw == "" }
func (value ArtifactJSON) MarshalJSON() ([]byte, error) {
	if value.IsZero() {
		return nil, artifactFrameError("zero canonical JSON", nil)
	}
	return value.Bytes(), nil
}

type ArtifactFrameType string

const (
	ArtifactFrameGetManifest   ArtifactFrameType = "get_manifest"
	ArtifactFrameManifest      ArtifactFrameType = "manifest"
	ArtifactFrameGetBlock      ArtifactFrameType = "get_block"
	ArtifactFrameBlock         ArtifactFrameType = "block"
	ArtifactFrameAck           ArtifactFrameType = "ack"
	ArtifactFrameProtocolError ArtifactFrameType = "protocol_error"
)

func (frameType ArtifactFrameType) Valid() bool {
	switch frameType {
	case ArtifactFrameGetManifest, ArtifactFrameManifest, ArtifactFrameGetBlock,
		ArtifactFrameBlock, ArtifactFrameAck, ArtifactFrameProtocolError:
		return true
	default:
		return false
	}
}

func (frameType ArtifactFrameType) IsRequest() bool {
	return frameType == ArtifactFrameGetManifest || frameType == ArtifactFrameGetBlock
}

func (frameType ArtifactFrameType) IsResponse() bool {
	switch frameType {
	case ArtifactFrameManifest, ArtifactFrameBlock, ArtifactFrameAck,
		ArtifactFrameProtocolError:
		return true
	default:
		return false
	}
}

// ArtifactFramePayload is sealed to content-addressed manifest/block reads and
// their closed responses. It cannot carry a filesystem path, generic command,
// remote write, or execution request.
type ArtifactFramePayload interface {
	CanonicalJSON() ArtifactJSON
	artifactFrameType() ArtifactFrameType
}

type ArtifactFrame struct {
	frameType ArtifactFrameType
	payload   ArtifactFramePayload
	canonical ArtifactJSON
}

type artifactFrameWire struct {
	Payload json.RawMessage   `json:"payload"`
	Type    ArtifactFrameType `json:"type"`
	Version uint8             `json:"version"`
}

func NewArtifactFrame(payload ArtifactFramePayload) (ArtifactFrame, error) {
	if payload == nil {
		return ArtifactFrame{}, artifactFrameError("typed payload is required", nil)
	}
	frameType, canonicalPayload, err := canonicalArtifactPayload(payload)
	if err != nil {
		return ArtifactFrame{}, err
	}
	canonical, err := artifactJSONFrom(artifactFrameWire{Payload: canonicalPayload.Bytes(),
		Type: frameType, Version: ArtifactFrameVersion}, artifactFrameMaximum(frameType))
	if err != nil {
		return ArtifactFrame{}, artifactFrameError("encode canonical envelope", err)
	}
	return ArtifactFrame{frameType: frameType, payload: payload, canonical: canonical}, nil
}

func canonicalArtifactPayload(payload ArtifactFramePayload) (ArtifactFrameType, ArtifactJSON, error) {
	var frameType ArtifactFrameType
	switch value := payload.(type) {
	case GetManifest:
		frameType = ArtifactFrameGetManifest
		if value.IsZero() {
			return "", ArtifactJSON{}, artifactFrameError("zero GetManifest payload", nil)
		}
	case Manifest:
		frameType = ArtifactFrameManifest
		if value.IsZero() {
			return "", ArtifactJSON{}, artifactFrameError("zero Manifest payload", nil)
		}
	case GetBlock:
		frameType = ArtifactFrameGetBlock
		if value.IsZero() {
			return "", ArtifactJSON{}, artifactFrameError("zero GetBlock payload", nil)
		}
	case Block:
		frameType = ArtifactFrameBlock
		if value.IsZero() {
			return "", ArtifactJSON{}, artifactFrameError("zero Block payload", nil)
		}
	case ArtifactAck:
		frameType = ArtifactFrameAck
		if value.IsZero() {
			return "", ArtifactJSON{}, artifactFrameError("zero Ack payload", nil)
		}
	case ArtifactProtocolError:
		frameType = ArtifactFrameProtocolError
		if value.IsZero() {
			return "", ArtifactJSON{}, artifactFrameError("zero ProtocolError payload", nil)
		}
	default:
		return "", ArtifactJSON{}, artifactFrameError("unknown Artifact payload implementation", nil)
	}
	if payload.artifactFrameType() != frameType || payload.CanonicalJSON().IsZero() {
		return "", ArtifactJSON{}, artifactFrameError("payload type or canonical bytes are inconsistent", nil)
	}
	return frameType, payload.CanonicalJSON(), nil
}

// ParseArtifactFrame admits exact canonical JSON only and reconstructs the
// selected typed payload before returning it.
func ParseArtifactFrame(raw []byte) (ArtifactFrame, error) {
	if len(raw) == 0 || len(raw) > maxArtifactFrameBytes() {
		return ArtifactFrame{}, artifactFrameError("empty or oversized envelope", nil)
	}
	canonical, err := newArtifactJSON(raw, maxArtifactFrameBytes(), true)
	if err != nil {
		return ArtifactFrame{}, err
	}
	var wire artifactFrameWire
	if err := decodeExactArtifactJSON(raw, &wire, maxArtifactFrameBytes()); err != nil {
		return ArtifactFrame{}, err
	}
	if wire.Version != ArtifactFrameVersion || !wire.Type.Valid() {
		return ArtifactFrame{}, artifactFrameError("unsupported version or frame type", nil)
	}
	if len(raw) > artifactFrameMaximum(wire.Type) {
		return ArtifactFrame{}, artifactFrameError("envelope exceeds typed frame limit", nil)
	}
	payload, err := parseArtifactPayload(wire.Type, wire.Payload)
	if err != nil {
		return ArtifactFrame{}, err
	}
	frame, err := NewArtifactFrame(payload)
	if err != nil {
		return ArtifactFrame{}, err
	}
	if frame.frameType != wire.Type || !bytes.Equal(frame.canonical.Bytes(), canonical.Bytes()) {
		return ArtifactFrame{}, artifactFrameError("envelope bytes are not canonical", nil)
	}
	return frame, nil
}

func parseArtifactPayload(frameType ArtifactFrameType, raw []byte) (ArtifactFramePayload, error) {
	switch frameType {
	case ArtifactFrameGetManifest:
		return parseGetManifest(raw)
	case ArtifactFrameManifest:
		return parseManifest(raw)
	case ArtifactFrameGetBlock:
		return parseGetBlock(raw)
	case ArtifactFrameBlock:
		return parseBlock(raw)
	case ArtifactFrameAck:
		return parseArtifactAck(raw)
	case ArtifactFrameProtocolError:
		return parseArtifactProtocolError(raw)
	default:
		return nil, artifactFrameError("unknown frame type", nil)
	}
}

// ReadArtifactFrame reads one uint32 big-endian length-prefixed frame and
// releases any optional resource reservation before it returns.
func ReadArtifactFrame(reader io.Reader) (ArtifactFrame, error) {
	return readArtifactFrameWithScope(reader, nil, maxArtifactFrameBytes())
}

// readArtifactStreamFrame keeps the declared frame bytes reserved until the
// returned idempotent release function is called by the stream owner.
func readArtifactStreamFrame(stream network.Stream, maximum int) (ArtifactFrame, func(), error) {
	if stream == nil || stream.Scope() == nil {
		return ArtifactFrame{}, nil, artifactFrameError("live stream scope is required", nil)
	}
	return readReservedArtifactFrame(stream, stream.Scope(), maximum)
}

func readReservedArtifactFrame(reader io.Reader, scope network.ResourceScope,
	maximum int,
) (ArtifactFrame, func(), error) {
	if scope == nil {
		return ArtifactFrame{}, nil, artifactFrameError("stream resource scope is required", nil)
	}
	return readArtifactFrameReserved(reader, scope, maximum)
}

func readArtifactFrameWithScope(reader io.Reader, scope network.ResourceScope,
	maximum int,
) (ArtifactFrame, error) {
	frame, release, err := readArtifactFrameReserved(reader, scope, maximum)
	if release != nil {
		release()
	}
	return frame, err
}

func readArtifactFrameReserved(reader io.Reader, scope network.ResourceScope,
	maximum int,
) (ArtifactFrame, func(), error) {
	if reader == nil || maximum <= 0 || maximum > maxArtifactFrameBytes() {
		return ArtifactFrame{}, nil, artifactFrameError("reader and valid message bound are required", nil)
	}
	var prefix [artifactFrameLengthBytes]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return ArtifactFrame{}, nil, artifactFrameError("read length prefix", err)
	}
	length := uint64(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 || length > uint64(maximum) {
		return ArtifactFrame{}, nil, artifactFrameError("declared length is empty or exceeds message bound", nil)
	}
	reserved := int(length)
	var release func()
	if scope != nil {
		if err := scope.ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
			return ArtifactFrame{}, nil, artifactFrameError("reserve stream frame memory", err)
		}
		var once sync.Once
		release = func() { once.Do(func() { scope.ReleaseMemory(reserved) }) }
	}
	raw := make([]byte, reserved)
	if _, err := io.ReadFull(reader, raw); err != nil {
		if release != nil {
			release()
		}
		return ArtifactFrame{}, nil, artifactFrameError("read declared envelope", err)
	}
	frame, err := ParseArtifactFrame(raw)
	if err != nil {
		if release != nil {
			release()
		}
		return ArtifactFrame{}, nil, err
	}
	return frame, release, nil
}

// WriteArtifactFrame writes one complete uint32 big-endian length-prefixed
// canonical envelope and rejects zero-progress writers.
func WriteArtifactFrame(writer io.Writer, frame ArtifactFrame) error {
	if writer == nil || frame.IsZero() {
		return artifactFrameError("writer and complete frame are required", nil)
	}
	raw := frame.canonical.Bytes()
	if len(raw) == 0 || len(raw) > artifactFrameMaximum(frame.frameType) || len(raw) > int(^uint32(0)) {
		return artifactFrameError("canonical envelope exceeds length prefix or typed frame limit", nil)
	}
	var prefix [artifactFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if err := writeFullArtifactFrame(writer, prefix[:]); err != nil {
		return artifactFrameError("write length prefix", err)
	}
	if err := writeFullArtifactFrame(writer, raw); err != nil {
		return artifactFrameError("write canonical envelope", err)
	}
	return nil
}

func writeFullArtifactFrame(writer io.Writer, value []byte) error {
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

func maxArtifactFrameBytes() int {
	if HermeticLimits().DirectFrameBytes < artifactDirectFrameBytes {
		return HermeticLimits().DirectFrameBytes
	}
	return artifactDirectFrameBytes
}

func artifactFrameMaximum(frameType ArtifactFrameType) int {
	switch frameType {
	case ArtifactFrameGetManifest, ArtifactFrameGetBlock, ArtifactFrameAck,
		ArtifactFrameProtocolError:
		return artifactSmallFrameBytes
	case ArtifactFrameManifest, ArtifactFrameBlock:
		return maxArtifactFrameBytes()
	default:
		return 0
	}
}

func artifactManifestMaximum() int {
	if HermeticLimits().ArtifactManifestBytes < artifactManifestBytes {
		return HermeticLimits().ArtifactManifestBytes
	}
	return artifactManifestBytes
}

func artifactBlockMaximum() int {
	if HermeticLimits().ArtifactBlockBytes < artifactBlockBytes {
		return HermeticLimits().ArtifactBlockBytes
	}
	return artifactBlockBytes
}

func (frame ArtifactFrame) Version() uint8                { return ArtifactFrameVersion }
func (frame ArtifactFrame) Type() ArtifactFrameType       { return frame.frameType }
func (frame ArtifactFrame) Payload() ArtifactFramePayload { return frame.payload }
func (frame ArtifactFrame) CanonicalJSON() ArtifactJSON   { return frame.canonical }
func (frame ArtifactFrame) IsRequest() bool               { return frame.frameType.IsRequest() }
func (frame ArtifactFrame) IsResponse() bool              { return frame.frameType.IsResponse() }
func (frame ArtifactFrame) IsZero() bool {
	return !frame.frameType.Valid() || frame.payload == nil || frame.canonical.IsZero()
}

// GetManifest carries the Channel authority scope explicitly. A root digest
// alone is never enough to select membership or Event/Work authorization.
type GetManifestSpec struct {
	ChannelID  model.ChannelID
	RootDigest model.Digest
}

type GetManifest struct {
	channelID  model.ChannelID
	rootDigest model.Digest
	canonical  ArtifactJSON
}

type getManifestWire struct {
	ChannelID  string `json:"channel_id"`
	RootDigest string `json:"root_digest"`
}

func NewGetManifest(spec GetManifestSpec) (GetManifest, error) {
	if spec.ChannelID.IsZero() || spec.RootDigest.IsZero() {
		return GetManifest{}, artifactFrameError("GetManifest Channel and root digest are required", nil)
	}
	canonical, err := artifactJSONFrom(getManifestWire{ChannelID: spec.ChannelID.String(),
		RootDigest: spec.RootDigest.String()},
		artifactSmallFrameBytes)
	if err != nil {
		return GetManifest{}, artifactFrameError("encode GetManifest", err)
	}
	return GetManifest{channelID: spec.ChannelID, rootDigest: spec.RootDigest,
		canonical: canonical}, nil
}

func parseGetManifest(raw []byte) (GetManifest, error) {
	var wire getManifestWire
	if err := decodeExactArtifactJSON(raw, &wire, artifactSmallFrameBytes); err != nil {
		return GetManifest{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	digest, digestErr := model.ParseDigest(wire.RootDigest)
	if channelErr != nil || digestErr != nil {
		return GetManifest{}, artifactFrameError("invalid GetManifest Channel or root digest",
			errors.Join(channelErr, digestErr))
	}
	payload, err := NewGetManifest(GetManifestSpec{ChannelID: channelID, RootDigest: digest})
	if err != nil {
		return GetManifest{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return GetManifest{}, artifactFrameError("GetManifest bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload GetManifest) ChannelID() model.ChannelID  { return payload.channelID }
func (payload GetManifest) RootDigest() model.Digest    { return payload.rootDigest }
func (payload GetManifest) CanonicalJSON() ArtifactJSON { return payload.canonical }
func (payload GetManifest) IsZero() bool {
	return payload.channelID.IsZero() || payload.rootDigest.IsZero() || payload.canonical.IsZero()
}
func (GetManifest) artifactFrameType() ArtifactFrameType { return ArtifactFrameGetManifest }

type ManifestSpec struct {
	RootDigest model.Digest
	Manifest   model.JSON
}

type Manifest struct {
	rootDigest     model.Digest
	manifestDigest model.Digest
	manifest       model.JSON
	canonical      ArtifactJSON
}

type manifestWire struct {
	ManifestBytes  string `json:"manifest_bytes"`
	ManifestDigest string `json:"manifest_digest"`
	RootDigest     string `json:"root_digest"`
}

func NewManifest(spec ManifestSpec) (Manifest, error) {
	if spec.RootDigest.IsZero() || spec.Manifest.IsZero() {
		return Manifest{}, artifactFrameError("Manifest root and canonical bytes are required", nil)
	}
	manifestBytes := spec.Manifest.Bytes()
	if len(manifestBytes) == 0 || len(manifestBytes) > artifactManifestMaximum() {
		return Manifest{}, artifactFrameError("Manifest bytes exceed the manifest limit", nil)
	}
	domainManifest, err := artifactdomain.ParseManifest(manifestBytes)
	if err != nil || domainManifest.RootDigest() != spec.RootDigest {
		return Manifest{}, artifactFrameError("Manifest bytes do not bind the declared root", err)
	}
	manifestDigest := domainManifest.ManifestDigest()
	canonical, err := artifactJSONFrom(manifestWire{
		ManifestBytes:  base64.StdEncoding.EncodeToString(manifestBytes),
		ManifestDigest: manifestDigest.String(),
		RootDigest:     spec.RootDigest.String(),
	}, maxArtifactFrameBytes())
	if err != nil {
		return Manifest{}, artifactFrameError("encode Manifest", err)
	}
	return Manifest{rootDigest: spec.RootDigest, manifestDigest: manifestDigest,
		manifest: spec.Manifest, canonical: canonical}, nil
}

func parseManifest(raw []byte) (Manifest, error) {
	var wire manifestWire
	if err := decodeExactArtifactJSON(raw, &wire, maxArtifactFrameBytes()); err != nil {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	rootDigest, rootErr := model.ParseDigest(wire.RootDigest)
	manifestDigest, digestErr := model.ParseDigest(wire.ManifestDigest)
	manifestBytes, bytesErr := base64.StdEncoding.DecodeString(wire.ManifestBytes)
	if rootErr != nil || digestErr != nil || bytesErr != nil {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	if len(manifestBytes) == 0 || len(manifestBytes) > artifactManifestMaximum() {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	if model.Sum(manifestBytes) != manifestDigest {
		return Manifest{}, errArtifactFrameManifestDigestMismatch
	}
	manifest, err := model.NewJSON(manifestBytes)
	if err != nil || !bytes.Equal(manifest.Bytes(), manifestBytes) {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	domainManifest, err := artifactdomain.ParseManifest(manifestBytes)
	if err != nil || domainManifest.TotalBytes() > artifactdomain.MaxTotalBytes {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	if domainManifest.RootDigest() != rootDigest ||
		domainManifest.ManifestDigest() != manifestDigest {
		return Manifest{}, errArtifactFrameManifestDigestMismatch
	}
	canonical, err := artifactJSONFrom(manifestWire{
		ManifestBytes:  base64.StdEncoding.EncodeToString(manifestBytes),
		ManifestDigest: manifestDigest.String(), RootDigest: rootDigest.String(),
	}, maxArtifactFrameBytes())
	if err != nil {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	if !bytes.Equal(canonical.Bytes(), raw) {
		return Manifest{}, errArtifactFrameManifestInvalid
	}
	return Manifest{rootDigest: rootDigest, manifestDigest: manifestDigest,
		manifest: manifest, canonical: canonical}, nil
}

func (payload Manifest) RootDigest() model.Digest     { return payload.rootDigest }
func (payload Manifest) ManifestDigest() model.Digest { return payload.manifestDigest }
func (payload Manifest) Manifest() model.JSON         { return payload.manifest }
func (payload Manifest) ManifestBytes() []byte        { return payload.manifest.Bytes() }
func (payload Manifest) CanonicalJSON() ArtifactJSON  { return payload.canonical }
func (payload Manifest) IsZero() bool {
	return payload.rootDigest.IsZero() || payload.manifestDigest.IsZero() ||
		payload.manifest.IsZero() || payload.canonical.IsZero()
}
func (Manifest) artifactFrameType() ArtifactFrameType { return ArtifactFrameManifest }

// GetBlock binds the block lookup to both its Channel authority scope and the
// root whose manifest must make that block reachable.
type GetBlockSpec struct {
	ChannelID   model.ChannelID
	RootDigest  model.Digest
	BlockDigest model.Digest
}

type GetBlock struct {
	channelID   model.ChannelID
	rootDigest  model.Digest
	blockDigest model.Digest
	canonical   ArtifactJSON
}

type getBlockWire struct {
	BlockDigest string `json:"block_digest"`
	ChannelID   string `json:"channel_id"`
	RootDigest  string `json:"root_digest"`
}

func NewGetBlock(spec GetBlockSpec) (GetBlock, error) {
	if spec.ChannelID.IsZero() || spec.RootDigest.IsZero() || spec.BlockDigest.IsZero() {
		return GetBlock{}, artifactFrameError("GetBlock Channel, root and block digests are required", nil)
	}
	canonical, err := artifactJSONFrom(getBlockWire{BlockDigest: spec.BlockDigest.String(),
		ChannelID: spec.ChannelID.String(), RootDigest: spec.RootDigest.String()},
		artifactSmallFrameBytes)
	if err != nil {
		return GetBlock{}, artifactFrameError("encode GetBlock", err)
	}
	return GetBlock{channelID: spec.ChannelID, rootDigest: spec.RootDigest,
		blockDigest: spec.BlockDigest, canonical: canonical}, nil
}

func parseGetBlock(raw []byte) (GetBlock, error) {
	var wire getBlockWire
	if err := decodeExactArtifactJSON(raw, &wire, artifactSmallFrameBytes); err != nil {
		return GetBlock{}, err
	}
	channelID, channelErr := model.ParseChannelID(wire.ChannelID)
	rootDigest, rootErr := model.ParseDigest(wire.RootDigest)
	blockDigest, blockErr := model.ParseDigest(wire.BlockDigest)
	if channelErr != nil || rootErr != nil || blockErr != nil {
		return GetBlock{}, artifactFrameError("invalid GetBlock Channel, root or block digest",
			errors.Join(channelErr, rootErr, blockErr))
	}
	payload, err := NewGetBlock(GetBlockSpec{ChannelID: channelID,
		RootDigest: rootDigest, BlockDigest: blockDigest})
	if err != nil {
		return GetBlock{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return GetBlock{}, artifactFrameError("GetBlock bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload GetBlock) ChannelID() model.ChannelID  { return payload.channelID }
func (payload GetBlock) RootDigest() model.Digest    { return payload.rootDigest }
func (payload GetBlock) BlockDigest() model.Digest   { return payload.blockDigest }
func (payload GetBlock) CanonicalJSON() ArtifactJSON { return payload.canonical }
func (payload GetBlock) IsZero() bool {
	return payload.channelID.IsZero() || payload.rootDigest.IsZero() ||
		payload.blockDigest.IsZero() || payload.canonical.IsZero()
}
func (GetBlock) artifactFrameType() ArtifactFrameType { return ArtifactFrameGetBlock }

type BlockSpec struct {
	BlockDigest model.Digest
	BlockBytes  []byte
}

type Block struct {
	blockDigest model.Digest
	blockBytes  []byte
	canonical   ArtifactJSON
}

type blockWire struct {
	BlockBytes  string `json:"block_bytes"`
	BlockDigest string `json:"block_digest"`
}

func NewBlock(spec BlockSpec) (Block, error) {
	if spec.BlockDigest.IsZero() || len(spec.BlockBytes) == 0 ||
		len(spec.BlockBytes) > artifactBlockMaximum() ||
		model.Sum(spec.BlockBytes) != spec.BlockDigest {
		return Block{}, artifactFrameError("Block bytes violate size or digest binding", nil)
	}
	blockBytes := append([]byte(nil), spec.BlockBytes...)
	canonical, err := artifactJSONFrom(blockWire{
		BlockBytes: base64.StdEncoding.EncodeToString(blockBytes), BlockDigest: spec.BlockDigest.String(),
	}, maxArtifactFrameBytes())
	if err != nil {
		return Block{}, artifactFrameError("encode Block", err)
	}
	return Block{blockDigest: spec.BlockDigest, blockBytes: blockBytes, canonical: canonical}, nil
}

func parseBlock(raw []byte) (Block, error) {
	var wire blockWire
	if err := decodeExactArtifactJSON(raw, &wire, maxArtifactFrameBytes()); err != nil {
		return Block{}, err
	}
	digest, digestErr := model.ParseDigest(wire.BlockDigest)
	blockBytes, bytesErr := base64.StdEncoding.DecodeString(wire.BlockBytes)
	if digestErr != nil || bytesErr != nil {
		return Block{}, errArtifactFrameBlockDigestMismatch
	}
	if len(blockBytes) == 0 || len(blockBytes) > artifactBlockMaximum() ||
		model.Sum(blockBytes) != digest {
		return Block{}, errArtifactFrameBlockDigestMismatch
	}
	blockBytes = append([]byte(nil), blockBytes...)
	canonical, err := artifactJSONFrom(blockWire{
		BlockBytes: base64.StdEncoding.EncodeToString(blockBytes), BlockDigest: digest.String(),
	}, maxArtifactFrameBytes())
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return Block{}, artifactFrameError("Block bytes are not canonical", nil)
	}
	return Block{blockDigest: digest, blockBytes: blockBytes, canonical: canonical}, nil
}

func (payload Block) BlockDigest() model.Digest   { return payload.blockDigest }
func (payload Block) BlockBytes() []byte          { return append([]byte(nil), payload.blockBytes...) }
func (payload Block) CanonicalJSON() ArtifactJSON { return payload.canonical }
func (payload Block) IsZero() bool {
	return payload.blockDigest.IsZero() || len(payload.blockBytes) == 0 || payload.canonical.IsZero()
}
func (Block) artifactFrameType() ArtifactFrameType { return ArtifactFrameBlock }

type ArtifactAck struct {
	canonical ArtifactJSON
}

type artifactAckWire struct{}

func NewArtifactAck() (ArtifactAck, error) {
	canonical, err := artifactJSONFrom(artifactAckWire{}, artifactSmallFrameBytes)
	if err != nil {
		return ArtifactAck{}, artifactFrameError("encode Ack", err)
	}
	return ArtifactAck{canonical: canonical}, nil
}

func parseArtifactAck(raw []byte) (ArtifactAck, error) {
	var wire artifactAckWire
	if err := decodeExactArtifactJSON(raw, &wire, artifactSmallFrameBytes); err != nil {
		return ArtifactAck{}, err
	}
	payload, err := NewArtifactAck()
	if err != nil {
		return ArtifactAck{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return ArtifactAck{}, artifactFrameError("Ack bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload ArtifactAck) CanonicalJSON() ArtifactJSON  { return payload.canonical }
func (payload ArtifactAck) IsZero() bool                 { return payload.canonical.IsZero() }
func (ArtifactAck) artifactFrameType() ArtifactFrameType { return ArtifactFrameAck }

func decodeExactArtifactJSON(raw []byte, destination any, maximum int) error {
	if _, err := newArtifactJSON(raw, maximum, true); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return artifactFrameError("decode exact wire value", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return artifactFrameError("wire value contains a trailing value", err)
	}
	return nil
}

func artifactFrameError(detail string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrArtifactFrame, detail)
	}
	return fmt.Errorf("%w: %s: %w", ErrArtifactFrame, detail, cause)
}
