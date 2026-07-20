package peer

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestArtifactFrameCanonicalTypedRoundTripAndDefensiveCopies(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-artifact-frames")
	domainManifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: "result.txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: "result.txt", Mode: 0o600}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootDigest := domainManifest.RootDigest()
	manifestJSON := domainManifest.CanonicalJSON()
	blockSource := []byte{0, 1, 2, 3}
	blockDigest := model.Sum(blockSource)

	getManifest, err := NewGetManifest(GetManifestSpec{ChannelID: channelID,
		RootDigest: rootDigest})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := NewManifest(ManifestSpec{RootDigest: rootDigest, Manifest: manifestJSON})
	if err != nil {
		t.Fatal(err)
	}
	getBlock, err := NewGetBlock(GetBlockSpec{ChannelID: channelID,
		RootDigest: rootDigest, BlockDigest: blockDigest})
	if err != nil {
		t.Fatal(err)
	}
	block, err := NewBlock(BlockSpec{BlockDigest: blockDigest, BlockBytes: blockSource})
	if err != nil {
		t.Fatal(err)
	}
	blockSource[0] = 99
	ack, err := NewArtifactAck()
	if err != nil {
		t.Fatal(err)
	}
	busy, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorBusy,
		Retryable: true, RetryAfter: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	denied, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorNotAuthorized})
	if err != nil {
		t.Fatal(err)
	}

	wantGetManifest := fmt.Sprintf(`{"payload":{"channel_id":"%s","root_digest":"%s"},"type":"get_manifest","version":1}`,
		channelID.String(), rootDigest.String())
	getManifestFrame, err := NewArtifactFrame(getManifest)
	if err != nil || getManifestFrame.CanonicalJSON().String() != wantGetManifest ||
		!getManifestFrame.IsRequest() || getManifestFrame.IsResponse() {
		t.Fatalf("NewArtifactFrame(GetManifest) = (%s, %v)",
			getManifestFrame.CanonicalJSON().String(), err)
	}
	if strings.Contains(getManifestFrame.CanonicalJSON().String(), "request_id") ||
		strings.Contains(getManifestFrame.CanonicalJSON().String(), "path") {
		t.Fatal("one-request Artifact stream carried correlation or filesystem path fields")
	}

	payloads := []struct {
		wantType ArtifactFrameType
		payload  ArtifactFramePayload
	}{
		{ArtifactFrameGetManifest, getManifest},
		{ArtifactFrameManifest, manifest},
		{ArtifactFrameGetBlock, getBlock},
		{ArtifactFrameBlock, block},
		{ArtifactFrameAck, ack},
		{ArtifactFrameProtocolError, busy},
		{ArtifactFrameProtocolError, denied},
	}
	var stream bytes.Buffer
	for _, test := range payloads {
		frame, frameErr := NewArtifactFrame(test.payload)
		if frameErr != nil {
			t.Fatalf("NewArtifactFrame(%s): %v", test.wantType, frameErr)
		}
		parsed, parseErr := ParseArtifactFrame(frame.CanonicalJSON().Bytes())
		if parseErr != nil || parsed.Version() != ArtifactFrameVersion ||
			parsed.Type() != test.wantType || parsed.IsZero() ||
			parsed.Payload().CanonicalJSON().String() != test.payload.CanonicalJSON().String() {
			t.Fatalf("ParseArtifactFrame(%s) = (%#v, %v)", test.wantType, parsed, parseErr)
		}
		if test.wantType.IsRequest() != frame.IsRequest() ||
			test.wantType.IsResponse() != frame.IsResponse() {
			t.Fatalf("Artifact direction classification for %s is inconsistent", test.wantType)
		}
		if err := WriteArtifactFrame(&stream, frame); err != nil {
			t.Fatalf("WriteArtifactFrame(%s): %v", test.wantType, err)
		}
	}
	for _, test := range payloads {
		parsed, parseErr := ReadArtifactFrame(&stream)
		if parseErr != nil || parsed.Type() != test.wantType {
			t.Fatalf("ReadArtifactFrame(%s) = (%#v, %v)", test.wantType, parsed, parseErr)
		}
	}
	if stream.Len() != 0 {
		t.Fatalf("framed reader left %d bytes", stream.Len())
	}

	blockCopy := block.BlockBytes()
	blockCopy[0] = 88
	manifestCopy := manifest.ManifestBytes()
	manifestCopy[0] = 'x'
	frameCopy := getManifestFrame.CanonicalJSON().Bytes()
	frameCopy[0] = 'x'
	if got := block.BlockBytes(); !bytes.Equal(got, []byte{0, 1, 2, 3}) ||
		manifest.ManifestBytes()[0] != '{' ||
		getManifestFrame.CanonicalJSON().Bytes()[0] != '{' {
		t.Fatal("Artifact payload or frame exposed mutable bytes")
	}
	if getManifest.ChannelID() != channelID || getManifest.RootDigest() != rootDigest ||
		getBlock.ChannelID() != channelID || getBlock.RootDigest() != rootDigest ||
		getBlock.BlockDigest() != blockDigest ||
		manifest.RootDigest() != rootDigest || manifest.ManifestDigest() != model.Sum(manifestJSON.Bytes()) ||
		manifest.Manifest().String() != manifestJSON.String() || block.BlockDigest() != blockDigest ||
		busy.Code() != ArtifactErrorBusy || !busy.Retryable() || busy.RetryAfter() != 250*time.Millisecond ||
		denied.Retryable() {
		t.Fatal("typed Artifact accessors changed their frozen values")
	}
}

func TestArtifactFrameDescriptorAuthorityIsOrderedCompleteAndCodecBound(t *testing.T) {
	t.Parallel()
	channelID, _ := model.ParseChannelID("channel-artifact-descriptor")
	domainManifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: "descriptor.txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: "descriptor.txt", Mode: 0o600}},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootDigest := domainManifest.RootDigest()
	blockBytes := []byte("descriptor block")
	blockDigest := model.Sum(blockBytes)
	getManifest, getManifestErr := NewGetManifest(GetManifestSpec{ChannelID: channelID, RootDigest: rootDigest})
	manifest, manifestErr := NewManifest(ManifestSpec{RootDigest: rootDigest, Manifest: domainManifest.CanonicalJSON()})
	getBlock, getBlockErr := NewGetBlock(GetBlockSpec{ChannelID: channelID, RootDigest: rootDigest, BlockDigest: blockDigest})
	block, blockErr := NewBlock(BlockSpec{BlockDigest: blockDigest, BlockBytes: blockBytes})
	ack, ackErr := NewArtifactAck()
	denied, deniedErr := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorNotAuthorized})
	if err := errors.Join(getManifestErr, manifestErr, getBlockErr, blockErr, ackErr, deniedErr); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		frameType ArtifactFrameType
		maximum   int
		request   bool
		payload   ArtifactFramePayload
	}{
		{ArtifactFrameGetManifest, artifactSmallFrameBytes, true, getManifest},
		{ArtifactFrameManifest, maxArtifactFrameBytes(), false, manifest},
		{ArtifactFrameGetBlock, artifactSmallFrameBytes, true, getBlock},
		{ArtifactFrameBlock, maxArtifactFrameBytes(), false, block},
		{ArtifactFrameAck, artifactSmallFrameBytes, false, ack},
		{ArtifactFrameProtocolError, artifactSmallFrameBytes, false, denied},
	}
	if len(artifactFrameDescriptors) != len(tests) {
		t.Fatalf("Artifact frame descriptor count = %d, want %d", len(artifactFrameDescriptors), len(tests))
	}
	seen := make(map[ArtifactFrameType]struct{}, len(tests))
	for index, test := range tests {
		descriptor := artifactFrameDescriptors[index]
		_, duplicate := seen[descriptor.frameType]
		seen[descriptor.frameType] = struct{}{}
		parsed, parseErr := descriptor.codec.parse(test.payload.CanonicalJSON().Bytes())
		frameType, canonical, canonicalErr := canonicalArtifactPayload(parsed)
		if duplicate || descriptor.frameType != test.frameType || !test.frameType.Valid() ||
			artifactFrameMaximum(test.frameType) != test.maximum ||
			test.frameType.IsRequest() != test.request || test.frameType.IsResponse() == test.request ||
			parseErr != nil || frameType != test.frameType ||
			canonicalErr != nil || canonical.String() != test.payload.CanonicalJSON().String() {
			t.Fatalf("Artifact frame descriptor %d parity failed: %#v, parse=%v canonical=%v", index, descriptor, parseErr, canonicalErr)
		}
	}
	ackPayload, _ := parseArtifactAck([]byte(`{}`))
	_, _, unboundErr := canonicalArtifactPayload(struct{ ArtifactAck }{ackPayload})
	if unknown := ArtifactFrameType("unknown"); unknown.Valid() || unknown.IsRequest() || unknown.IsResponse() ||
		artifactFrameMaximum(unknown) != 0 || !errors.Is(unboundErr, ErrArtifactFrame) {
		t.Fatal("unknown Artifact frame type or payload implementation acquired descriptor policy")
	}
}

func TestArtifactFrameRejectsUnknownNonCanonicalAndUnboundValues(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-artifact-rejection")
	rootDigest := model.Sum([]byte("artifact-rejection-root"))
	blockBytes := []byte("block")
	blockDigest := model.Sum(blockBytes)
	validGet := fmt.Sprintf(`{"payload":{"channel_id":"%s","root_digest":"%s"},"type":"get_manifest","version":1}`,
		channelID.String(), rootDigest.String())
	validBlockGet := fmt.Sprintf(`{"payload":{"block_digest":"%s","channel_id":"%s","root_digest":"%s"},"type":"get_block","version":1}`,
		blockDigest.String(), channelID.String(), rootDigest.String())

	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "unsupported version", raw: []byte(strings.Replace(validGet, `"version":1`, `"version":2`, 1))},
		{name: "unknown type", raw: []byte(strings.Replace(validGet, `"get_manifest"`, `"execute"`, 1))},
		{name: "unknown envelope field", raw: []byte(fmt.Sprintf(
			`{"payload":{"channel_id":"%s","root_digest":"%s"},"path":"/tmp/x","type":"get_manifest","version":1}`,
			channelID.String(), rootDigest.String()))},
		{name: "unknown request field", raw: []byte(strings.Replace(validGet, `"root_digest":`, `"command":"write","root_digest":`, 1))},
		{name: "missing request Channel", raw: []byte(fmt.Sprintf(
			`{"payload":{"root_digest":"%s"},"type":"get_manifest","version":1}`, rootDigest.String()))},
		{name: "missing request root", raw: []byte(fmt.Sprintf(
			`{"payload":{"channel_id":"%s"},"type":"get_manifest","version":1}`, channelID.String()))},
		{name: "wrong Channel type", raw: []byte(fmt.Sprintf(
			`{"payload":{"channel_id":1,"root_digest":"%s"},"type":"get_manifest","version":1}`,
			rootDigest.String()))},
		{name: "invalid Channel identifier", raw: []byte(fmt.Sprintf(
			`{"payload":{"channel_id":"%s","root_digest":"%s"},"type":"get_manifest","version":1}`,
			"channel invalid", rootDigest.String()))},
		{name: "uppercase digest", raw: []byte(strings.Replace(validGet, rootDigest.String(), strings.ToUpper(rootDigest.String()), 1))},
		{name: "noncanonical envelope whitespace", raw: []byte(" " + validGet)},
		{name: "noncanonical envelope key order", raw: []byte(fmt.Sprintf(
			`{"version":1,"type":"get_manifest","payload":{"channel_id":"%s","root_digest":"%s"}}`,
			channelID.String(), rootDigest.String()))},
		{name: "trailing JSON value", raw: []byte(validGet + `{}`)},
		{name: "duplicate field", raw: []byte(strings.Replace(validGet, `"version":1`, `"version":1,"version":1`, 1))},
		{name: "mismatched payload type", raw: []byte(fmt.Sprintf(
			`{"payload":{"block_digest":"%s"},"type":"get_manifest","version":1}`, blockDigest.String()))},
		{name: "GetBlock missing Channel", raw: []byte(fmt.Sprintf(
			`{"payload":{"block_digest":"%s","root_digest":"%s"},"type":"get_block","version":1}`,
			blockDigest.String(), rootDigest.String()))},
		{name: "GetBlock missing root", raw: []byte(fmt.Sprintf(
			`{"payload":{"block_digest":"%s","channel_id":"%s"},"type":"get_block","version":1}`,
			blockDigest.String(), channelID.String()))},
		{name: "GetBlock missing block", raw: []byte(fmt.Sprintf(
			`{"payload":{"channel_id":"%s","root_digest":"%s"},"type":"get_block","version":1}`,
			channelID.String(), rootDigest.String()))},
		{name: "Channel in GetBlock root", raw: []byte(strings.Replace(validBlockGet,
			rootDigest.String(), channelID.String(), 1))},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseArtifactFrame(test.raw); !errors.Is(err, ErrArtifactFrame) {
				t.Fatalf("ParseArtifactFrame() error = %v", err)
			}
		})
	}

	if _, err := NewGetManifest(GetManifestSpec{RootDigest: rootDigest}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("missing-Channel GetManifest error = %v", err)
	}
	if _, err := NewGetManifest(GetManifestSpec{ChannelID: channelID}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("missing-root GetManifest error = %v", err)
	}
	if _, err := NewGetBlock(GetBlockSpec{RootDigest: rootDigest,
		BlockDigest: blockDigest}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("missing-Channel GetBlock error = %v", err)
	}
	if _, err := NewGetBlock(GetBlockSpec{ChannelID: channelID,
		BlockDigest: blockDigest}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("missing-root GetBlock error = %v", err)
	}
	if _, err := NewGetBlock(GetBlockSpec{ChannelID: channelID,
		RootDigest: rootDigest}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("missing-block GetBlock error = %v", err)
	}
	if _, err := NewManifest(ManifestSpec{RootDigest: rootDigest}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("zero Manifest error = %v", err)
	}
	if _, err := NewBlock(BlockSpec{BlockDigest: blockDigest, BlockBytes: []byte("wrong")}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("unbound Block error = %v", err)
	}
	if _, err := NewBlock(BlockSpec{BlockDigest: model.Sum(nil), BlockBytes: []byte{}}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("zero-length Block error = %v", err)
	}

	nonCanonicalManifest := []byte(`{"z":1, "a":2}`)
	nonCanonicalManifestFrame := artifactFrameEnvelopeForTest(t, ArtifactFrameManifest, map[string]any{
		"manifest_bytes":  base64.StdEncoding.EncodeToString(nonCanonicalManifest),
		"manifest_digest": model.Sum(nonCanonicalManifest).String(),
		"root_digest":     rootDigest.String(),
	})
	if _, err := ParseArtifactFrame(nonCanonicalManifestFrame); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("noncanonical Manifest bytes error = %v", err)
	}

	canonicalManifest := []byte(`{"a":2,"z":1}`)
	wrongManifestDigest := artifactFrameEnvelopeForTest(t, ArtifactFrameManifest, map[string]any{
		"manifest_bytes":  base64.StdEncoding.EncodeToString(canonicalManifest),
		"manifest_digest": model.Sum([]byte("wrong")).String(),
		"root_digest":     rootDigest.String(),
	})
	if _, err := ParseArtifactFrame(wrongManifestDigest); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("unbound Manifest digest error = %v", err)
	}

	wrongBlockDigest := artifactFrameEnvelopeForTest(t, ArtifactFrameBlock, map[string]any{
		"block_bytes":  base64.StdEncoding.EncodeToString(blockBytes),
		"block_digest": model.Sum([]byte("wrong")).String(),
	})
	if _, err := ParseArtifactFrame(wrongBlockDigest); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("unbound Block digest error = %v", err)
	}
	zeroLengthBlock := artifactFrameEnvelopeForTest(t, ArtifactFrameBlock, map[string]any{
		"block_bytes": "", "block_digest": model.Sum(nil).String(),
	})
	if _, err := ParseArtifactFrame(zeroLengthBlock); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("zero-length Block frame error = %v", err)
	}
	invalidBlockEncoding := artifactFrameEnvelopeForTest(t, ArtifactFrameBlock, map[string]any{
		"block_bytes": "***", "block_digest": blockDigest.String(),
	})
	if _, err := ParseArtifactFrame(invalidBlockEncoding); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("invalid Block encoding error = %v", err)
	}

	ackWithField := []byte(`{"payload":{"path":"workspace/file"},"type":"ack","version":1}`)
	if _, err := ParseArtifactFrame(ackWithField); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("open Ack payload error = %v", err)
	}
}

func TestArtifactFrameReturnsOnlyClosedContentDiagnostics(t *testing.T) {
	t.Parallel()

	blockBytes := []byte("closed diagnostic block")
	blockDigest := model.Sum(blockBytes)
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: "closed.txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: "closed.txt", Mode: 0o600, SizeBytes: uint64(len(blockBytes)),
			Blocks: []artifactdomain.ManifestBlock{{Digest: blockDigest,
				LengthBytes: uint64(len(blockBytes))}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	invalidManifest := []byte(`{"remote-frame-canary":true}`)
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "manifest structure", raw: artifactFrameEnvelopeForTest(t, ArtifactFrameManifest,
			manifestWire{ManifestBytes: base64.StdEncoding.EncodeToString(invalidManifest),
				ManifestDigest: model.Sum(invalidManifest).String(),
				RootDigest:     manifest.RootDigest().String()}), want: errArtifactFrameManifestInvalid},
		{name: "manifest content digest", raw: artifactFrameEnvelopeForTest(t, ArtifactFrameManifest,
			manifestWire{ManifestBytes: base64.StdEncoding.EncodeToString(manifest.CanonicalJSON().Bytes()),
				ManifestDigest: model.Sum([]byte("wrong manifest digest")).String(),
				RootDigest:     manifest.RootDigest().String()}), want: errArtifactFrameManifestDigestMismatch},
		{name: "manifest root binding", raw: artifactFrameEnvelopeForTest(t, ArtifactFrameManifest,
			manifestWire{ManifestBytes: base64.StdEncoding.EncodeToString(manifest.CanonicalJSON().Bytes()),
				ManifestDigest: manifest.ManifestDigest().String(),
				RootDigest:     model.Sum([]byte("wrong root digest")).String()}),
			want: errArtifactFrameManifestDigestMismatch},
		{name: "block content digest", raw: artifactFrameEnvelopeForTest(t, ArtifactFrameBlock,
			blockWire{BlockBytes: base64.StdEncoding.EncodeToString(blockBytes),
				BlockDigest: model.Sum([]byte("wrong block digest")).String()}),
			want: errArtifactFrameBlockDigestMismatch},
		{name: "block byte encoding", raw: artifactFrameEnvelopeForTest(t, ArtifactFrameBlock,
			blockWire{BlockBytes: "%remote-frame-canary", BlockDigest: blockDigest.String()}),
			want: errArtifactFrameBlockDigestMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseArtifactFrame(test.raw)
			if !errors.Is(err, ErrArtifactFrame) || !errors.Is(err, test.want) ||
				strings.Contains(err.Error(), "remote-frame-canary") {
				t.Fatalf("closed content diagnostic = %v", err)
			}
		})
	}
}

func TestArtifactReadRequestsPreserveExactChannelRootAuthorityTuples(t *testing.T) {
	t.Parallel()

	channelA, _ := model.ParseChannelID("channel-artifact-authority-a")
	channelB, _ := model.ParseChannelID("channel-artifact-authority-b")
	rootA := model.Sum([]byte("artifact-authority-root-a"))
	rootB := model.Sum([]byte("artifact-authority-root-b"))
	blockDigest := model.Sum([]byte("artifact-authority-block"))

	manifestCases := []GetManifestSpec{
		{ChannelID: channelA, RootDigest: rootA},
		{ChannelID: channelB, RootDigest: rootA},
		{ChannelID: channelA, RootDigest: rootB},
	}
	manifestWires := make(map[string]struct{}, len(manifestCases))
	for _, spec := range manifestCases {
		request, err := NewGetManifest(spec)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := NewArtifactFrame(request)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf(`{"payload":{"channel_id":"%s","root_digest":"%s"},"type":"get_manifest","version":1}`,
			spec.ChannelID.String(), spec.RootDigest.String())
		if frame.CanonicalJSON().String() != want {
			t.Fatalf("GetManifest authority wire = %s, want %s", frame.CanonicalJSON().String(), want)
		}
		parsed, err := ParseArtifactFrame(frame.CanonicalJSON().Bytes())
		payload, ok := parsed.Payload().(GetManifest)
		if err != nil || !ok || payload.ChannelID() != spec.ChannelID ||
			payload.RootDigest() != spec.RootDigest {
			t.Fatalf("parsed GetManifest authority = (%#v, %v)", parsed, err)
		}
		manifestWires[frame.CanonicalJSON().String()] = struct{}{}
	}
	if len(manifestWires) != len(manifestCases) {
		t.Fatal("cross-Channel or cross-root GetManifest authority tuples collapsed")
	}

	blockCases := []GetBlockSpec{
		{ChannelID: channelA, RootDigest: rootA, BlockDigest: blockDigest},
		{ChannelID: channelB, RootDigest: rootA, BlockDigest: blockDigest},
		{ChannelID: channelA, RootDigest: rootB, BlockDigest: blockDigest},
	}
	blockWires := make(map[string]struct{}, len(blockCases))
	for _, spec := range blockCases {
		request, err := NewGetBlock(spec)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := NewArtifactFrame(request)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf(`{"payload":{"block_digest":"%s","channel_id":"%s","root_digest":"%s"},"type":"get_block","version":1}`,
			spec.BlockDigest.String(), spec.ChannelID.String(), spec.RootDigest.String())
		if frame.CanonicalJSON().String() != want {
			t.Fatalf("GetBlock authority wire = %s, want %s", frame.CanonicalJSON().String(), want)
		}
		parsed, err := ParseArtifactFrame(frame.CanonicalJSON().Bytes())
		payload, ok := parsed.Payload().(GetBlock)
		if err != nil || !ok || payload.ChannelID() != spec.ChannelID ||
			payload.RootDigest() != spec.RootDigest || payload.BlockDigest() != spec.BlockDigest {
			t.Fatalf("parsed GetBlock authority = (%#v, %v)", parsed, err)
		}
		blockWires[frame.CanonicalJSON().String()] = struct{}{}
	}
	if len(blockWires) != len(blockCases) {
		t.Fatal("cross-Channel or cross-root GetBlock authority tuples collapsed")
	}
}

func TestArtifactFrameExactHermeticContentAndFrameLimits(t *testing.T) {
	limits := HermeticLimits()
	if artifactManifestMaximum() != limits.ArtifactManifestBytes ||
		artifactBlockMaximum() != limits.ArtifactBlockBytes ||
		maxArtifactFrameBytes() != limits.DirectFrameBytes ||
		artifactManifestMaximum() != 4<<20 || artifactBlockMaximum() != 1<<20 ||
		maxArtifactFrameBytes() != 8<<20 {
		t.Fatalf("Artifact frame limits are not the Hermetic profile: %#v", limits)
	}
	channelID, _ := model.ParseChannelID("channel-artifact-limit")

	entries := make([]artifactdomain.ManifestEntry, 0, artifactdomain.MaxEntries)
	entries = append(entries, artifactdomain.ManifestEntry{Kind: artifactdomain.EntryDirectory,
		LogicalPath: "bundle", Mode: 0o700})
	for index := 1; index < artifactdomain.MaxEntries; index++ {
		prefix := fmt.Sprintf("bundle/%04d-", index)
		const escapedPathBytes = 80
		logical := prefix + strings.Repeat(`\`, escapedPathBytes) +
			strings.Repeat("p", artifactdomain.MaxLogicalPathBytes-len(prefix)-escapedPathBytes)
		content := []byte(fmt.Sprintf("block-%d", index))
		entries = append(entries, artifactdomain.ManifestEntry{Kind: artifactdomain.EntryFile,
			LogicalPath: logical, Mode: 0o600, SizeBytes: uint64(len(content)),
			Blocks: []artifactdomain.ManifestBlock{{Digest: model.Sum(content),
				LengthBytes: uint64(len(content))}}})
	}
	domainManifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryDirectory, RootPath: "bundle", Entries: entries,
	})
	if err != nil {
		t.Fatalf("large valid manifest: %v", err)
	}
	rootDigest := domainManifest.RootDigest()
	manifest, err := NewManifest(ManifestSpec{RootDigest: rootDigest,
		Manifest: domainManifest.CanonicalJSON()})
	if err != nil {
		t.Fatalf("NewManifest(large valid): %v", err)
	}
	manifestFrame, err := NewArtifactFrame(manifest)
	if err != nil || len(manifestFrame.CanonicalJSON().Bytes()) <= model.MaxCanonicalJSONBytes ||
		len(manifestFrame.CanonicalJSON().Bytes()) > maxArtifactFrameBytes() {
		t.Fatalf("large valid Manifest frame bytes = %d, manifest = %d, error = %v",
			len(manifestFrame.CanonicalJSON().Bytes()), len(domainManifest.CanonicalJSON().Bytes()), err)
	}
	if len(domainManifest.CanonicalJSON().Bytes()) > artifactManifestMaximum() {
		t.Fatalf("domain Manifest exceeds transport limit: %d", len(domainManifest.CanonicalJSON().Bytes()))
	}
	parsedManifest, err := ParseArtifactFrame(manifestFrame.CanonicalJSON().Bytes())
	if err != nil || parsedManifest.Type() != ArtifactFrameManifest {
		t.Fatalf("ParseArtifactFrame(large Manifest) = (%#v, %v)", parsedManifest, err)
	}

	otherDomainManifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryFile, RootPath: "other.txt",
		Entries: []artifactdomain.ManifestEntry{{Kind: artifactdomain.EntryFile,
			LogicalPath: "other.txt", Mode: 0o600}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewManifest(ManifestSpec{RootDigest: rootDigest,
		Manifest: otherDomainManifest.CanonicalJSON()}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("Manifest root binding error = %v", err)
	}

	tooLargeManifest := []byte(`"` + strings.Repeat("x", artifactManifestMaximum()) + `"`)
	tooLargeManifestFrame := artifactFrameEnvelopeForTest(t, ArtifactFrameManifest, map[string]any{
		"manifest_bytes":  base64.StdEncoding.EncodeToString(tooLargeManifest),
		"manifest_digest": model.Sum(tooLargeManifest).String(),
		"root_digest":     rootDigest.String(),
	})
	if _, err := ParseArtifactFrame(tooLargeManifestFrame); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("oversized Manifest bytes error = %v", err)
	}

	blockBytes := bytes.Repeat([]byte{0xa5}, artifactBlockMaximum())
	block, err := NewBlock(BlockSpec{BlockDigest: model.Sum(blockBytes), BlockBytes: blockBytes})
	if err != nil {
		t.Fatalf("NewBlock(exact limit): %v", err)
	}
	blockFrame, err := NewArtifactFrame(block)
	if err != nil {
		t.Fatalf("NewArtifactFrame(exact-limit Block): %v", err)
	}
	if parsed, parseErr := ParseArtifactFrame(blockFrame.CanonicalJSON().Bytes()); parseErr != nil ||
		parsed.Type() != ArtifactFrameBlock {
		t.Fatalf("ParseArtifactFrame(exact-limit Block) = (%#v, %v)", parsed, parseErr)
	}
	tooLargeBlock := append(blockBytes, 0)
	if _, err := NewBlock(BlockSpec{BlockDigest: model.Sum(tooLargeBlock), BlockBytes: tooLargeBlock}); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("oversized Block bytes error = %v", err)
	}

	oversizedEnvelope := make([]byte, maxArtifactFrameBytes()+1)
	if _, err := ParseArtifactFrame(oversizedEnvelope); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("oversized direct frame error = %v", err)
	}
	oversizedRequest := artifactFrameEnvelopeForTest(t, ArtifactFrameGetManifest, map[string]any{
		"channel_id": channelID.String(), "root_digest": rootDigest.String(),
		"padding": strings.Repeat("x", artifactSmallFrameBytes),
	})
	if len(oversizedRequest) <= artifactSmallFrameBytes {
		t.Fatal("request limit fixture is not oversized")
	}
	if _, err := ParseArtifactFrame(oversizedRequest); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("oversized request frame error = %v", err)
	}
}

func TestArtifactProtocolErrorIsClosedAndHasStableRetryPolicy(t *testing.T) {
	t.Parallel()

	for _, code := range []ArtifactProtocolErrorCode{ArtifactErrorNotAuthorized, ArtifactErrorCorrupt} {
		payload, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: code})
		if err != nil || payload.Code() != code || payload.Retryable() || payload.RetryAfter() != 0 {
			t.Fatalf("NewArtifactProtocolError(%s) = (%#v, %v)", code, payload, err)
		}
	}
	busy, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorBusy,
		Retryable: true, RetryAfter: HermeticLimits().ArtifactRequestTimeout})
	if err != nil || !busy.Retryable() {
		t.Fatalf("bounded busy ProtocolError = (%#v, %v)", busy, err)
	}

	invalid := []ArtifactProtocolErrorSpec{
		{},
		{Code: "future"},
		{Code: "not_found"},
		{Code: "corrupt_source"},
		{Code: ArtifactErrorBusy},
		{Code: ArtifactErrorBusy, Retryable: true},
		{Code: ArtifactErrorBusy, Retryable: true, RetryAfter: time.Microsecond},
		{Code: ArtifactErrorBusy, Retryable: true,
			RetryAfter: HermeticLimits().ArtifactRequestTimeout + time.Millisecond},
		{Code: ArtifactErrorNotAuthorized, Retryable: true, RetryAfter: time.Millisecond},
		{Code: ArtifactErrorNotAuthorized, RetryAfter: time.Millisecond},
	}
	for index, spec := range invalid {
		if _, err := NewArtifactProtocolError(spec); !errors.Is(err, ErrArtifactFrame) {
			t.Errorf("invalid ProtocolError %d error = %v", index, err)
		}
	}

	for _, code := range []string{"future", "not_found", "corrupt_source"} {
		openError := []byte(fmt.Sprintf(
			`{"payload":{"code":"%s","retry_after_ms":0,"retryable":false},"type":"protocol_error","version":1}`,
			code))
		if _, err := ParseArtifactFrame(openError); !errors.Is(err, ErrArtifactFrame) {
			t.Fatalf("open ProtocolError code %q error = %v", code, err)
		}
	}
}

func TestArtifactFrameLengthPrefixShortWritesAndEarlySizeFence(t *testing.T) {
	t.Parallel()

	ack, _ := NewArtifactAck()
	frame, _ := NewArtifactFrame(ack)
	var stream bytes.Buffer
	if err := WriteArtifactFrame(&stream, frame); err != nil {
		t.Fatal(err)
	}
	encoded := stream.Bytes()
	if got := binary.BigEndian.Uint32(encoded[:artifactFrameLengthBytes]); got != uint32(len(frame.CanonicalJSON().Bytes())) {
		t.Fatalf("length prefix = %d", got)
	}
	if parsed, err := ReadArtifactFrame(bytes.NewReader(encoded)); err != nil ||
		parsed.Type() != ArtifactFrameAck {
		t.Fatalf("ReadArtifactFrame() = (%#v, %v)", parsed, err)
	}

	chunked := &artifactFrameChunkWriter{maximum: 2}
	if err := WriteArtifactFrame(chunked, frame); err != nil ||
		!bytes.Equal(chunked.buffer.Bytes(), encoded) {
		t.Fatalf("chunked WriteArtifactFrame() = (%x, %v)", chunked.buffer.Bytes(), err)
	}
	if err := WriteArtifactFrame(artifactFrameZeroWriter{}, frame); !errors.Is(err, ErrArtifactFrame) ||
		!errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-progress writer error = %v", err)
	}
	if _, err := ReadArtifactFrame(bytes.NewReader(encoded[:3])); !errors.Is(err, ErrArtifactFrame) ||
		!errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short prefix error = %v", err)
	}
	if _, err := ReadArtifactFrame(bytes.NewReader(encoded[:len(encoded)-1])); !errors.Is(err, ErrArtifactFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short payload error = %v", err)
	}
	if _, err := ReadArtifactFrame(nil); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("nil reader error = %v", err)
	}

	var prefix [artifactFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(maxArtifactFrameBytes()+1))
	oversized := bytes.NewReader(append(prefix[:], []byte("must-not-be-read")...))
	if _, err := ReadArtifactFrame(oversized); !errors.Is(err, ErrArtifactFrame) {
		t.Fatalf("oversized declared frame error = %v", err)
	}
	if oversized.Len() != len("must-not-be-read") {
		t.Fatalf("size fence consumed %d payload bytes", len("must-not-be-read")-oversized.Len())
	}
}

func TestArtifactFrameStreamReservationCoversDecodeAndReleases(t *testing.T) {
	t.Parallel()

	ack, _ := NewArtifactAck()
	frame, _ := NewArtifactFrame(ack)
	var encoded bytes.Buffer
	if err := WriteArtifactFrame(&encoded, frame); err != nil {
		t.Fatal(err)
	}
	scope := &artifactFrameTestScope{}
	parsed, release, err := readReservedArtifactFrame(&encoded, scope, artifactSmallFrameBytes)
	wantReserved := len(frame.CanonicalJSON().Bytes())
	if err != nil || parsed.Type() != ArtifactFrameAck || release == nil ||
		scope.reserved != wantReserved || scope.released != 0 ||
		scope.priority != network.ReservationPriorityAlways {
		t.Fatalf("readReservedArtifactFrame() = (%#v, %v), scope=%#v", parsed, err, scope)
	}
	release()
	release()
	if scope.released != wantReserved {
		t.Fatalf("idempotent release = %d, want %d", scope.released, wantReserved)
	}

	encoded.Reset()
	_ = WriteArtifactFrame(&encoded, frame)
	reserveError := errors.New("budget exhausted")
	rejected := &artifactFrameTestScope{reserveError: reserveError}
	if _, release, err := readReservedArtifactFrame(&encoded, rejected, artifactSmallFrameBytes); !errors.Is(err, ErrArtifactFrame) || !errors.Is(err, reserveError) || release != nil {
		t.Fatalf("rejected reservation = (release=%t, %v)", release != nil, err)
	}
	if encoded.Len() != wantReserved {
		t.Fatalf("reservation failure consumed %d payload bytes", wantReserved-encoded.Len())
	}

	badRaw := []byte(`{"payload":{},"type":"future","version":1}`)
	encoded.Reset()
	var prefix [artifactFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(badRaw)))
	encoded.Write(prefix[:])
	encoded.Write(badRaw)
	parseScope := &artifactFrameTestScope{}
	if _, release, err := readReservedArtifactFrame(&encoded, parseScope, artifactSmallFrameBytes); !errors.Is(err, ErrArtifactFrame) || release != nil || parseScope.released != len(badRaw) {
		t.Fatalf("parse failure reservation = (release=%t, %v), scope=%#v",
			release != nil, err, parseScope)
	}

	invalidManifest := []byte(`{"remote-reservation-canary":true}`)
	badManifest := artifactFrameEnvelopeForTest(t, ArtifactFrameManifest, manifestWire{
		ManifestBytes:  base64.StdEncoding.EncodeToString(invalidManifest),
		ManifestDigest: model.Sum(invalidManifest).String(),
		RootDigest:     model.Sum([]byte("reservation root")).String(),
	})
	encoded.Reset()
	binary.BigEndian.PutUint32(prefix[:], uint32(len(badManifest)))
	encoded.Write(prefix[:])
	encoded.Write(badManifest)
	manifestScope := &artifactFrameTestScope{}
	if _, release, err := readReservedArtifactFrame(&encoded, manifestScope,
		maxArtifactFrameBytes()); !errors.Is(err, errArtifactFrameManifestInvalid) ||
		release != nil || manifestScope.reserved != len(badManifest) ||
		manifestScope.released != len(badManifest) ||
		strings.Contains(err.Error(), "remote-reservation-canary") {
		t.Fatalf("typed parse failure reservation = (release=%t, %v), scope=%#v",
			release != nil, err, manifestScope)
	}
}

func artifactFrameEnvelopeForTest(t testing.TB, frameType ArtifactFrameType,
	payload any,
) []byte {
	t.Helper()
	payloadJSON, err := artifactJSONFrom(payload, maxArtifactFrameBytes())
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := artifactJSONFrom(artifactFrameWire{Payload: payloadJSON.Bytes(),
		Type: frameType, Version: ArtifactFrameVersion}, maxArtifactFrameBytes())
	if err != nil {
		t.Fatal(err)
	}
	return envelope.Bytes()
}

type artifactFrameChunkWriter struct {
	buffer  bytes.Buffer
	maximum int
}

func (writer *artifactFrameChunkWriter) Write(value []byte) (int, error) {
	if len(value) > writer.maximum {
		value = value[:writer.maximum]
	}
	return writer.buffer.Write(value)
}

type artifactFrameZeroWriter struct{}

func (artifactFrameZeroWriter) Write([]byte) (int, error) { return 0, nil }

type artifactFrameTestScope struct {
	reserved     int
	released     int
	priority     uint8
	reserveError error
}

func (scope *artifactFrameTestScope) ReserveMemory(size int, priority uint8) error {
	scope.priority = priority
	if scope.reserveError != nil {
		return scope.reserveError
	}
	scope.reserved += size
	return nil
}

func (scope *artifactFrameTestScope) ReleaseMemory(size int) { scope.released += size }
func (scope *artifactFrameTestScope) Stat() network.ScopeStat {
	return network.ScopeStat{Memory: int64(scope.reserved - scope.released)}
}
func (scope *artifactFrameTestScope) BeginSpan() (network.ResourceScopeSpan, error) {
	return scope, nil
}
func (scope *artifactFrameTestScope) Done() {}

var _ network.ResourceScopeSpan = (*artifactFrameTestScope)(nil)
var _ io.Writer = (*artifactFrameChunkWriter)(nil)
