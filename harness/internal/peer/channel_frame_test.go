package peer

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelFrameCanonicalTypedRoundTrip(t *testing.T) {
	t.Parallel()

	fixture := newChannelFrameFixture(t)
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: fixture.channelID,
		GrantID: fixture.grantID, JoinerNonce: bytes.Repeat([]byte{0x11}, model.EnrollmentNonceBytes),
		SupportedVersions: []uint8{ChannelFrameVersion}, OriginEpoch: fixture.joinerEpoch,
		DisplayLabel: "joiner", AdvertisedMultiaddrs: []string{
			"/ip4/127.0.0.2/tcp/4002", "/ip4/127.0.0.1/tcp/4001",
		}})
	if err != nil {
		t.Fatal(err)
	}
	wantInitFrame := `{"payload":{"advertised_addrs":["/ip4/127.0.0.1/tcp/4001","/ip4/127.0.0.2/tcp/4002"],"channel_id":"channel-frame-codec","display_label":"joiner","grant_id":"grant-frame-codec","joiner_nonce":"ERERERERERERERERERERERERERERERERERERERERERE=","origin_epoch":"epoch-frame-joiner","supported_versions":[1]},"request_id":"request-frame-codec","type":"enroll_init","version":1}`
	initFrame, err := NewChannelFrame(fixture.requestID, init)
	if err != nil || initFrame.CanonicalJSON().String() != wantInitFrame {
		t.Fatalf("NewChannelFrame(EnrollInit) = (%s, %v)", initFrame.CanonicalJSON().String(), err)
	}

	challenge, err := NewEnrollChallenge(EnrollChallengeSpec{
		OwnerNonce:      bytes.Repeat([]byte{0x22}, model.EnrollmentNonceBytes),
		SelectedVersion: ChannelFrameVersion, Limits: model.DefaultMemberLimits(),
		RosterHead: fixture.ownerMember.Head(),
	})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := NewEnrollProof(model.Sum([]byte("canonical enrollment proof")))
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := NewEnrollAccepted(ChannelEnrollmentAccepted, fixture.joiningMember,
		fixture.roster, fixture.receipt)
	if err != nil {
		t.Fatal(err)
	}
	protocolError, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBusy,
		Retryable: true, RetryAfter: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	payloads := []struct {
		wantType ChannelFrameType
		payload  ChannelFramePayload
	}{
		{ChannelFrameEnrollInit, init},
		{ChannelFrameEnrollChallenge, challenge},
		{ChannelFrameEnrollProof, proof},
		{ChannelFrameEnrollAccepted, accepted},
		{ChannelFrameProtocolError, protocolError},
	}
	var stream bytes.Buffer
	for _, test := range payloads {
		frame, err := NewChannelFrame(fixture.requestID, test.payload)
		if err != nil {
			t.Fatalf("NewChannelFrame(%s): %v", test.wantType, err)
		}
		parsed, err := ParseChannelFrame(frame.CanonicalJSON().Bytes())
		if err != nil || parsed.Version() != ChannelFrameVersion || parsed.Type() != test.wantType ||
			parsed.RequestID() != fixture.requestID ||
			parsed.Payload().CanonicalJSON().String() != test.payload.CanonicalJSON().String() || parsed.IsZero() {
			t.Fatalf("ParseChannelFrame(%s) = (%#v, %v)", test.wantType, parsed, err)
		}
		if err := WriteChannelFrame(&stream, frame); err != nil {
			t.Fatalf("WriteChannelFrame(%s): %v", test.wantType, err)
		}
	}
	for _, test := range payloads {
		parsed, err := ReadChannelFrame(&stream)
		if err != nil || parsed.Type() != test.wantType {
			t.Fatalf("ReadChannelFrame(%s) = (%#v, %v)", test.wantType, parsed, err)
		}
	}
	if stream.Len() != 0 {
		t.Fatalf("framed reader left %d bytes", stream.Len())
	}

	parsedInit := initFrame.Payload().(EnrollInit)
	nonce := parsedInit.JoinerNonce()
	versions := parsedInit.SupportedVersions()
	addresses := parsedInit.AdvertisedMultiaddrs()
	nonce[0] ^= 0xff
	versions[0] = 99
	addresses[0] = "changed"
	if parsedInit.JoinerNonce()[0] != 0x11 || parsedInit.SupportedVersions()[0] != 1 ||
		parsedInit.AdvertisedMultiaddrs()[0] == "changed" || parsedInit.ChannelID() != fixture.channelID {
		t.Fatal("EnrollInit exposed mutable canonical input")
	}
	acceptedRoster := accepted.RosterSnapshot()
	acceptedRoster[0] = model.Member{}
	if accepted.RosterSnapshot()[0].IsZero() || accepted.Status() != ChannelEnrollmentAccepted ||
		accepted.MemberRecord().Head() != fixture.joiningMember.Head() ||
		accepted.JoinReceipt().ReceiptID() != fixture.receipt.ReceiptID() {
		t.Fatal("EnrollAccepted exposed mutable or mismatched evidence")
	}
	for _, payload := range []ChannelFramePayload{init, challenge, proof, accepted} {
		wire := payload.CanonicalJSON().String()
		for _, forbidden := range []string{"bearer_secret", "enrollment_token", "verifier", `"secret"`} {
			if strings.Contains(wire, forbidden) {
				t.Fatalf("%T carried forbidden credential field %q: %s", payload, forbidden, wire)
			}
		}
	}
}

func TestChannelFrameRejectsUnknownNoncanonicalAndMismatchedValues(t *testing.T) {
	t.Parallel()

	fixture := newChannelFrameFixture(t)
	proof, _ := NewEnrollProof(model.Sum([]byte("proof")))
	frame, _ := NewChannelFrame(fixture.requestID, proof)
	valid := frame.CanonicalJSON().Bytes()

	var envelope map[string]any
	if err := json.Unmarshal(valid, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["unknown"] = true
	unknownField, _ := model.CanonicalMarshal(envelope)
	delete(envelope, "unknown")
	envelope["type"] = "unknown"
	unknownType, _ := model.CanonicalMarshal(envelope)
	envelope["type"] = string(ChannelFrameEnrollProof)
	envelope["version"] = 2
	wrongVersion, _ := model.CanonicalMarshal(envelope)
	envelope["version"] = 1
	payload := envelope["payload"].(map[string]any)
	payload["unknown"] = true
	unknownPayloadField, _ := model.CanonicalMarshal(envelope)

	cases := [][]byte{
		nil,
		append([]byte(" "), valid...),
		append(append([]byte(nil), valid...), '\n'),
		unknownField,
		unknownType,
		wrongVersion,
		unknownPayloadField,
		[]byte(`{"payload":{"proof":"sha256:0000000000000000000000000000000000000000000000000000000000000000"},"request_id":"request-frame-codec","type":"enroll_proof","type":"enroll_init","version":1}`),
	}
	for index, raw := range cases {
		if _, err := ParseChannelFrame(raw); !errors.Is(err, ErrChannelFrame) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
	oversized := make([]byte, maxChannelFrameBytes()+1)
	if _, err := ParseChannelFrame(oversized); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("oversized envelope error = %v", err)
	}

	otherRequest, _ := model.ParseEnrollmentRequestID("request-frame-codec-other")
	accepted, _ := NewEnrollAccepted(ChannelEnrollmentAccepted, fixture.joiningMember,
		fixture.roster, fixture.receipt)
	if _, err := NewChannelFrame(otherRequest, accepted); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("accepted request substitution error = %v", err)
	}
	if _, err := NewChannelFrame(fixture.requestID, nil); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("nil payload error = %v", err)
	}
}

func TestChannelFrameLengthPrefixIsBoundedAndFailClosed(t *testing.T) {
	t.Parallel()

	requestID, _ := model.ParseEnrollmentRequestID("request-frame-length")
	payload, _ := NewEnrollProof(model.Sum([]byte("length-bound proof")))
	frame, _ := NewChannelFrame(requestID, payload)

	chunked := &channelFrameChunkWriter{limit: 3}
	if err := WriteChannelFrame(chunked, frame); err != nil {
		t.Fatal(err)
	}
	encoded := chunked.buffer.Bytes()
	if got := int(binary.BigEndian.Uint32(encoded[:channelFrameLengthBytes])); got != len(frame.CanonicalJSON().Bytes()) || len(encoded) != channelFrameLengthBytes+got {
		t.Fatalf("length prefix = %d for %d bytes", got, len(encoded))
	}
	parsed, err := ReadChannelFrame(bytes.NewReader(encoded))
	if err != nil || parsed.CanonicalJSON().String() != frame.CanonicalJSON().String() {
		t.Fatalf("ReadChannelFrame() = (%#v, %v)", parsed, err)
	}
	if err := WriteChannelFrame(channelFrameZeroWriter{}, frame); !errors.Is(err, io.ErrShortWrite) ||
		!errors.Is(err, ErrChannelFrame) {
		t.Fatalf("zero-progress writer error = %v", err)
	}

	var zero [channelFrameLengthBytes]byte
	var oversized [channelFrameLengthBytes]byte
	binary.BigEndian.PutUint32(oversized[:], uint32(maxChannelFrameBytes()+1))
	declared := make([]byte, channelFrameLengthBytes)
	binary.BigEndian.PutUint32(declared, uint32(len(frame.CanonicalJSON().Bytes())+1))
	truncatedBody := append(declared, frame.CanonicalJSON().Bytes()...)
	cases := [][]byte{
		nil,
		{0, 0},
		zero[:],
		oversized[:],
		truncatedBody,
	}
	for index, raw := range cases {
		if _, err := ReadChannelFrame(bytes.NewReader(raw)); !errors.Is(err, ErrChannelFrame) {
			t.Fatalf("case %d error = %v", index, err)
		}
	}
	counter := &channelFrameCountingReader{reader: bytes.NewReader(oversized[:])}
	if _, err := ReadChannelFrame(counter); !errors.Is(err, ErrChannelFrame) || counter.bytes != channelFrameLengthBytes {
		t.Fatalf("oversize pre-allocation fence = (%d bytes, %v)", counter.bytes, err)
	}
}

func TestChannelFramePayloadRulesAndTerminalEnrollmentEvidence(t *testing.T) {
	t.Parallel()

	fixture := newChannelFrameFixture(t)
	baseInit := EnrollInitSpec{ChannelID: fixture.channelID, GrantID: fixture.grantID,
		JoinerNonce: bytes.Repeat([]byte{1}, model.EnrollmentNonceBytes), SupportedVersions: []uint8{1},
		OriginEpoch: fixture.joinerEpoch, DisplayLabel: "joiner",
		AdvertisedMultiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"}}
	badInit := baseInit
	badInit.SupportedVersions = []uint8{2, 1}
	if _, err := NewEnrollInit(badInit); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("unsorted protocol range error = %v", err)
	}
	badInit = baseInit
	badInit.SupportedVersions = []uint8{1, 1}
	if _, err := NewEnrollInit(badInit); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("duplicate protocol version error = %v", err)
	}
	badInit = baseInit
	badInit.SupportedVersions = []uint8{2}
	if payload, err := NewEnrollInit(badInit); err != nil || payload.SupportedVersions()[0] != 2 {
		t.Fatalf("future-only protocol offer = (%#v, %v)", payload, err)
	}
	badInit = baseInit
	badInit.AdvertisedMultiaddrs = append(badInit.AdvertisedMultiaddrs,
		badInit.AdvertisedMultiaddrs[0])
	if _, err := NewEnrollInit(badInit); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("duplicate address error = %v", err)
	}
	if _, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBadProof,
		Retryable: true, RetryAfter: time.Second}); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("permanent error relabeled retryable = %v", err)
	}
	if _, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBusy,
		Retryable: true}); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("retryable error without retry_after = %v", err)
	}

	terminal := fixture.terminalJoiningMember(t)
	terminalRoster := append(append([]model.Member(nil), fixture.roster...), terminal)
	accepted, err := NewEnrollAccepted(ChannelEnrollmentMemberRevoked,
		fixture.joiningMember, terminalRoster, fixture.receipt)
	if err != nil || accepted.MemberRecord().Head() != fixture.receipt.MemberHead() ||
		accepted.RosterSnapshot()[len(terminalRoster)-1].Head() != terminal.Head() {
		t.Fatalf("terminal EnrollAccepted = (%#v, %v)", accepted, err)
	}
	if _, err := NewEnrollAccepted(ChannelEnrollmentChannelClosed,
		fixture.joiningMember, terminalRoster, fixture.receipt); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("terminal member did not take member_revoked priority: %v", err)
	}
	if _, err := NewEnrollAccepted(ChannelEnrollmentMemberRevoked,
		fixture.joiningMember, fixture.roster, fixture.receipt); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("member_revoked without terminal suffix error = %v", err)
	}
	if _, err := NewEnrollAccepted(ChannelEnrollmentAccepted,
		fixture.joiningMember, fixture.roster[1:], fixture.receipt); !errors.Is(err, ErrChannelFrame) {
		t.Fatalf("incomplete roster error = %v", err)
	}
	ownerLeft := fixture.ownerLeftMember(t)
	closedRoster := append(append([]model.Member(nil), fixture.roster...), ownerLeft)
	closed, err := NewEnrollAccepted(ChannelEnrollmentChannelClosed,
		fixture.joiningMember, closedRoster, fixture.receipt)
	if err != nil || closed.Status() != ChannelEnrollmentChannelClosed {
		t.Fatalf("closed EnrollAccepted = (%#v, %v)", closed, err)
	}
}

type channelFrameFixture struct {
	requestID     model.EnrollmentRequestID
	channelID     model.ChannelID
	grantID       model.GrantID
	descriptor    model.SignedChannelDescriptor
	owner         authorityTestPeer
	joiner        authorityTestPeer
	ownerMember   model.Member
	joiningMember model.Member
	joinerEpoch   model.OriginEpoch
	roster        []model.Member
	receipt       model.EnrollmentReceipt
	createdAt     time.Time
}

func newChannelFrameFixture(t *testing.T) channelFrameFixture {
	t.Helper()
	owner := testAuthorityPeer(t, "channel-frame-owner")
	joiner := testAuthorityPeer(t, "channel-frame-joiner")
	channelID, _ := model.ParseChannelID("channel-frame-codec")
	grantID, _ := model.ParseGrantID("grant-frame-codec")
	requestID, _ := model.ParseEnrollmentRequestID("request-frame-codec")
	ownerEpoch, _ := model.ParseOriginEpoch("epoch-frame-owner")
	joinerEpoch, _ := model.ParseOriginEpoch("epoch-frame-joiner")
	createdAt := time.Date(2026, 7, 18, 8, 9, 10, 11, time.UTC)
	descriptorRecord, err := model.NewChannelDescriptor(model.ChannelDescriptorSpec{ID: channelID,
		Name: "Frame Channel", OwnerPeerID: owner.modelID, OwnerPublicKey: owner.publicKey,
		MemberLimit: model.MaxMembersPerChannel, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	descriptorMessage, _ := model.ChannelDescriptorSigningMessage(channelID, descriptorRecord.Digest())
	descriptor, err := model.AttachChannelDescriptorSignature(descriptorRecord,
		ed25519.Sign(owner.privateKey, descriptorMessage))
	if err != nil {
		t.Fatal(err)
	}
	ownerRecord, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptorRecord.Digest(), Revision: 1, PeerID: owner.modelID,
		OriginEpoch: ownerEpoch, DisplayLabel: "owner", PublicKey: owner.publicKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4000"}, Protocols: model.RequiredMemberProtocols(),
		Limits: model.DefaultMemberLimits(), Status: model.MemberActive, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	ownerMessage, _ := model.MemberRecordSigningMessage(channelID, ownerRecord.Digest())
	ownerMember, err := model.AttachMemberSignature(ownerRecord, ed25519.Sign(owner.privateKey, ownerMessage))
	if err != nil {
		t.Fatal(err)
	}
	previous := ownerMember.Head().Digest()
	joiningRecord, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptorRecord.Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: joiner.modelID, OriginEpoch: joinerEpoch, DisplayLabel: "joiner",
		PublicKey: joiner.publicKey, Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"},
		Protocols: model.RequiredMemberProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberActive, CreatedAt: createdAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	joiningMessage, _ := model.MemberRecordSigningMessage(channelID, joiningRecord.Digest())
	joiningMember, err := model.AttachMemberSignature(joiningRecord,
		ed25519.Sign(owner.privateKey, joiningMessage))
	if err != nil {
		t.Fatal(err)
	}
	joinIdentity, err := model.EnrollmentJoinIdentityDigest(channelID, grantID, joiner.modelID,
		joiner.publicKey, joinerEpoch)
	if err != nil {
		t.Fatal(err)
	}
	receiptID, _ := model.ParseEnrollmentReceiptID("receipt-frame-codec")
	receiptRecord, err := model.NewEnrollmentReceiptRecord(model.EnrollmentReceiptRecordSpec{
		ReceiptID: receiptID, RequestID: requestID, GrantID: grantID, ChannelID: channelID,
		MemberPeerID: joiner.modelID, JoinIdentityDigest: joinIdentity, MemberHead: joiningMember.Head(),
		AcceptedAt: joiningMember.CreatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	receiptMessage, _ := model.EnrollmentReceiptSigningMessage(channelID, receiptRecord.Digest())
	receipt, err := model.AttachEnrollmentReceiptSignature(receiptRecord,
		ed25519.Sign(owner.privateKey, receiptMessage))
	if err != nil {
		t.Fatal(err)
	}
	roster := []model.Member{ownerMember, joiningMember}
	if _, err := model.NewVerifiedRoster(descriptor, roster); err != nil {
		t.Fatal(err)
	}
	if err := model.VerifyEnrollmentReceiptEvidence(descriptor, joiningMember, receipt); err != nil {
		t.Fatal(err)
	}
	return channelFrameFixture{requestID: requestID, channelID: channelID, grantID: grantID,
		descriptor: descriptor, owner: owner, joiner: joiner, ownerMember: ownerMember,
		joiningMember: joiningMember, joinerEpoch: joinerEpoch, roster: roster,
		receipt: receipt, createdAt: createdAt}
}

func (fixture channelFrameFixture) terminalJoiningMember(t *testing.T) model.Member {
	t.Helper()
	previous := fixture.joiningMember.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: fixture.channelID,
		DescriptorDigest: fixture.descriptor.Descriptor().Digest(), Revision: 3, PreviousDigest: &previous,
		PeerID: fixture.joiner.modelID, OriginEpoch: fixture.joinerEpoch, DisplayLabel: "joiner",
		PublicKey: fixture.joiner.publicKey, Multiaddrs: fixture.joiningMember.Multiaddrs(),
		Protocols: model.RequiredMemberProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberRevoked, CreatedAt: fixture.createdAt.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.MemberRecordSigningMessage(fixture.channelID, record.Digest())
	member, err := model.AttachMemberSignature(record, ed25519.Sign(fixture.owner.privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func (fixture channelFrameFixture) ownerLeftMember(t *testing.T) model.Member {
	t.Helper()
	previous := fixture.joiningMember.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: fixture.channelID,
		DescriptorDigest: fixture.descriptor.Descriptor().Digest(), Revision: 3, PreviousDigest: &previous,
		PeerID: fixture.owner.modelID, OriginEpoch: fixture.ownerMember.OriginEpoch(), DisplayLabel: "owner",
		PublicKey: fixture.owner.publicKey, Multiaddrs: fixture.ownerMember.Multiaddrs(),
		Protocols: model.RequiredMemberProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberLeft, CreatedAt: fixture.createdAt.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.MemberRecordSigningMessage(fixture.channelID, record.Digest())
	member, err := model.AttachMemberSignature(record, ed25519.Sign(fixture.owner.privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}

type channelFrameChunkWriter struct {
	buffer bytes.Buffer
	limit  int
}

func (writer *channelFrameChunkWriter) Write(value []byte) (int, error) {
	if len(value) > writer.limit {
		value = value[:writer.limit]
	}
	return writer.buffer.Write(value)
}

type channelFrameZeroWriter struct{}

func (channelFrameZeroWriter) Write([]byte) (int, error) { return 0, nil }

type channelFrameCountingReader struct {
	reader io.Reader
	bytes  int
}

func (reader *channelFrameCountingReader) Read(value []byte) (int, error) {
	count, err := reader.reader.Read(value)
	reader.bytes += count
	return count, err
}
