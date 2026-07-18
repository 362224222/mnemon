package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestOpenEnrollmentGrantIsBoundedAndSecretFree(t *testing.T) {
	t.Parallel()
	id, _ := ParseGrantID("grant-one")
	channelID, _ := ParseChannelID("channel-one")
	createdAt := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	verifier, err := VerifierForEnrollment(bytes.Repeat([]byte{0x11}, EnrollmentSecretBytes), channelID, id)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := newOpenEnrollmentGrant(id, channelID, verifier,
		createdAt.Add(time.Hour), 7, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if grant.ID() != id || grant.ChannelID() != channelID || grant.MaxUses() != 7 ||
		!grant.ExpiresAt().Equal(createdAt.Add(time.Hour)) {
		t.Fatalf("grant = %#v", grant)
	}
	if _, err := newOpenEnrollmentGrant(id, channelID, grant.Verifier(), createdAt, 1, createdAt); !errors.Is(err, ErrInvariant) {
		t.Fatalf("nonfuture expiry error = %v", err)
	}
	if _, err := newOpenEnrollmentGrant(id, channelID, grant.Verifier(),
		createdAt.Add(time.Hour), 8, createdAt); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized use count error = %v", err)
	}
}

func TestEnrollmentTokenV1GoldenVector(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "r5-golden-owner", "channel-golden-v1")
	grantID, _ := ParseGrantID("grant-golden-v1")
	secret := make([]byte, EnrollmentSecretBytes)
	for index := range secret {
		secret[index] = byte(index)
	}
	payload, err := NewEnrollmentTokenPayload(EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"}, GrantID: grantID,
		BearerSecret: secret, ExpiresAt: time.Date(2026, 7, 18, 2, 2, 3, 4, time.UTC),
		MaxUses: 7, ProtocolMinVersion: 1, ProtocolMaxVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := EnrollmentTokenSigningMessage(payload.Descriptor().Descriptor().ID(), payload.Digest())
	signature := ed25519.Sign(ownerPrivate, message)
	token, err := AttachEnrollmentTokenSignature(payload, signature)
	if err != nil {
		t.Fatal(err)
	}
	verifier, _ := VerifierForEnrollment(secret, payload.Descriptor().Descriptor().ID(), grantID)
	const wantPayload = `{"bearer_secret":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=","channel_descriptor":{"descriptor":{"channel_id":"channel-golden-v1","created_at":"2026-07-18T01:02:03.000000004Z","member_limit":8,"name":"Enrollment Team","owner_peer_id":"12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B","owner_public_key":"KofW9SpW4pLrOfLxG0wZvnwzsbhyWRuEKcBiiqrAR9g=","schema_version":1},"descriptor_digest":"sha256:a53c08335ea2a9ff7f9816b66590678a8d245a698684e6c805bbe1eeb11ac369","owner_signature":"Wer1OWuPWMVFVwh7sMVIwlzhOGQrOcqNxpkoLAr9txP8PB2bpujsIK+GR/cp89ImmVLr7+xJaurUabzBuZXyAQ=="},"expires_at":"2026-07-18T02:02:03.000000004Z","grant_id":"grant-golden-v1","max_uses":7,"owner_multiaddrs":["/ip4/127.0.0.1/tcp/4001"],"protocol_max_version":1,"protocol_min_version":1,"schema_version":1}`
	const wantPayloadDigest = "sha256:6acbad2ad63c81a755a0c94f86f96daf0bc57905f46a0a62fab585a94670e2ad"
	const wantMessage = "6d6e656d6f6e2f72352f656e726f6c6c6d656e742d746f6b656e2f31006368616e6e656c2d676f6c64656e2d7631006acbad2ad63c81a755a0c94f86f96daf0bc57905f46a0a62fab585a94670e2ad"
	const wantSignature = "070204de7710bb43c9acfe1ea4bf778b0c8b46eeb80eefb06166ec133a27de886b792529b883fc46119837aa7080711db0219ac26f7f166f1b32f04fc401c507"
	const wantVerifier = "sha256:611004ece77f0f386d91b5ab99fd0c8ff436447e117e78b55650f35866e5d1f3"
	const wantTokenDigest = "sha256:9bab6fb77bc20c476169894b287e353b22ebdcc95719e70126821bab852929ae"
	if payload.RevealCanonicalJSON().String() != wantPayload || payload.Digest().String() != wantPayloadDigest ||
		hex.EncodeToString(message) != wantMessage || hex.EncodeToString(signature) != wantSignature ||
		"sha256:"+hex.EncodeToString(verifier.Bytes()) != wantVerifier ||
		Sum([]byte(token.Reveal())).String() != wantTokenDigest {
		t.Fatalf("enrollment token v1 golden drift:\npayload=%s\npayload_digest=%s\nmessage=%s\nsignature=%s\nverifier=%s\ntoken_digest=%s",
			payload.RevealCanonicalJSON().String(), payload.Digest().String(), hex.EncodeToString(message),
			hex.EncodeToString(signature), hex.EncodeToString(verifier.Bytes()),
			Sum([]byte(token.Reveal())).String())
	}
}

func TestEnrollmentTokenRoundTripRejectsTamperingAndProtectsBearer(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "token-owner", "channel-token")
	grantID, _ := ParseGrantID("grant-token")
	secret := bytes.Repeat([]byte{0x5a}, EnrollmentSecretBytes)
	payload, err := NewEnrollmentTokenPayload(EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: []string{"/ip4/127.0.0.2/tcp/4001", "/ip4/127.0.0.1/tcp/4001"},
		GrantID:         grantID, BearerSecret: secret, ExpiresAt: descriptor.Descriptor().CreatedAt().Add(time.Hour),
		MaxUses: 3, ProtocolMinVersion: 1, ProtocolMaxVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := EnrollmentTokenSigningMessage(descriptor.Descriptor().ID(), payload.Digest())
	token, err := AttachEnrollmentTokenSignature(payload, ed25519.Sign(ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEnrollmentToken(token.Reveal())
	if err != nil || VerifyEnrollmentToken(parsed) != nil || parsed.Reveal() != token.Reveal() ||
		parsed.Payload().OwnerMultiaddrs()[0] != "/ip4/127.0.0.1/tcp/4001" {
		t.Fatalf("ParseEnrollmentToken() = (%#v, %v)", parsed, err)
	}

	secret[0] ^= 0xff
	payloadSecret := parsed.Payload().BearerSecret()
	addresses := parsed.Payload().OwnerMultiaddrs()
	signature := parsed.OwnerSignature()
	payloadSecret[0] ^= 0xff
	addresses[0] = "changed"
	signature[0] ^= 0xff
	if parsed.Payload().BearerSecret()[0] != 0x5a || parsed.Payload().OwnerMultiaddrs()[0] == "changed" ||
		!bytes.Equal(parsed.OwnerSignature(), token.OwnerSignature()) {
		t.Fatal("enrollment token exposed mutable bearer material")
	}

	wrong := ed25519.Sign(ownerPrivate, append(message, 'x'))
	if _, err := AttachEnrollmentTokenSignature(payload, wrong); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong token signature error = %v", err)
	}
	tampered := tamperEnrollmentToken(t, token, func(envelope map[string]any) {
		payloadMap := envelope["payload"].(map[string]any)
		payloadMap["grant_id"] = "grant-other"
	})
	if _, err := ParseEnrollmentToken(tampered); err == nil {
		t.Fatal("tampered token payload was accepted")
	}
	tamperedSignature := tamperEnrollmentToken(t, token, func(envelope map[string]any) {
		envelope["owner_signature"] = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0}, ed25519.SignatureSize))
	})
	if _, err := ParseEnrollmentToken(tamperedSignature); err == nil {
		t.Fatal("wrong token signature was accepted")
	}
	if _, err := ParseEnrollmentToken(token.Reveal() + "="); err == nil {
		t.Fatal("padded token was accepted")
	}
	if _, err := NewEnrollmentTokenPayload(EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: []string{"bad"}, GrantID: grantID, BearerSecret: make([]byte, 31),
		ExpiresAt: descriptor.Descriptor().CreatedAt(), MaxUses: 8,
		ProtocolMinVersion: 1, ProtocolMaxVersion: 2}); err == nil {
		t.Fatal("invalid token bounds were accepted")
	}
	channelA, _ := ParseChannelID("a")
	grantBC, _ := ParseGrantID("bc")
	channelAB, _ := ParseChannelID("ab")
	grantC, _ := ParseGrantID("c")
	verifierA, _ := VerifierForEnrollment(parsed.Payload().BearerSecret(), channelA, grantBC)
	verifierB, _ := VerifierForEnrollment(parsed.Payload().BearerSecret(), channelAB, grantC)
	if verifierA == verifierB {
		t.Fatal("length-ambiguous Channel/grant bindings produced the same verifier")
	}
}

func TestEnrollmentTokenGrantBindingAndCredentialRedaction(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "binding-owner", "channel-binding")
	grantID, _ := ParseGrantID("grant-binding")
	secret := bytes.Repeat([]byte{0x6b}, EnrollmentSecretBytes)
	payload, err := NewEnrollmentTokenPayload(EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"}, GrantID: grantID,
		BearerSecret: secret, ExpiresAt: descriptor.Descriptor().CreatedAt().Add(time.Hour),
		MaxUses: 3, ProtocolMinVersion: 1, ProtocolMaxVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := EnrollmentTokenSigningMessage(descriptor.Descriptor().ID(), payload.Digest())
	token, err := AttachEnrollmentTokenSignature(payload, ed25519.Sign(ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := NewOpenEnrollmentGrantForToken(token, descriptor.Descriptor().CreatedAt())
	if err != nil || VerifyEnrollmentTokenAgainstGrant(token, grant) != nil {
		t.Fatalf("exact token/grant pair = (%#v, %v)", grant, err)
	}
	storedVerifier, err := EnrollmentVerifierFromStoredBytes(grant.Verifier().Bytes())
	if err != nil || storedVerifier != grant.Verifier() {
		t.Fatalf("stored verifier round trip = (%v, %v)", storedVerifier, err)
	}
	if _, err := EnrollmentVerifierFromStoredBytes(make([]byte, 32)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero stored verifier error = %v", err)
	}

	otherPayload, err := NewEnrollmentTokenPayload(EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: payload.OwnerMultiaddrs(), GrantID: grantID,
		BearerSecret: bytes.Repeat([]byte{0x6c}, EnrollmentSecretBytes), ExpiresAt: payload.ExpiresAt(),
		MaxUses: payload.MaxUses(), ProtocolMinVersion: 1, ProtocolMaxVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	otherMessage, _ := EnrollmentTokenSigningMessage(descriptor.Descriptor().ID(), otherPayload.Digest())
	otherToken, _ := AttachEnrollmentTokenSignature(otherPayload, ed25519.Sign(ownerPrivate, otherMessage))
	if err := VerifyEnrollmentTokenAgainstGrant(otherToken, grant); !errors.Is(err, ErrInvalid) {
		t.Fatalf("different bearer binding error = %v", err)
	}

	wrongGrantID, _ := ParseGrantID("grant-binding-other")
	wrongChannelID, _ := ParseChannelID("channel-binding-other")
	cases := []struct {
		name      string
		grantID   GrantID
		channelID ChannelID
		verifier  EnrollmentVerifier
		expiresAt time.Time
		maxUses   uint8
	}{
		{name: "grant", grantID: wrongGrantID, channelID: grant.ChannelID(), verifier: grant.Verifier(), expiresAt: grant.ExpiresAt(), maxUses: grant.MaxUses()},
		{name: "Channel", grantID: grant.ID(), channelID: wrongChannelID, verifier: grant.Verifier(), expiresAt: grant.ExpiresAt(), maxUses: grant.MaxUses()},
		{name: "expiry", grantID: grant.ID(), channelID: grant.ChannelID(), verifier: grant.Verifier(), expiresAt: grant.ExpiresAt().Add(time.Minute), maxUses: grant.MaxUses()},
		{name: "capacity", grantID: grant.ID(), channelID: grant.ChannelID(), verifier: grant.Verifier(), expiresAt: grant.ExpiresAt(), maxUses: grant.MaxUses() - 1},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			mismatch, err := newOpenEnrollmentGrant(test.grantID, test.channelID, test.verifier,
				test.expiresAt, test.maxUses, grant.CreatedAt())
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyEnrollmentTokenAgainstGrant(token, mismatch); !errors.Is(err, ErrInvalid) {
				t.Fatalf("mismatched %s error = %v", test.name, err)
			}
		})
	}

	var structured bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&structured, nil))
	spec := EnrollmentTokenSpec{BearerSecret: secret}
	logger.Info("credentials", "token", token, "payload", payload, "spec", spec,
		"grant", grant, "verifier", grant.Verifier())
	printed := fmt.Sprintf("%v|%+v|%#v|%q|%v|%+v|%#v|%v|%#v|%v|%#v|%v|%#v",
		token, token, token, token, payload, payload, payload, spec, spec, grant, grant,
		grant.Verifier(), grant.Verifier())
	printed += fmt.Sprintf("|%d|%x|%o|%d|%x", token, payload, spec, grant, grant.Verifier())
	output := printed + structured.String()
	if strings.Contains(output, token.Reveal()) ||
		strings.Contains(output, base64.StdEncoding.EncodeToString(secret)) ||
		strings.Contains(output, hex.EncodeToString(grant.Verifier().Bytes())) {
		t.Fatalf("credential leaked through formatting or slog: %s", output)
	}
	if !strings.Contains(output, "REDACTED enrollment token") ||
		!strings.Contains(output, "REDACTED enrollment verifier") {
		t.Fatalf("credential output was not explicitly redacted: %s", output)
	}
	if raw, err := json.Marshal(spec); err == nil || bytes.Contains(raw, secret) {
		t.Fatalf("transient token spec JSON serialization = (%q, %v), want fail closed", raw, err)
	}
}

func TestEnrollmentAddressSetsRequireOneThroughEightEntries(t *testing.T) {
	t.Parallel()
	descriptor, _ := enrollmentDescriptorFixture(t, "address-owner", "channel-address-bounds")
	grantID, _ := ParseGrantID("grant-address-bounds")
	base := EnrollmentTokenSpec{Descriptor: descriptor, GrantID: grantID,
		BearerSecret: bytes.Repeat([]byte{0x31}, EnrollmentSecretBytes),
		ExpiresAt:    descriptor.Descriptor().CreatedAt().Add(time.Hour), MaxUses: 1,
		ProtocolMinVersion: 1, ProtocolMaxVersion: 1}
	for _, addresses := range [][]string{nil, nineEnrollmentAddresses()} {
		base.OwnerMultiaddrs = addresses
		if _, err := NewEnrollmentTokenPayload(base); !errors.Is(err, ErrLimit) {
			t.Fatalf("owner address count %d error = %v", len(addresses), err)
		}
		if _, err := AdvertisedAddressDigest(addresses); !errors.Is(err, ErrLimit) {
			t.Fatalf("advertised address count %d error = %v", len(addresses), err)
		}
	}
}

func TestEnrollmentTranscriptProofAndJoinIdentityV1GoldenVector(t *testing.T) {
	t.Parallel()
	owner, _, _ := canonicalDescriptorIdentity(t, "r5-golden-owner")
	joiner, joinerKey, _ := canonicalDescriptorIdentity(t, "r5-golden-joiner")
	channelID, _ := ParseChannelID("channel-golden-v1")
	grantID, _ := ParseGrantID("grant-golden-v1")
	requestID, _ := ParseEnrollmentRequestID("request-golden-transcript")
	epoch, _ := ParseOriginEpoch("epoch-golden-joiner")
	rosterHead, _ := NewRecordHead(1, Sum([]byte("golden-roster-head")))
	ownerNonce := make([]byte, EnrollmentNonceBytes)
	joinerNonce := make([]byte, EnrollmentNonceBytes)
	for index := range ownerNonce {
		ownerNonce[index] = byte(0x20 + index)
		joinerNonce[index] = byte(0x40 + index)
	}
	spec := EnrollmentTranscriptSpec{ChannelID: channelID, GrantID: grantID, RequestID: requestID,
		OwnerPeerID:  owner,
		JoinerPeerID: joiner, OwnerNonce: ownerNonce, JoinerNonce: joinerNonce, SelectedVersion: 1,
		Limits: DefaultMemberLimits(), JoinerOriginEpoch: epoch, JoinerDisplayLabel: "golden-joiner",
		JoinerPublicKey: joinerKey, AdvertisedMultiaddrs: []string{
			"/ip4/127.0.0.2/tcp/4002", "/ip4/127.0.0.1/tcp/4002"}, RosterHead: rosterHead}
	transcript, err := NewEnrollmentTranscript(spec)
	if err != nil {
		t.Fatal(err)
	}
	secret := make([]byte, EnrollmentSecretBytes)
	for index := range secret {
		secret[index] = byte(index)
	}
	verifier, _ := VerifierForEnrollment(secret, channelID, grantID)
	proof, _ := ComputeEnrollmentProof(verifier, transcript)
	joinIdentity, _ := transcript.JoinIdentityDigest()
	const wantCanonical = `{"advertised_address_digest":"sha256:708b0828e53e23dca79451c1337d778cec6ec125ad9e389d272d4b57ba516d53","channel_id":"channel-golden-v1","grant_id":"grant-golden-v1","joiner_display_label":"golden-joiner","joiner_nonce":"QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl8=","joiner_origin_epoch":"epoch-golden-joiner","joiner_peer_id":"12D3KooWLzW3XvRNG5Jv84reMiXzrU1QpkwQCrw4EP8AVSv4GDKJ","joiner_public_key":"pglE843DR77AgTEa4fukzBPo7WcxqmmQtxA8IehTnL0=","limits":{"profile":"r5-hermetic-v1"},"owner_nonce":"ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8=","owner_peer_id":"12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B","request_id":"request-golden-transcript","roster_head":{"digest":"sha256:77bfab6133b82776458211d454016699343aa32574cd7e35ef161805847fdebc","revision":1},"schema_version":1,"selected_version":1}`
	const wantAddressDigest = "sha256:708b0828e53e23dca79451c1337d778cec6ec125ad9e389d272d4b57ba516d53"
	const wantProof = "sha256:6d58f962ea7c96cfc08beba546c8a1438d743de5cee82dcf02a043cb33b25df8"
	const wantJoinIdentity = "sha256:4e8c674dd866931664b3cb59c23b7b47ef75b418d9005836dbd2c6429921673d"
	if transcript.CanonicalJSON().String() != wantCanonical ||
		transcript.AdvertisedAddressDigest().String() != wantAddressDigest || proof.String() != wantProof ||
		joinIdentity.String() != wantJoinIdentity {
		t.Fatalf("enrollment transcript v1 golden drift:\ncanonical=%s\naddress_digest=%s\nproof=%s\njoin_identity=%s",
			transcript.CanonicalJSON().String(), transcript.AdvertisedAddressDigest().String(),
			proof.String(), joinIdentity.String())
	}
}

func TestEnrollmentTranscriptRoundTripProofFreshnessAndInputBounds(t *testing.T) {
	t.Parallel()
	owner, _, _ := canonicalDescriptorIdentity(t, "transcript-owner")
	joiner, joinerKey, _ := canonicalDescriptorIdentity(t, "transcript-joiner")
	other, otherKey, _ := canonicalDescriptorIdentity(t, "transcript-other")
	channelID, _ := ParseChannelID("channel-transcript")
	grantID, _ := ParseGrantID("grant-transcript")
	requestID, _ := ParseEnrollmentRequestID("request-transcript")
	epoch, _ := ParseOriginEpoch("epoch-transcript")
	rosterHead, _ := NewRecordHead(1, Sum([]byte("transcript-roster-head")))
	base := EnrollmentTranscriptSpec{ChannelID: channelID, GrantID: grantID, RequestID: requestID,
		OwnerPeerID:  owner,
		JoinerPeerID: joiner, OwnerNonce: bytes.Repeat([]byte{1}, EnrollmentNonceBytes),
		JoinerNonce: bytes.Repeat([]byte{2}, EnrollmentNonceBytes), SelectedVersion: 1,
		Limits: DefaultMemberLimits(), JoinerOriginEpoch: epoch, JoinerDisplayLabel: "joiner",
		JoinerPublicKey: joinerKey, AdvertisedMultiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"},
		RosterHead: rosterHead}
	transcript, err := NewEnrollmentTranscript(base)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEnrollmentTranscript(transcript.CanonicalJSON().Bytes())
	if err != nil || parsed.CanonicalJSON().String() != transcript.CanonicalJSON().String() {
		t.Fatalf("ParseEnrollmentTranscript() = (%#v, %v)", parsed, err)
	}
	verifier, _ := VerifierForEnrollment(bytes.Repeat([]byte{0x44}, EnrollmentSecretBytes), channelID, grantID)
	proof, _ := ComputeEnrollmentProof(verifier, transcript)
	if err := VerifyEnrollmentProof(verifier, parsed, proof); err != nil {
		t.Fatal(err)
	}
	identity, _ := transcript.JoinIdentityDigest()
	freshSpec := base
	freshSpec.OwnerNonce = bytes.Repeat([]byte{3}, EnrollmentNonceBytes)
	freshSpec.JoinerNonce = bytes.Repeat([]byte{4}, EnrollmentNonceBytes)
	freshSpec.AdvertisedMultiaddrs = []string{"/ip4/127.0.0.2/tcp/4001"}
	fresh, err := NewEnrollmentTranscript(freshSpec)
	if err != nil {
		t.Fatal(err)
	}
	freshIdentity, _ := fresh.JoinIdentityDigest()
	freshProof, _ := ComputeEnrollmentProof(verifier, fresh)
	if identity != freshIdentity || proof == freshProof {
		t.Fatalf("retry identity/proof = (%s, %s), want stable identity and fresh proof", freshIdentity, freshProof)
	}
	otherRequestID, _ := ParseEnrollmentRequestID("request-transcript-other")
	requestSpec := freshSpec
	requestSpec.RequestID = otherRequestID
	requestTranscript, err := NewEnrollmentTranscript(requestSpec)
	if err != nil {
		t.Fatal(err)
	}
	requestIdentity, _ := requestTranscript.JoinIdentityDigest()
	requestProof, _ := ComputeEnrollmentProof(verifier, requestTranscript)
	otherRosterHead, _ := NewRecordHead(1, Sum([]byte("transcript-roster-head-other")))
	rosterSpec := freshSpec
	rosterSpec.RosterHead = otherRosterHead
	rosterTranscript, err := NewEnrollmentTranscript(rosterSpec)
	if err != nil {
		t.Fatal(err)
	}
	rosterIdentity, _ := rosterTranscript.JoinIdentityDigest()
	rosterProof, _ := ComputeEnrollmentProof(verifier, rosterTranscript)
	if requestIdentity != identity || rosterIdentity != identity || requestProof == freshProof || rosterProof == freshProof {
		t.Fatal("request ID or challenge roster head was not proof-bound independently of stable join identity")
	}
	if err := VerifyEnrollmentProof(verifier, transcript, Sum([]byte("wrong"))); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong proof error = %v", err)
	}

	base.OwnerNonce[0] ^= 0xff
	base.JoinerNonce[0] ^= 0xff
	base.JoinerPublicKey[0] ^= 0xff
	if transcript.OwnerNonce()[0] != 1 || transcript.JoinerNonce()[0] != 2 ||
		bytes.Equal(transcript.JoinerPublicKey(), base.JoinerPublicKey) {
		t.Fatal("enrollment transcript did not defensively copy input")
	}
	copyNonce := transcript.OwnerNonce()
	copyKey := transcript.JoinerPublicKey()
	copyNonce[0] ^= 0xff
	copyKey[0] ^= 0xff
	if transcript.OwnerNonce()[0] != 1 || bytes.Equal(transcript.JoinerPublicKey(), copyKey) {
		t.Fatal("enrollment transcript exposed mutable storage")
	}

	bad := freshSpec
	bad.OwnerNonce = make([]byte, EnrollmentNonceBytes-1)
	if _, err := NewEnrollmentTranscript(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short nonce error = %v", err)
	}
	bad = freshSpec
	bad.SelectedVersion = 2
	if _, err := NewEnrollmentTranscript(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("alternate protocol error = %v", err)
	}
	bad = freshSpec
	bad.JoinerPublicKey = otherKey
	if _, err := NewEnrollmentTranscript(bad); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("mismatched key error = %v", err)
	}
	bad = freshSpec
	bad.JoinerPeerID, bad.JoinerPublicKey = other, otherKey
	bad.AdvertisedMultiaddrs = []string{"not-an-address"}
	if _, err := NewEnrollmentTranscript(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad address error = %v", err)
	}
	bad = freshSpec
	bad.Limits, _ = NewJSON([]byte(`{"profile":"other"}`))
	if _, err := NewEnrollmentTranscript(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("alternate limits error = %v", err)
	}
}

func TestEnrollmentRequestIDIsStableAndDomainSeparated(t *testing.T) {
	identity := Sum([]byte("stable enrollment join identity"))
	request, err := EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil || request.IsZero() {
		t.Fatalf("EnrollmentRequestIDForJoinIdentity() = (%q,%v)", request.String(), err)
	}
	replayed, err := EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil || replayed != request {
		t.Fatalf("stable request replay = (%q,%v), want %q", replayed.String(), err, request.String())
	}
	use, receipt, err := EnrollmentEvidenceIDs(identity)
	if err != nil || request.String() == use.String() || request.String() == receipt.String() {
		t.Fatalf("domain-separated evidence IDs = request %q use %q receipt %q err %v",
			request.String(), use.String(), receipt.String(), err)
	}
	if _, err := EnrollmentRequestIDForJoinIdentity(Digest{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero join identity request error = %v", err)
	}
}

func TestEnrollmentReceiptV1GoldenVectorAndVerification(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "receipt-golden-owner", "channel-receipt-golden")
	member, grantID, identity := enrollmentMemberFixture(t, descriptor, ownerPrivate,
		"receipt-golden-joiner", "grant-receipt-golden")
	receiptID, _ := ParseEnrollmentReceiptID("receipt-golden-v1")
	requestID, _ := ParseEnrollmentRequestID("request-golden-v1")
	record, err := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: descriptor.Descriptor().ID(),
		MemberPeerID: member.PeerID(), JoinIdentityDigest: identity, MemberHead: member.Head(),
		AcceptedAt: member.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	transcript := enrollmentTranscriptForMember(t, descriptor, member, grantID, requestID)
	message, _ := EnrollmentReceiptSigningMessage(record.ChannelID(), record.Digest())
	signature := ed25519.Sign(ownerPrivate, message)
	receipt, err := AttachEnrollmentReceiptSignature(record, signature)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnrollmentReceipt(descriptor, member, transcript, receipt); err != nil {
		t.Fatal(err)
	}
	const wantCanonical = `{"accepted_at":"2026-07-18T01:03:03.000000004Z","channel_id":"channel-receipt-golden","grant_id":"grant-receipt-golden","join_identity_digest":"sha256:ac98b70197ff21e213708963b40f3b077d197a84815c9ae09e27361f11820e63","member_head":{"digest":"sha256:b2adde13c6b6c4c0267627e2b2cfb54b57ec550ef6ac0dd018f1957a988a2b4b","revision":2},"member_peer_id":"12D3KooWEaog9hR5zVxqAgBV4K5JfyRwvJddypziRWUgv5DAWZ94","receipt_id":"receipt-golden-v1","request_id":"request-golden-v1","schema_version":1}`
	const wantDigest = "sha256:0391d1877b9262e0e541a40b15d461ee636daf24cb837a2142f4448cf6915205"
	const wantMessage = "6d6e656d6f6e2f72352f656e726f6c6c6d656e742d726563656970742f31006368616e6e656c2d726563656970742d676f6c64656e000391d1877b9262e0e541a40b15d461ee636daf24cb837a2142f4448cf6915205"
	const wantSignature = "18a8ac64cb1c8b782d8dd03d4c81318e11d4660a00570cce06997bd2b1f44f55edffc3a9118f025cf7808c850564e95be1fbb9449da94313922ed49dd4fd2e09"
	const wantWireDigest = "sha256:c43d77640b50b66da1bf1f5ce7d86aca23ebba01512864def99b505d510cbb74"
	if record.CanonicalJSON().String() != wantCanonical || record.Digest().String() != wantDigest ||
		hex.EncodeToString(message) != wantMessage || hex.EncodeToString(signature) != wantSignature ||
		Sum(receipt.WireJSON().Bytes()).String() != wantWireDigest {
		t.Fatalf("enrollment receipt v1 golden drift:\ncanonical=%s\ndigest=%s\nmessage=%s\nsignature=%s\nwire_digest=%s",
			record.CanonicalJSON().String(), record.Digest().String(), hex.EncodeToString(message),
			hex.EncodeToString(signature), Sum(receipt.WireJSON().Bytes()).String())
	}
}

func TestEnrollmentReceiptEvidenceIsRestartVerifiableWithoutTranscript(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "receipt-evidence-owner",
		"channel-receipt-evidence")
	member, grantID, identity := enrollmentMemberFixture(t, descriptor, ownerPrivate,
		"receipt-evidence-joiner", "grant-receipt-evidence")
	receiptID, _ := ParseEnrollmentReceiptID("receipt-evidence")
	requestID, _ := ParseEnrollmentRequestID("request-evidence")
	record, err := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: descriptor.Descriptor().ID(),
		MemberPeerID: member.PeerID(), JoinIdentityDigest: identity, MemberHead: member.Head(),
		AcceptedAt: member.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := EnrollmentReceiptSigningMessage(record.ChannelID(), record.Digest())
	receipt, _ := AttachEnrollmentReceiptSignature(record, ed25519.Sign(ownerPrivate, message))

	durableDescriptor, err := ParseSignedChannelDescriptor(descriptor.WireJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	durableMember, err := ParseMember(member.WireJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	durableReceipt, err := ParseEnrollmentReceipt(receipt.WireJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnrollmentReceiptEvidence(durableDescriptor, durableMember, durableReceipt); err != nil {
		t.Fatalf("durable receipt evidence requires no nonce/transcript: %v", err)
	}

	tamperedRecord, err := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: descriptor.Descriptor().ID(),
		MemberPeerID: member.PeerID(), JoinIdentityDigest: identity, MemberHead: member.Head(),
		AcceptedAt: member.CreatedAt().Add(time.Nanosecond)})
	if err != nil {
		t.Fatal(err)
	}
	tamperedReceipt, _ := AttachEnrollmentReceiptSignature(tamperedRecord, receipt.OwnerSignature())
	if err := VerifyEnrollmentReceiptEvidence(descriptor, member, tamperedReceipt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("tampered durable receipt error = %v", err)
	}
	otherMember, _, _ := enrollmentMemberFixture(t, descriptor, ownerPrivate,
		"receipt-evidence-other", "grant-receipt-evidence-other")
	if err := VerifyEnrollmentReceiptEvidence(descriptor, otherMember, receipt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong durable member error = %v", err)
	}
}

func TestEnrollmentReceiptRoundTripRejectsSubstitutionAndProtectsEvidence(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "receipt-owner", "channel-receipt")
	member, grantID, identity := enrollmentMemberFixture(t, descriptor, ownerPrivate,
		"receipt-joiner", "grant-receipt")
	receiptID, _ := ParseEnrollmentReceiptID("receipt-one")
	requestID, _ := ParseEnrollmentRequestID("request-one")
	record, err := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: descriptor.Descriptor().ID(),
		MemberPeerID: member.PeerID(), JoinIdentityDigest: identity, MemberHead: member.Head(),
		AcceptedAt: member.CreatedAt().Add(time.Nanosecond)})
	if err != nil {
		t.Fatal(err)
	}
	transcript := enrollmentTranscriptForMember(t, descriptor, member, grantID, requestID)
	message, _ := EnrollmentReceiptSigningMessage(record.ChannelID(), record.Digest())
	receipt, _ := AttachEnrollmentReceiptSignature(record, ed25519.Sign(ownerPrivate, message))
	parsed, err := ParseEnrollmentReceipt(receipt.WireJSON().Bytes())
	if err != nil || VerifyEnrollmentReceipt(descriptor, member, transcript, parsed) != nil ||
		parsed.MemberHead() != member.Head() || parsed.RequestID() != requestID {
		t.Fatalf("ParseEnrollmentReceipt() = (%#v, %v)", parsed, err)
	}
	signature := parsed.OwnerSignature()
	wire := parsed.WireJSON().Bytes()
	signature[0] ^= 0xff
	wire[0] ^= 0xff
	if !bytes.Equal(parsed.OwnerSignature(), receipt.OwnerSignature()) ||
		!bytes.Equal(parsed.WireJSON().Bytes(), receipt.WireJSON().Bytes()) {
		t.Fatal("enrollment receipt exposed mutable evidence")
	}

	wrong, _ := AttachEnrollmentReceiptSignature(record, ed25519.Sign(ownerPrivate, append(message, 'x')))
	if err := VerifyEnrollmentReceipt(descriptor, member, transcript, wrong); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong receipt signature error = %v", err)
	}
	tampered := bytes.Replace(receipt.WireJSON().Bytes(), []byte("receipt-one"), []byte("receipt-two"), 1)
	if _, err := ParseEnrollmentReceipt(tampered); err == nil {
		t.Fatal("tampered enrollment receipt was accepted")
	}
	otherMember, _, _ := enrollmentMemberFixture(t, descriptor, ownerPrivate,
		"receipt-other", "grant-receipt-other")
	if err := VerifyEnrollmentReceipt(descriptor, otherMember, transcript, receipt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("substituted member error = %v", err)
	}
	otherRequestID, _ := ParseEnrollmentRequestID("request-other")
	otherTranscript := enrollmentTranscriptForMember(t, descriptor, member, grantID, otherRequestID)
	if err := VerifyEnrollmentReceipt(descriptor, member, otherTranscript, receipt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("substituted request error = %v", err)
	}
	genesisHead, _ := NewRecordHead(1, Sum([]byte("genesis")))
	if _, err := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: descriptor.Descriptor().ID(),
		MemberPeerID: member.PeerID(), JoinIdentityDigest: identity, MemberHead: genesisHead,
		AcceptedAt: member.CreatedAt()}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("genesis receipt error = %v", err)
	}
}

func enrollmentDescriptorFixture(t *testing.T, ownerLabel, channelText string) (SignedChannelDescriptor, ed25519.PrivateKey) {
	t.Helper()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, ownerLabel)
	channelID, _ := ParseChannelID(channelText)
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: "Enrollment Team",
		OwnerPeerID: owner, OwnerPublicKey: ownerKey, MemberLimit: MaxMembersPerChannel,
		CreatedAt: time.Date(2026, 7, 18, 1, 2, 3, 4, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	signed, err := AttachChannelDescriptorSignature(descriptor, ed25519.Sign(ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	return signed, ownerPrivate
}

func nineEnrollmentAddresses() []string {
	return []string{
		"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.2/tcp/4001",
		"/ip4/127.0.0.3/tcp/4001", "/ip4/127.0.0.4/tcp/4001",
		"/ip4/127.0.0.5/tcp/4001", "/ip4/127.0.0.6/tcp/4001",
		"/ip4/127.0.0.7/tcp/4001", "/ip4/127.0.0.8/tcp/4001",
		"/ip4/127.0.0.9/tcp/4001",
	}
}

func enrollmentMemberFixture(t *testing.T, descriptor SignedChannelDescriptor, ownerPrivate ed25519.PrivateKey,
	joinerLabel, grantText string,
) (Member, GrantID, Digest) {
	t.Helper()
	joiner, joinerKey, _ := canonicalDescriptorIdentity(t, joinerLabel)
	grantID, _ := ParseGrantID(grantText)
	epoch, _ := ParseOriginEpoch("epoch-" + joinerLabel)
	previous := Sum([]byte("owner-genesis"))
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: descriptor.Descriptor().ID(),
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: joiner, OriginEpoch: epoch, DisplayLabel: joinerLabel, PublicKey: joinerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4002"}, Protocols: requiredMemberProtocols,
		Limits: DefaultMemberLimits(), Status: MemberActive,
		CreatedAt: descriptor.Descriptor().CreatedAt().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	member, err := AttachMemberSignature(record, ed25519.Sign(ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := EnrollmentJoinIdentityDigest(record.ChannelID(), grantID, joiner, joinerKey, epoch)
	if err != nil {
		t.Fatal(err)
	}
	return member, grantID, identity
}

func enrollmentTranscriptForMember(t *testing.T, descriptor SignedChannelDescriptor, member Member,
	grantID GrantID, requestID EnrollmentRequestID,
) EnrollmentTranscript {
	t.Helper()
	previous, ok := member.PreviousDigest()
	if !ok || member.Head().Revision() <= 1 {
		t.Fatal("joining member lacks a predecessor")
	}
	rosterHead, err := NewRecordHead(member.Head().Revision()-1, previous)
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := NewEnrollmentTranscript(EnrollmentTranscriptSpec{
		ChannelID: descriptor.Descriptor().ID(), GrantID: grantID, RequestID: requestID,
		OwnerPeerID: descriptor.Descriptor().OwnerPeerID(), JoinerPeerID: member.PeerID(),
		OwnerNonce:  bytes.Repeat([]byte{0x31}, EnrollmentNonceBytes),
		JoinerNonce: bytes.Repeat([]byte{0x32}, EnrollmentNonceBytes), SelectedVersion: 1,
		Limits: member.Limits(), JoinerOriginEpoch: member.OriginEpoch(),
		JoinerDisplayLabel: member.DisplayLabel(), JoinerPublicKey: member.PublicKey(),
		AdvertisedMultiaddrs: member.Multiaddrs(), RosterHead: rosterHead})
	if err != nil {
		t.Fatal(err)
	}
	return transcript
}

func tamperEnrollmentToken(t *testing.T, token EnrollmentToken, mutate func(map[string]any)) string {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(token.RevealWireJSON().Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	mutate(envelope)
	raw, err := CanonicalMarshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return EnrollmentTokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

func TestEnrollmentWireRejectsUnknownAndNoncanonicalFields(t *testing.T) {
	t.Parallel()
	descriptor, ownerPrivate := enrollmentDescriptorFixture(t, "wire-owner", "channel-wire")
	member, grantID, identity := enrollmentMemberFixture(t, descriptor, ownerPrivate, "wire-joiner", "grant-wire")
	receiptID, _ := ParseEnrollmentReceiptID("receipt-wire")
	requestID, _ := ParseEnrollmentRequestID("request-wire")
	record, _ := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: descriptor.Descriptor().ID(),
		MemberPeerID: member.PeerID(), JoinIdentityDigest: identity, MemberHead: member.Head(),
		AcceptedAt: member.CreatedAt()})
	message, _ := EnrollmentReceiptSigningMessage(record.ChannelID(), record.Digest())
	receipt, _ := AttachEnrollmentReceiptSignature(record, ed25519.Sign(ownerPrivate, message))
	if _, err := ParseEnrollmentReceipt(append([]byte(" "), receipt.WireJSON().Bytes()...)); err == nil {
		t.Fatal("noncanonical receipt was accepted")
	}
	unknown := strings.TrimSuffix(record.CanonicalJSON().String(), "}") + `,"unknown":true}`
	canonicalUnknown, _ := CanonicalizeJSON([]byte(unknown))
	if _, err := ParseEnrollmentReceiptRecord(canonicalUnknown); err == nil {
		t.Fatal("unknown receipt field was accepted")
	}
}

func TestEnrollmentEvidenceIDsAreStableSeparatedAndValidated(t *testing.T) {
	t.Parallel()
	identity := Sum([]byte("stable enrollment identity"))
	useID, receiptID, err := EnrollmentEvidenceIDs(identity)
	if err != nil {
		t.Fatalf("EnrollmentEvidenceIDs() error = %v", err)
	}
	secondUse, secondReceipt, err := EnrollmentEvidenceIDs(identity)
	if err != nil || secondUse != useID || secondReceipt != receiptID {
		t.Fatalf("stable IDs = (%v, %v), want (%v, %v), err=%v",
			secondUse, secondReceipt, useID, receiptID, err)
	}
	if useID.String() == receiptID.String() || !strings.HasPrefix(useID.String(), "enrollment-use-") ||
		!strings.HasPrefix(receiptID.String(), "enrollment-receipt-") {
		t.Fatalf("domain-separated IDs = %q, %q", useID.String(), receiptID.String())
	}
	if _, _, err := EnrollmentEvidenceIDs(Digest{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero join identity error = %v", err)
	}
}
