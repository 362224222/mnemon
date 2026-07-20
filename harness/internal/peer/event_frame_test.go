package peer

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestEventFrameCanonicalTypedRoundTripAndCopies(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-events-codec")
	originPeerID, _ := model.ParsePeerID("peer-events-origin")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-origin")
	publications := []model.SignedPublication{
		newEventFramePublication(t, channelID, originPeerID, originEpoch, 1, 8),
		newEventFramePublication(t, channelID, originPeerID, originEpoch, 2, 8),
	}
	pull, err := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, AfterChannelSequence: 0, Limit: 32})
	if err != nil {
		t.Fatal(err)
	}
	page, err := NewPullPage(PullPageSpec{Publications: publications,
		ScannedChannelSequence: 2, SourceFloor: 1, SourceHead: 2, OriginEpoch: originEpoch})
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := NewCursorAck(CursorAckSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, ContiguousChannelSequence: 2})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := NewEventAck()
	if err != nil {
		t.Fatal(err)
	}
	historyGap, err := NewEventProtocolError(EventProtocolErrorSpec{
		Code: EventErrorHistoryGap, SourceFloor: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	busy, err := NewEventProtocolError(EventProtocolErrorSpec{Code: EventErrorBusy,
		Retryable: true, RetryAfter: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	wantPull := `{"payload":{"after_channel_seq":0,"channel_id":"channel-events-codec","limit":32,"origin_epoch":"epoch-events-origin"},"type":"pull_request","version":1}`
	pullFrame, err := NewEventFrame(pull)
	if err != nil || pullFrame.CanonicalJSON().String() != wantPull {
		t.Fatalf("NewEventFrame(PullRequest) = (%s, %v)", pullFrame.CanonicalJSON().String(), err)
	}
	if strings.Contains(pullFrame.CanonicalJSON().String(), "request_id") {
		t.Fatal("one-request Events stream unexpectedly carried a request ID")
	}
	pageFrame, err := NewEventFrame(page)
	if err != nil {
		t.Fatal(err)
	}
	var pageEnvelope struct {
		Payload struct {
			Publications []json.RawMessage `json:"publications"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(pageFrame.CanonicalJSON().Bytes(), &pageEnvelope); err != nil ||
		len(pageEnvelope.Payload.Publications) != len(publications) {
		t.Fatalf("decode PullPage exact publication array: %v", err)
	}
	for index := range publications {
		if !bytes.Equal(pageEnvelope.Payload.Publications[index], publications[index].WireJSON().Bytes()) {
			t.Fatalf("PullPage publication %d changed its exact signed wire", index+1)
		}
	}

	payloads := []struct {
		wantType EventFrameType
		payload  EventFramePayload
	}{
		{EventFramePullRequest, pull},
		{EventFramePullPage, page},
		{EventFrameCursorAck, cursor},
		{EventFrameAck, ack},
		{EventFrameProtocolError, historyGap},
		{EventFrameProtocolError, busy},
	}
	var stream bytes.Buffer
	for _, test := range payloads {
		frame, frameErr := NewEventFrame(test.payload)
		if frameErr != nil {
			t.Fatalf("NewEventFrame(%s): %v", test.wantType, frameErr)
		}
		parsed, parseErr := ParseEventFrame(frame.CanonicalJSON().Bytes())
		if parseErr != nil || parsed.Version() != EventFrameVersion || parsed.Type() != test.wantType ||
			parsed.Payload().CanonicalJSON().String() != test.payload.CanonicalJSON().String() || parsed.IsZero() {
			t.Fatalf("ParseEventFrame(%s) = (%#v, %v)", test.wantType, parsed, parseErr)
		}
		if writeErr := WriteEventFrame(&stream, frame); writeErr != nil {
			t.Fatalf("WriteEventFrame(%s): %v", test.wantType, writeErr)
		}
	}
	for _, test := range payloads {
		parsed, parseErr := ReadEventFrame(&stream)
		if parseErr != nil || parsed.Type() != test.wantType {
			t.Fatalf("ReadEventFrame(%s) = (%#v, %v)", test.wantType, parsed, parseErr)
		}
	}
	if stream.Len() != 0 {
		t.Fatalf("framed reader left %d bytes", stream.Len())
	}

	gotPublications := page.Publications()
	gotPublications[0] = model.PublicationEvidence{}
	wireCopy := page.Publications()[0].WireJSON().Bytes()
	wireCopy[0] = 'x'
	if page.Publications()[0].Digest() != publications[0].Digest() ||
		page.Publications()[0].WireJSON().Bytes()[0] != '{' || page.OriginEpoch() != originEpoch ||
		page.ScannedChannelSequence() != 2 || page.SourceFloor() != 1 || page.SourceHead() != 2 {
		t.Fatal("PullPage exposed mutable publication state or changed cursor fields")
	}
	if pull.ChannelID() != channelID || pull.OriginEpoch() != originEpoch ||
		pull.AfterChannelSequence() != 0 || pull.Limit() != 32 ||
		cursor.ChannelID() != channelID || cursor.OriginEpoch() != originEpoch ||
		cursor.ContiguousChannelSequence() != 2 || historyGap.SourceFloor() != 2 ||
		historyGap.Retryable() || busy.RetryAfter() != 250*time.Millisecond {
		t.Fatal("typed Events payload accessors changed their frozen values")
	}
}

func TestPullPageRetainsUnsupportedPublicationEvidence(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-events-evidence")
	originPeerID, _ := model.ParsePeerID("peer-events-evidence")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-evidence")
	base := newEventFramePublication(t, channelID, originPeerID, originEpoch, 1, 8)
	privateKey := eventFramePublicationPrivateKey()
	publicKey := privateKey.Public().(ed25519.PublicKey)

	tests := []struct {
		name       string
		wantSchema uint64
		mutate     func(map[string]any)
	}{
		{name: "schema v2", wantSchema: model.SchemaVersion + 1, mutate: func(body map[string]any) {
			body["schema_version"] = json.Number("2")
			body["future_semantics"] = map[string]any{"mode": "opaque"}
		}},
		{name: "schema v1 unknown Event type", wantSchema: model.SchemaVersion, mutate: func(body map[string]any) {
			body["event"].(map[string]any)["event_type"] = "review.future"
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			rawPublication := resignEventFramePublication(t, base.WireJSON().Bytes(),
				privateKey, test.mutate)
			rawFrame := eventFramePullPage(t, originEpoch, 1, 1, 1, rawPublication)
			frame, err := ParseEventFrame(rawFrame)
			if err != nil {
				t.Fatalf("ParseEventFrame() error = %v", err)
			}
			page, ok := frame.Payload().(PullPage)
			if !ok || frame.Type() != EventFramePullPage ||
				!bytes.Equal(frame.CanonicalJSON().Bytes(), rawFrame) {
				t.Fatalf("unsupported PullPage frame = %#v", frame)
			}
			publications := page.Publications()
			if len(publications) != 1 {
				t.Fatalf("PublicationEvidence count = %d", len(publications))
			}
			evidence := publications[0]
			if evidence.IsZero() || evidence.IsSupported() || evidence.SchemaVersion() != test.wantSchema ||
				evidence.ChannelID() != channelID || evidence.OriginPeerID() != originPeerID ||
				evidence.OriginEpoch() != originEpoch || evidence.ChannelSequence() != 1 ||
				!bytes.Equal(evidence.WireJSON().Bytes(), rawPublication) {
				t.Fatalf("unsupported PublicationEvidence = %#v", evidence)
			}
			if err := model.VerifyPublicationEvidence(publicKey, evidence); err != nil {
				t.Fatalf("VerifyPublicationEvidence() error = %v", err)
			}
			if _, err := model.ParseSignedPublication(rawPublication); err == nil {
				t.Fatal("strict Gossip publication parser accepted unsupported evidence")
			}

			publications[0] = model.PublicationEvidence{}
			wireCopy := evidence.WireJSON().Bytes()
			wireCopy[0] = 'x'
			if page.Publications()[0].IsZero() || page.Publications()[0].WireJSON().Bytes()[0] != '{' {
				t.Fatal("PullPage exposed mutable PublicationEvidence state")
			}
		})
	}
}

func TestPullPageRejectsMalformedEvidenceAsACompleteFrame(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-events-evidence-reject")
	otherChannelID, _ := model.ParseChannelID("channel-events-evidence-other")
	originPeerID, _ := model.ParsePeerID("peer-events-evidence-reject")
	otherPeerID, _ := model.ParsePeerID("peer-events-evidence-other")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-evidence-reject")
	otherEpoch, _ := model.ParseOriginEpoch("epoch-events-evidence-other")
	privateKey := eventFramePublicationPrivateKey()
	first := newEventFramePublication(t, channelID, originPeerID, originEpoch, 1, 8)
	second := newEventFramePublication(t, channelID, originPeerID, originEpoch, 2, 8)
	third := newEventFramePublication(t, channelID, originPeerID, originEpoch, 3, 8)
	otherChannel := newEventFramePublication(t, otherChannelID, originPeerID, originEpoch, 2, 8)
	otherOrigin := newEventFramePublication(t, channelID, otherPeerID, originEpoch, 2, 8)
	otherOriginEpoch := newEventFramePublication(t, channelID, originPeerID, otherEpoch, 2, 8)

	wrongDigest := eventFrameCanonicalMutation(t, first.WireJSON().String(), func(value map[string]any) {
		value["publication_digest"] = model.Sum([]byte("wrong PullPage evidence body")).String()
	})
	missingStableHeader := resignEventFramePublication(t, first.WireJSON().Bytes(), privateKey,
		func(body map[string]any) { delete(body, "channel_id") })
	validFrame := eventFramePullPage(t, originEpoch, 1, 1, 1, first.WireJSON().Bytes())
	noncanonical := bytes.Replace(validFrame, []byte(`"publications":[{`),
		[]byte(`"publications":[ {`), 1)
	if bytes.Equal(noncanonical, validFrame) {
		t.Fatal("could not construct a noncanonical nested publication frame")
	}
	oversizedPublication := append([]byte{'"'}, bytes.Repeat([]byte{'x'}, model.MaxPublicationBytes)...)
	oversizedPublication = append(oversizedPublication, '"')

	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "wrong digest", raw: eventFramePullPage(t, originEpoch, 1, 1, 1, wrongDigest)},
		{name: "noncanonical", raw: noncanonical},
		{name: "missing stable header", raw: eventFramePullPage(t, originEpoch, 1, 1, 1, missingStableHeader)},
		{name: "sparse sequence", raw: eventFramePullPage(t, originEpoch, 3, 1, 3,
			first.WireJSON().Bytes(), third.WireJSON().Bytes())},
		{name: "cross Channel", raw: eventFramePullPage(t, originEpoch, 2, 1, 2,
			first.WireJSON().Bytes(), otherChannel.WireJSON().Bytes())},
		{name: "cross origin", raw: eventFramePullPage(t, originEpoch, 2, 1, 2,
			first.WireJSON().Bytes(), otherOrigin.WireJSON().Bytes())},
		{name: "cross origin epoch", raw: eventFramePullPage(t, originEpoch, 2, 1, 2,
			first.WireJSON().Bytes(), otherOriginEpoch.WireJSON().Bytes())},
		{name: "oversized publication", raw: eventFramePullPage(t, originEpoch, 1, 1, 1,
			oversizedPublication)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseEventFrame(test.raw); !errors.Is(err, ErrEventFrame) {
				t.Fatalf("ParseEventFrame() error = %v", err)
			}
		})
	}

	valid := eventFramePullPage(t, originEpoch, 2, 1, 2,
		first.WireJSON().Bytes(), second.WireJSON().Bytes())
	if _, err := ParseEventFrame(valid); err != nil {
		t.Fatalf("valid complete PullPage frame error = %v", err)
	}
}

func TestEventFrameEmptyPageAndAckHaveExactBytes(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-events-empty")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-empty")
	page, err := NewPullPage(PullPageSpec{SourceFloor: 1, SourceHead: 0,
		ScannedChannelSequence: 0, OriginEpoch: originEpoch})
	if err != nil {
		t.Fatalf("empty NewPullPage(): %v", err)
	}
	pageFrame, err := NewEventFrame(page)
	if err != nil {
		t.Fatal(err)
	}
	wantPage := `{"payload":{"origin_epoch":"epoch-events-empty","publications":[],"scanned_channel_seq":0,"source_floor":1,"source_head":0},"type":"pull_page","version":1}`
	if pageFrame.CanonicalJSON().String() != wantPage || len(page.Publications()) != 0 {
		t.Fatalf("empty page bytes = %s", pageFrame.CanonicalJSON().String())
	}
	ack, _ := NewEventAck()
	ackFrame, err := NewEventFrame(ack)
	if err != nil || ackFrame.CanonicalJSON().String() != `{"payload":{},"type":"ack","version":1}` {
		t.Fatalf("Ack frame = (%s, %v)", ackFrame.CanonicalJSON().String(), err)
	}
	cursor, err := NewCursorAck(CursorAckSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, ContiguousChannelSequence: 0})
	if err != nil || cursor.ContiguousChannelSequence() != 0 {
		t.Fatalf("zero baseline CursorAck = (%#v, %v)", cursor, err)
	}
}

func TestEventFrameRejectsEnvelopeAndPayloadSchemaDrift(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-events-schema")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-schema")
	pull, _ := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, Limit: 1})
	frame, _ := NewEventFrame(pull)
	canonical := frame.CanonicalJSON().String()

	mutations := map[string][]byte{
		"noncanonical whitespace": []byte(strings.Replace(canonical, `{"payload":`, `{ "payload":`, 1)),
		"unknown envelope field": eventFrameCanonicalMutation(t, canonical, func(value map[string]any) {
			value["request_id"] = "forbidden"
		}),
		"unknown payload field": eventFrameCanonicalMutation(t, canonical, func(value map[string]any) {
			value["payload"].(map[string]any)["origin_peer_id"] = "peer-forbidden"
		}),
		"unsupported version": eventFrameCanonicalMutation(t, canonical, func(value map[string]any) {
			value["version"] = json.Number("2")
		}),
		"unknown type": eventFrameCanonicalMutation(t, canonical, func(value map[string]any) {
			value["type"] = "pull_response"
		}),
		"type payload mismatch": eventFrameCanonicalMutation(t, canonical, func(value map[string]any) {
			value["type"] = string(EventFrameAck)
		}),
		"null Ack":               []byte(`{"payload":null,"type":"ack","version":1}`),
		"duplicate envelope key": []byte(`{"payload":{},"type":"ack","type":"ack","version":1}`),
	}
	for name, raw := range mutations {
		if _, err := ParseEventFrame(raw); !errors.Is(err, ErrEventFrame) {
			t.Errorf("%s: ParseEventFrame() error = %v", name, err)
		}
	}

	oversizedSmall := []byte(fmt.Sprintf(`{"payload":{"after_channel_seq":0,"channel_id":"%s","extra":"%s","limit":1,"origin_epoch":"%s"},"type":"pull_request","version":1}`,
		channelID.String(), strings.Repeat("x", eventSmallFrameBytes), originEpoch.String()))
	if _, err := ParseEventFrame(oversizedSmall); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("oversized small frame error = %v", err)
	}
	oversizedPage := []byte(fmt.Sprintf(`{"payload":{"origin_epoch":"%s","padding":"%s","publications":[],"scanned_channel_seq":0,"source_floor":1,"source_head":0},"type":"pull_page","version":1}`,
		originEpoch.String(), strings.Repeat("x", eventPullPageFrameBytes)))
	if _, err := ParseEventFrame(oversizedPage); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("oversized PullPage frame error = %v", err)
	}
}

func TestPullAndCursorConstructorsEnforceSQLiteAndPageBounds(t *testing.T) {
	t.Parallel()

	channelID, _ := model.ParseChannelID("channel-events-bounds")
	otherChannelID, _ := model.ParseChannelID("channel-events-other")
	originPeerID, _ := model.ParsePeerID("peer-events-bounds")
	otherPeerID, _ := model.ParsePeerID("peer-events-other")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-bounds")
	otherEpoch, _ := model.ParseOriginEpoch("epoch-events-other")

	badPulls := []PullRequestSpec{
		{OriginEpoch: originEpoch, Limit: 1},
		{ChannelID: channelID, Limit: 1},
		{ChannelID: channelID, OriginEpoch: originEpoch},
		{ChannelID: channelID, OriginEpoch: originEpoch, Limit: 33},
		{ChannelID: channelID, OriginEpoch: originEpoch, Limit: 1,
			AfterChannelSequence: model.MaxSQLiteInteger + 1},
	}
	for index, spec := range badPulls {
		if _, err := NewPullRequest(spec); !errors.Is(err, ErrEventFrame) {
			t.Errorf("bad PullRequest %d error = %v", index, err)
		}
	}
	if _, err := NewCursorAck(CursorAckSpec{ChannelID: channelID,
		OriginEpoch: originEpoch, ContiguousChannelSequence: model.MaxSQLiteInteger + 1}); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("oversized CursorAck error = %v", err)
	}

	first := newEventFramePublication(t, channelID, originPeerID, originEpoch, 1, 4)
	second := newEventFramePublication(t, channelID, originPeerID, originEpoch, 2, 4)
	third := newEventFramePublication(t, channelID, originPeerID, originEpoch, 3, 4)
	otherChannel := newEventFramePublication(t, otherChannelID, originPeerID, originEpoch, 2, 4)
	otherOrigin := newEventFramePublication(t, channelID, otherPeerID, originEpoch, 2, 4)
	otherEpochPublication := newEventFramePublication(t, channelID, originPeerID, otherEpoch, 2, 4)
	tooMany := make([]model.SignedPublication, eventPullPageLimit+1)
	for index := range tooMany {
		tooMany[index] = first
	}
	badPages := []PullPageSpec{
		{OriginEpoch: originEpoch, SourceFloor: 0, SourceHead: 0},
		{OriginEpoch: originEpoch, SourceFloor: 3, SourceHead: 1, ScannedChannelSequence: 2},
		{OriginEpoch: originEpoch, SourceFloor: 2, SourceHead: 2, ScannedChannelSequence: 0},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 1, ScannedChannelSequence: 2},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 1,
			ScannedChannelSequence: 1, Publications: tooMany},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 2,
			ScannedChannelSequence: 2, Publications: []model.SignedPublication{first, otherChannel}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 2,
			ScannedChannelSequence: 2, Publications: []model.SignedPublication{first, otherOrigin}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 2,
			ScannedChannelSequence: 2, Publications: []model.SignedPublication{first, otherEpochPublication}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 2,
			ScannedChannelSequence: 2, Publications: []model.SignedPublication{second, first}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 2,
			ScannedChannelSequence: 2, Publications: []model.SignedPublication{first, first}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 3,
			ScannedChannelSequence: 3, Publications: []model.SignedPublication{first, third}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 3,
			ScannedChannelSequence: 3, Publications: []model.SignedPublication{first, second}},
		{OriginEpoch: originEpoch, SourceFloor: 2, SourceHead: 2,
			ScannedChannelSequence: 2, Publications: []model.SignedPublication{first}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 1,
			ScannedChannelSequence: 1, Publications: []model.SignedPublication{second}},
		{OriginEpoch: originEpoch, SourceFloor: 1, SourceHead: 1,
			ScannedChannelSequence: 1, Publications: []model.SignedPublication{{}}},
	}
	for index, spec := range badPages {
		if _, err := NewPullPage(spec); !errors.Is(err, ErrEventFrame) {
			t.Errorf("bad PullPage %d error = %v", index, err)
		}
	}

	page, err := NewPullPage(PullPageSpec{OriginEpoch: originEpoch, SourceFloor: 1,
		SourceHead: 2, ScannedChannelSequence: 2,
		Publications: []model.SignedPublication{first, second}})
	if err != nil || len(page.Publications()) != 2 {
		t.Fatalf("bounded PullPage = (%#v, %v)", page, err)
	}
}

func TestPullPageRejectsAggregatePublicationLimit(t *testing.T) {
	channelID, _ := model.ParseChannelID("channel-events-page-limit")
	originPeerID, _ := model.ParsePeerID("peer-events-page-limit")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-page-limit")
	publications := make([]model.SignedPublication, 0, eventPullPageLimit)
	total := 0
	for sequence := uint64(1); total <= eventPullPageFrameBytes; sequence++ {
		publication := newEventFramePublication(t, channelID, originPeerID, originEpoch,
			sequence, 54<<10)
		publications = append(publications, publication)
		total += len(publication.WireJSON().Bytes())
		if len(publications) > eventPullPageLimit {
			t.Fatalf("could not exceed aggregate page bound within %d publications", eventPullPageLimit)
		}
	}
	if _, err := NewPullPage(PullPageSpec{OriginEpoch: originEpoch, SourceFloor: 1,
		SourceHead: uint64(len(publications)), ScannedChannelSequence: uint64(len(publications)),
		Publications: publications}); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("aggregate publication limit error = %v (count=%d bytes=%d)", err, len(publications), total)
	}
}

func TestPullPageRejectsEnvelopeOverheadBeyondPageLimit(t *testing.T) {
	channelID, _ := model.ParseChannelID("channel-events-envelope-limit")
	originPeerID, _ := model.ParsePeerID("peer-events-envelope-limit")
	originEpoch, _ := model.ParseOriginEpoch("epoch-events-envelope-limit")
	publications := make([]model.SignedPublication, 0, eventPullPageLimit)
	total := 0
	for sequence := uint64(1); sequence <= 16; sequence++ {
		publication := newEventFramePublication(t, channelID, originPeerID, originEpoch,
			sequence, 60<<10)
		publications = append(publications, publication)
		total += len(publication.WireJSON().Bytes())
	}
	if total >= eventPullPageFrameBytes {
		t.Fatalf("fixed publication prefix unexpectedly consumes %d bytes", total)
	}
	low, high := 0, 60<<10
	var last model.SignedPublication
	for low <= high {
		middle := low + (high-low)/2
		candidate := newEventFramePublication(t, channelID, originPeerID, originEpoch, 17, middle)
		if total+len(candidate.WireJSON().Bytes()) <= eventPullPageFrameBytes {
			last = candidate
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if last.Digest().IsZero() {
		t.Fatal("could not construct an aggregate just below the publication-byte limit")
	}
	publications = append(publications, last)
	wirePublications := make([]json.RawMessage, len(publications))
	total = 0
	for index, publication := range publications {
		wirePublications[index] = publication.WireJSON().Bytes()
		total += len(wirePublications[index])
	}
	payload, err := model.JSONFrom(pullPageWire{OriginEpoch: originEpoch.String(),
		Publications: wirePublications, ScannedChannelSequence: 17, SourceFloor: 1, SourceHead: 17})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := model.JSONFrom(eventFrameWire{Payload: payload.Bytes(),
		Type: EventFramePullPage, Version: EventFrameVersion})
	if err != nil {
		t.Fatal(err)
	}
	if total > eventPullPageFrameBytes || len(envelope.Bytes()) <= eventPullPageFrameBytes {
		t.Fatalf("boundary construction = publications %d, envelope %d", total, len(envelope.Bytes()))
	}
	if _, err := NewPullPage(PullPageSpec{OriginEpoch: originEpoch, SourceFloor: 1,
		SourceHead: 17, ScannedChannelSequence: 17,
		Publications: publications}); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("envelope overhead limit error = %v", err)
	}
}

func TestEventProtocolErrorStableRetryPolicy(t *testing.T) {
	t.Parallel()

	tests := []EventProtocolErrorSpec{
		{Code: EventErrorBusy, Retryable: true, RetryAfter: time.Millisecond},
		{Code: EventErrorHistoryGap, SourceFloor: 3},
		{Code: EventErrorNotOrigin},
		{Code: EventErrorNotMember},
		{Code: EventErrorMemberRevoked},
		{Code: EventErrorChannelClosed},
		{Code: EventErrorOriginEpochMismatch},
	}
	for _, spec := range tests {
		payload, err := NewEventProtocolError(spec)
		if err != nil || payload.Code() != spec.Code || payload.Retryable() != spec.Retryable ||
			payload.RetryAfter() != spec.RetryAfter || payload.SourceFloor() != spec.SourceFloor {
			t.Errorf("NewEventProtocolError(%s) = (%#v, %v)", spec.Code, payload, err)
		}
	}

	invalid := []EventProtocolErrorSpec{
		{},
		{Code: EventErrorBusy},
		{Code: EventErrorBusy, Retryable: true},
		{Code: EventErrorBusy, Retryable: true, RetryAfter: time.Microsecond},
		{Code: EventErrorNotMember, Retryable: true, RetryAfter: time.Millisecond},
		{Code: EventErrorHistoryGap},
		{Code: EventErrorHistoryGap, SourceFloor: model.MaxSQLiteInteger + 1},
		{Code: EventErrorNotOrigin, SourceFloor: 1},
	}
	for index, spec := range invalid {
		if _, err := NewEventProtocolError(spec); !errors.Is(err, ErrEventFrame) {
			t.Errorf("invalid ProtocolError %d error = %v", index, err)
		}
	}

	raw := []byte(`{"code":"busy","retry_after":9223372036854775807,"retryable":true,"source_floor":0}`)
	if _, err := parseEventProtocolError(raw); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("overflowing retry_after error = %v", err)
	}
}

func TestEventFrameLengthPrefixShortIOAndSizeFence(t *testing.T) {
	t.Parallel()

	ack, _ := NewEventAck()
	frame, _ := NewEventFrame(ack)
	var stream bytes.Buffer
	if err := WriteEventFrame(&stream, frame); err != nil {
		t.Fatal(err)
	}
	encoded := stream.Bytes()
	if got := binary.BigEndian.Uint32(encoded[:eventFrameLengthBytes]); got != uint32(len(frame.CanonicalJSON().Bytes())) {
		t.Fatalf("length prefix = %d", got)
	}
	if parsed, err := ReadEventFrame(bytes.NewReader(encoded)); err != nil || parsed.Type() != EventFrameAck {
		t.Fatalf("ReadEventFrame() = (%#v, %v)", parsed, err)
	}

	chunked := &eventFrameChunkWriter{maximum: 2}
	if err := WriteEventFrame(chunked, frame); err != nil || !bytes.Equal(chunked.buffer.Bytes(), encoded) {
		t.Fatalf("chunked WriteEventFrame() = (%x, %v)", chunked.buffer.Bytes(), err)
	}
	if err := WriteEventFrame(eventFrameZeroWriter{}, frame); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("zero-progress writer error = %v", err)
	}
	if _, err := ReadEventFrame(bytes.NewReader(encoded[:3])); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("short prefix error = %v", err)
	}
	if _, err := ReadEventFrame(bytes.NewReader(encoded[:len(encoded)-1])); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("short payload error = %v", err)
	}

	var prefix [eventFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(maxEventFrameBytes()+1))
	oversized := bytes.NewReader(append(prefix[:], []byte("must-not-be-read")...))
	if _, err := ReadEventFrame(oversized); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("oversized declared frame error = %v", err)
	}
	if oversized.Len() != len("must-not-be-read") {
		t.Fatalf("size fence consumed %d payload bytes", len("must-not-be-read")-oversized.Len())
	}
	if _, err := ReadEventFrame(nil); !errors.Is(err, ErrEventFrame) {
		t.Fatalf("nil reader error = %v", err)
	}
}

func TestEventFrameStreamScopeReservationCoversDecodeAndReleases(t *testing.T) {
	t.Parallel()

	ack, _ := NewEventAck()
	frame, _ := NewEventFrame(ack)
	var encoded bytes.Buffer
	if err := WriteEventFrame(&encoded, frame); err != nil {
		t.Fatal(err)
	}
	scope := &eventFrameTestScope{}
	parsed, release, err := readReservedEventFrame(&encoded, scope, eventSmallFrameBytes)
	wantReserved := len(frame.CanonicalJSON().Bytes())
	if err != nil || parsed.Type() != EventFrameAck || release == nil ||
		scope.reserved != wantReserved || scope.released != 0 || scope.priority != network.ReservationPriorityAlways {
		t.Fatalf("readReservedEventFrame() = (%#v, %v), scope=%#v", parsed, err, scope)
	}
	release()
	release()
	if scope.released != wantReserved {
		t.Fatalf("idempotent release = %d, want %d", scope.released, wantReserved)
	}

	encoded.Reset()
	_ = WriteEventFrame(&encoded, frame)
	rejected := &eventFrameTestScope{reserveError: errors.New("budget exhausted")}
	if _, release, err := readReservedEventFrame(&encoded, rejected, eventSmallFrameBytes); !errors.Is(err, ErrEventFrame) || release != nil {
		t.Fatalf("rejected reservation = (release=%t, %v)", release != nil, err)
	}
	if encoded.Len() != wantReserved {
		t.Fatalf("reservation failure consumed %d payload bytes", wantReserved-encoded.Len())
	}
}

func newEventFramePublication(t testing.TB, channelID model.ChannelID,
	originPeerID model.PeerID, originEpoch model.OriginEpoch, sequence uint64,
	contentBytes int,
) model.SignedPublication {
	t.Helper()
	audiencePeerID, _ := model.ParsePeerID("peer-events-audience")
	audience, err := model.NewAudience([]model.PeerID{audiencePeerID})
	if err != nil {
		t.Fatal(err)
	}
	workID, _ := model.ParseWorkID(fmt.Sprintf("work-events-%d", sequence))
	work, err := model.NewWorkRef(originPeerID, workID)
	if err != nil {
		t.Fatal(err)
	}
	originHead, _ := model.NewRecordHead(1, model.Sum([]byte("events-origin-head")))
	rosterHead, _ := model.NewRecordHead(2, model.Sum([]byte("events-roster-head")))
	scope, err := model.NewEventScope(channelID, originPeerID, originEpoch, sequence,
		sequence, originHead, rosterHead, work)
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := model.ParseEventID(fmt.Sprintf("event-events-%d", sequence))
	payload, err := model.JSONFrom(map[string]any{"content": strings.Repeat("x", contentBytes)})
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := time.Date(2026, 7, 19, 1, 2, 3, int(sequence), time.UTC)
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-events",
		Type: model.EventReviewOffered, Audience: audience, Summary: "event direct repair",
		Payload: payload, CreatedAt: acceptedAt, AcceptedAt: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := eventFramePublicationPrivateKey()
	message, _ := model.PublicationSigningMessage(channelID, body.Digest())
	publication, err := model.AttachSignature(body, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

type eventFrameSignedPublicationWire struct {
	OriginSignature   []byte          `json:"origin_signature"`
	Publication       json.RawMessage `json:"publication"`
	PublicationDigest string          `json:"publication_digest"`
}

func eventFramePublicationPrivateKey() ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("mnemon/events/frame/test-signing-key"))
	return ed25519.NewKeyFromSeed(seed[:])
}

func resignEventFramePublication(t testing.TB, raw []byte, privateKey ed25519.PrivateKey,
	mutate func(map[string]any),
) []byte {
	t.Helper()
	var wire eventFrameSignedPublicationWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(wire.Publication))
	decoder.UseNumber()
	var body map[string]any
	if err := decoder.Decode(&body); err != nil {
		t.Fatal(err)
	}
	channelID, err := model.ParseChannelID(body["channel_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	mutate(body)
	canonical, err := model.JSONFrom(body)
	if err != nil {
		t.Fatal(err)
	}
	digest := model.Sum(canonical.Bytes())
	message, err := model.PublicationSigningMessage(channelID, digest)
	if err != nil {
		t.Fatal(err)
	}
	wire.Publication = canonical.Bytes()
	wire.PublicationDigest = digest.String()
	wire.OriginSignature = ed25519.Sign(privateKey, message)
	result, err := model.JSONFrom(wire)
	if err != nil {
		t.Fatal(err)
	}
	return result.Bytes()
}

func eventFramePullPage(t testing.TB, originEpoch model.OriginEpoch,
	scannedChannelSequence, sourceFloor, sourceHead uint64, publications ...[]byte,
) []byte {
	t.Helper()
	wirePublications := make([]json.RawMessage, len(publications))
	for index, publication := range publications {
		wirePublications[index] = append(json.RawMessage(nil), publication...)
	}
	payload, err := model.JSONFrom(pullPageWire{OriginEpoch: originEpoch.String(),
		Publications: wirePublications, ScannedChannelSequence: scannedChannelSequence,
		SourceFloor: sourceFloor, SourceHead: sourceHead})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := model.JSONFrom(eventFrameWire{Payload: payload.Bytes(),
		Type: EventFramePullPage, Version: EventFrameVersion})
	if err != nil {
		t.Fatal(err)
	}
	return frame.Bytes()
}

func eventFrameCanonicalMutation(t testing.TB, raw string,
	mutate func(map[string]any),
) []byte {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	canonical, err := model.CanonicalMarshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

type eventFrameChunkWriter struct {
	buffer  bytes.Buffer
	maximum int
}

func (writer *eventFrameChunkWriter) Write(value []byte) (int, error) {
	if len(value) > writer.maximum {
		value = value[:writer.maximum]
	}
	return writer.buffer.Write(value)
}

type eventFrameZeroWriter struct{}

func (eventFrameZeroWriter) Write([]byte) (int, error) { return 0, nil }

type eventFrameTestScope struct {
	reserved     int
	released     int
	priority     uint8
	reserveError error
}

func (scope *eventFrameTestScope) ReserveMemory(size int, priority uint8) error {
	scope.priority = priority
	if scope.reserveError != nil {
		return scope.reserveError
	}
	scope.reserved += size
	return nil
}

func (scope *eventFrameTestScope) ReleaseMemory(size int) { scope.released += size }
func (scope *eventFrameTestScope) Stat() network.ScopeStat {
	return network.ScopeStat{Memory: int64(scope.reserved - scope.released)}
}
func (scope *eventFrameTestScope) BeginSpan() (network.ResourceScopeSpan, error) {
	return scope, nil
}
func (scope *eventFrameTestScope) Done() {}

var _ network.ResourceScopeSpan = (*eventFrameTestScope)(nil)
var _ io.Writer = (*eventFrameChunkWriter)(nil)
