package store

import (
	"crypto/ed25519"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestParseOperationCaptureClosedCanonicalClosure(t *testing.T) {
	t.Parallel()
	rootA, rootB := model.Sum([]byte("root-a")), model.Sum([]byte("root-b"))
	manifestA, manifestB := model.Sum([]byte("manifest-a")), model.Sum([]byte("manifest-b"))
	if rootB.String() < rootA.String() {
		rootA, rootB, manifestA, manifestB = rootB, rootA, manifestB, manifestA
	}
	value, _ := model.JSONFrom(struct {
		Roots []struct {
			ManifestDigest model.Digest `json:"manifest_digest"`
			RootDigest     model.Digest `json:"root_digest"`
		} `json:"roots"`
	}{[]struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}{{manifestA, rootA}, {manifestB, rootB}}})
	roots, err := parseOperationCapture(value)
	if err != nil || len(roots) != 2 || roots[0].RootDigest != rootA || roots[1].ManifestDigest != manifestB {
		t.Fatalf("parseOperationCapture() = (%#v, %v)", roots, err)
	}
	empty, _ := model.NewJSON([]byte(`{"roots":[]}`))
	if roots, err := parseOperationCapture(empty); err != nil || len(roots) != 0 {
		t.Fatalf("empty closure = (%#v, %v)", roots, err)
	}
	badValues := []string{
		`{"other":[]}`, `{"roots":null}`, `{"roots":["` + rootA.String() + `"]}`,
		`{"roots":[{"manifest_digest":"` + manifestA.String() + `","root_digest":"` + rootA.String() + `","x":1}]}`,
		`{"roots":[{"manifest_digest":"` + manifestB.String() + `","root_digest":"` + rootB.String() + `"},{"manifest_digest":"` + manifestA.String() + `","root_digest":"` + rootA.String() + `"}]}`,
	}
	for _, raw := range badValues {
		value, err := model.NewJSON([]byte(raw))
		if err == nil {
			_, err = parseOperationCapture(value)
		}
		if err == nil {
			t.Errorf("parseOperationCapture(%s) accepted", raw)
		}
	}
}

func TestParseOperationCaptureRejectsTooManyRoots(t *testing.T) {
	t.Parallel()
	type encodedRoot struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}
	roots := make([]encodedRoot, model.MaxArtifactRefs+1)
	for index := range roots {
		roots[index] = encodedRoot{
			ManifestDigest: model.Sum([]byte("manifest-limit-" + string(rune(index)))),
			RootDigest:     model.Sum([]byte("root-limit-" + string(rune(index)))),
		}
	}
	sort.Slice(roots, func(i, j int) bool {
		return roots[i].RootDigest.String() < roots[j].RootDigest.String()
	})
	value, err := model.JSONFrom(struct {
		Roots []encodedRoot `json:"roots"`
	}{roots})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseOperationCapture(value); err == nil {
		t.Fatalf("parseOperationCapture() accepted %d roots", len(roots))
	}
}

func TestValidateLocalPublicationAuthenticatesExactBody(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	event := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"review","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now)
	body, _ := model.NewPublicationBody(event)
	publicKey, privateKey, _ := ed25519.GenerateKey(nil)
	message, _ := model.PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	publication, _ := model.AttachSignature(body, ed25519.Sign(privateKey, message))
	if rebuilt, err := validateLocalPublication(publication, publicKey); err != nil || rebuilt.Digest() != event.Digest() {
		t.Fatalf("validateLocalPublication() = (%#v, %v)", rebuilt, err)
	}
	wrongKey, _, _ := ed25519.GenerateKey(nil)
	if _, err := validateLocalPublication(publication, wrongKey); !errors.Is(err, ErrPublicationInvalid) {
		t.Fatalf("wrong key error = %v", err)
	}
	forged, _ := model.AttachSignature(body, make([]byte, ed25519.SignatureSize))
	if _, err := validateLocalPublication(forged, publicKey); !errors.Is(err, ErrPublicationInvalid) {
		t.Fatalf("forged signature error = %v", err)
	}
}

func newCodecEvent(t *testing.T, eventType model.EventType, payloadText string, now time.Time) model.Event {
	t.Helper()
	home, _ := model.ParsePeerID("peer-codec-home")
	reviewer, _ := model.ParsePeerID("peer-codec-reviewer")
	channel, _ := model.ParseChannelID("channel-codec")
	eventID, _ := model.ParseEventID("event-codec-" + string(eventType))
	workID, _ := model.ParseWorkID("work-codec")
	work, _ := model.NewWorkRef(home, workID)
	member, _ := model.NewRecordHead(1, model.Sum([]byte("member")))
	roster, _ := model.NewRecordHead(2, model.Sum([]byte("roster")))
	epoch, _ := model.ParseOriginEpoch("epoch-codec")
	origin, target := home, reviewer
	if eventType.ParticipantInput() {
		origin, target = reviewer, home
	}
	scope, _ := model.NewEventScope(channel, origin, epoch, 1, 1, member, roster, work)
	audience, _ := model.NewAudience([]model.PeerID{target})
	payload, err := model.NewJSON([]byte(payloadText))
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope, Source: model.EventSourceLocal,
		ActorPrincipal: "principal-codec", Type: eventType, Audience: audience, Summary: "codec",
		Payload: payload, CreatedAt: now, AcceptedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return event
}
