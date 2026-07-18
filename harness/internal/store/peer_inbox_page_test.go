package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPutPeerInboxPageQuarantinesUnsupportedPublicationAndAdvancesCursor(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-unsupported", 0)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "unsupported-first", true),
		fixture.publication(t, 2, 2, "unsupported-future", true),
		fixture.publication(t, 3, 3, "unsupported-third", true),
	}
	spec := fixture.pageSpec(t, publications, 0, 3, fixture.at)
	future := unsupportedPeerPublicationEvidence(t, fixture, publications[1], func(body map[string]any) {
		body["schema_version"] = float64(model.SchemaVersion + 1)
		body["future_semantics"] = map[string]any{"opaque": true}
	})
	spec.Publications[1] = future

	result, err := fixture.store.PutPeerInboxPage(context.Background(), spec)
	if err != nil || !result.Quarantined || len(result.Items) != 3 ||
		result.Items[0].Disposition != PeerInboxStored ||
		result.Items[1].Disposition != PeerInboxQuarantined ||
		result.Items[2].Disposition != PeerInboxStored ||
		result.Cursor.ContiguousChannelSequence != 3 || result.Cursor.ObservedChannelSequence != 3 {
		t.Fatalf("unsupported page = (%#v,%v)", result, err)
	}
	var status, diagnostic string
	var wire, roots []byte
	var audience int
	if err := fixture.store.db.QueryRow(`SELECT status,diagnostic,publication_json,
		is_audience,required_artifact_roots_json FROM peer_inbox WHERE channel_seq=2`).
		Scan(&status, &diagnostic, &wire, &audience, &roots); err != nil ||
		status != "quarantined" || diagnostic != "unsupported_publication_schema" || audience != 1 ||
		!bytes.Equal(wire, future.WireJSON().Bytes()) || string(roots) != `[]` {
		t.Fatalf("unsupported durable evidence = (%q,%q,%d,%q,%q,%v)",
			status, diagnostic, audience, wire, roots, err)
	}
	for _, table := range []string{"events", "works", "agent_handlings", "agent_runs", "artifact_roots"} {
		var count int
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count after unsupported quarantine = (%d,%v), want zero", table, count, err)
		}
	}
	var pressure, pending int64
	if err := fixture.store.db.QueryRow(`SELECT pending_bytes FROM peer_inbox_node_pressure
		WHERE singleton_id=1`).Scan(&pressure); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT COALESCE(SUM(length(publication_json)+
		length(required_artifact_roots_json)),0) FROM peer_inbox WHERE status IN
		('stored','waiting_artifact','ready','processing','retry')`).Scan(&pending); err != nil || pressure != pending {
		t.Fatalf("unsupported pressure = node %d pending %d err %v", pressure, pending, err)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatalf("restart after unsupported quarantine: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	fixture.store = reopened
	replay, err := fixture.store.PutPeerInboxPage(context.Background(), spec)
	if err != nil || len(replay.Items) != 3 || replay.Cursor != result.Cursor {
		t.Fatalf("unsupported response-loss replay = (%#v,%v)", replay, err)
	}
	for index, item := range replay.Items {
		if item.Disposition != PeerInboxDuplicate || item.InboxID != result.Items[index].InboxID {
			t.Fatalf("unsupported replay item %d = %#v", index, item)
		}
	}
}

func TestPutPeerInboxPageRejectsUnauthenticatedUnsupportedEvidenceAtomically(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-unsupported-signature", 0)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "unsupported-signature-first", true),
		fixture.publication(t, 2, 2, "unsupported-signature-second", true),
	}
	spec := fixture.pageSpec(t, publications, 0, 2, fixture.at)
	future := unsupportedPeerPublicationEvidence(t, fixture, publications[1], func(body map[string]any) {
		body["schema_version"] = float64(model.SchemaVersion + 1)
	})
	spec.Publications[1] = tamperPeerPublicationEvidenceSignature(t, future)
	if _, err := fixture.store.PutPeerInboxPage(context.Background(), spec); err == nil {
		t.Fatal("unauthenticated unsupported publication was durably covered")
	}
	var inboxes int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxes); err != nil || inboxes != 0 {
		t.Fatalf("Inbox count after signature rollback = (%d,%v)", inboxes, err)
	}
	var contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT contiguous_channel_seq,observed_channel_seq
		FROM peer_cursors`).Scan(&contiguous, &observed); err != nil || contiguous != 0 || observed != 0 {
		t.Fatalf("cursor after signature rollback = (%d,%d,%v)", contiguous, observed, err)
	}
}

func TestPutPeerInboxPageQuarantinesSignedInvalidV1Body(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-invalid-v1", 0)
	publication := fixture.publication(t, 1, 1, "invalid-v1", true)
	spec := fixture.pageSpec(t, []model.SignedPublication{publication}, 0, 1, fixture.at)
	spec.Publications[0] = unsupportedPeerPublicationEvidence(t, fixture, publication,
		func(body map[string]any) {
			body["event"].(map[string]any)["event_type"] = "review.future"
		})
	result, err := fixture.store.PutPeerInboxPage(context.Background(), spec)
	if err != nil || !result.Quarantined || len(result.Items) != 1 ||
		result.Items[0].Disposition != PeerInboxQuarantined ||
		result.Cursor.ContiguousChannelSequence != 1 {
		t.Fatalf("invalid v1 page = (%#v,%v)", result, err)
	}
	var diagnostic string
	if err := fixture.store.db.QueryRow(`SELECT diagnostic FROM peer_inbox`).Scan(&diagnostic); err != nil ||
		diagnostic != "invalid_publication_schema_v1" {
		t.Fatalf("invalid v1 diagnostic = (%q,%v)", diagnostic, err)
	}
}

func TestPutPeerInboxPageQuarantinesUnsupportedNonaudienceWithoutDomainEffect(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxObserverFixture(t, "page-unsupported-observer")
	publication := fixture.publication(t, 1, 1, "unsupported-observer", false)
	spec := fixture.pageSpec(t, []model.SignedPublication{publication}, 0, 1, fixture.at)
	spec.Publications[0] = unsupportedPeerPublicationEvidence(t, fixture, publication,
		func(body map[string]any) {
			body["schema_version"] = float64(model.SchemaVersion + 1)
		})
	result, err := fixture.store.PutPeerInboxPage(context.Background(), spec)
	if err != nil || len(result.Items) != 1 || result.Items[0].Disposition != PeerInboxQuarantined ||
		result.Cursor.ContiguousChannelSequence != 1 {
		t.Fatalf("unsupported nonaudience page = (%#v,%v)", result, err)
	}
	var status string
	var audience int
	if err := fixture.store.db.QueryRow(`SELECT status,is_audience FROM peer_inbox`).
		Scan(&status, &audience); err != nil || status != "quarantined" || audience != 0 {
		t.Fatalf("unsupported nonaudience Inbox = (%q,%d,%v)", status, audience, err)
	}
	var events int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&events); err != nil || events != 0 {
		t.Fatalf("unsupported nonaudience Event count = (%d,%v)", events, err)
	}
}

func TestPutPeerInboxPageUnsupportedChallengerCannotBypassConflictFence(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-unsupported-conflict", 0)
	incumbent := fixture.publication(t, 1, 1, "unsupported-incumbent", true)
	fixture.put(t, incumbent, fixture.at)
	challenger := unsupportedPeerPublicationEvidence(t, fixture, incumbent, func(body map[string]any) {
		body["schema_version"] = float64(model.SchemaVersion + 1)
	})
	spec := fixture.pageSpec(t, []model.SignedPublication{incumbent}, 0, 1,
		fixture.at.Add(time.Second))
	spec.Publications[0] = challenger
	result, err := fixture.store.PutPeerInboxPage(context.Background(), spec)
	if err != nil || !result.Quarantined || len(result.Items) != 1 ||
		result.Items[0].Disposition != PeerInboxConflicted || result.Items[0].ConflictID == "" ||
		result.Cursor.ContiguousChannelSequence != 1 {
		t.Fatalf("unsupported challenger = (%#v,%v)", result, err)
	}
	var wire []byte
	if err := fixture.store.db.QueryRow(`SELECT conflicting_publication_json
		FROM publication_conflicts`).Scan(&wire); err != nil ||
		!bytes.Equal(wire, challenger.WireJSON().Bytes()) {
		t.Fatalf("unsupported challenger evidence = (%q,%v)", wire, err)
	}
	var quarantines int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM origin_quarantines`).Scan(&quarantines); err != nil ||
		quarantines != 1 {
		t.Fatalf("unsupported origin quarantine = (%d,%v)", quarantines, err)
	}
}

func TestPutPeerInboxPageRollsBackUnsupportedQuarantineWithTrailingFailure(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-unsupported-rollback", 0)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "unsupported-rollback-first", true),
		fixture.publication(t, 2, 2, "unsupported-rollback-future", true),
		fixture.publication(t, 3, 3, "unsupported-rollback-third", true),
	}
	spec := fixture.pageSpec(t, publications, 0, 3, fixture.at)
	spec.Publications[1] = unsupportedPeerPublicationEvidence(t, fixture, publications[1],
		func(body map[string]any) {
			body["schema_version"] = float64(model.SchemaVersion + 1)
		})
	mustExec(t, fixture.store, `CREATE TRIGGER test_unsupported_page_trailing_failure
		BEFORE INSERT ON peer_inbox WHEN NEW.channel_seq=3
		BEGIN SELECT RAISE(ABORT,'injected unsupported page trailing failure'); END`)
	if _, err := fixture.store.PutPeerInboxPage(context.Background(), spec); err == nil {
		t.Fatal("unsupported page trailing failure unexpectedly committed")
	}
	var inboxes int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxes); err != nil || inboxes != 0 {
		t.Fatalf("Inbox count after unsupported rollback = (%d,%v)", inboxes, err)
	}
	var contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT contiguous_channel_seq,observed_channel_seq
		FROM peer_cursors`).Scan(&contiguous, &observed); err != nil || contiguous != 0 || observed != 0 {
		t.Fatalf("cursor after unsupported rollback = (%d,%d,%v)", contiguous, observed, err)
	}
}

func TestPutPeerInboxPageCommitsContinuousPageAndCursorAtomically(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-commit", 0)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "page-first", true),
		fixture.publication(t, 2, 2, "page-second", true),
		fixture.publication(t, 3, 3, "page-third", true),
	}
	result, err := fixture.store.PutPeerInboxPage(context.Background(), fixture.pageSpec(t, publications,
		0, 3, fixture.at))
	if err != nil || result.Quarantined || len(result.Items) != 3 ||
		result.Cursor.ContiguousChannelSequence != 3 || result.Cursor.ObservedChannelSequence != 3 {
		t.Fatalf("PutPeerInboxPage() = (%#v,%v)", result, err)
	}
	for index, item := range result.Items {
		if item.Disposition != PeerInboxStored || item.Cursor != result.Cursor {
			t.Fatalf("item %d = %#v", index, item)
		}
	}
	var inboxes, events int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxes); err != nil || inboxes != 3 {
		t.Fatalf("Inbox count = (%d,%v)", inboxes, err)
	}
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&events); err != nil || events != 0 {
		t.Fatalf("semantic Event count = (%d,%v)", events, err)
	}

	replay, err := fixture.store.PutPeerInboxPage(context.Background(), fixture.pageSpec(t, publications,
		0, 3, fixture.at.Add(time.Second)))
	if err != nil || len(replay.Items) != 3 || replay.Cursor != result.Cursor {
		t.Fatalf("response-loss replay = (%#v,%v), first %#v", replay, err, result)
	}
	for index, item := range replay.Items {
		if item.Disposition != PeerInboxDuplicate || item.InboxID != result.Items[index].InboxID {
			t.Fatalf("replay item %d = %#v", index, item)
		}
	}
}

func TestPutPeerInboxPageRollsBackEveryItemAndCursorOnFailure(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-rollback", 0)
	mustExec(t, fixture.store, `CREATE TRIGGER test_peer_inbox_page_failure
		BEFORE INSERT ON peer_inbox WHEN NEW.channel_seq=2
		BEGIN SELECT RAISE(ABORT,'injected Peer Inbox page failure'); END`)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "rollback-first", true),
		fixture.publication(t, 2, 2, "rollback-second", true),
	}
	if _, err := fixture.store.PutPeerInboxPage(context.Background(), fixture.pageSpec(t, publications,
		0, 2, fixture.at)); err == nil {
		t.Fatal("PutPeerInboxPage() unexpectedly succeeded")
	}
	var count int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("Inbox count after rollback = (%d,%v)", count, err)
	}
	var contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT contiguous_channel_seq,observed_channel_seq
		FROM peer_cursors`).Scan(&contiguous, &observed); err != nil || contiguous != 0 || observed != 0 {
		t.Fatalf("cursor after rollback = (%d,%d,%v)", contiguous, observed, err)
	}
	var channelBytes, nodeBytes uint64
	if err := fixture.store.db.QueryRow(`SELECT COALESCE(SUM(pending_bytes),0)
		FROM peer_inbox_pressure`).Scan(&channelBytes); err != nil || channelBytes != 0 {
		t.Fatalf("Channel pressure after rollback = (%d,%v)", channelBytes, err)
	}
	if err := fixture.store.db.QueryRow(`SELECT pending_bytes FROM peer_inbox_node_pressure
		WHERE singleton_id=1`).Scan(&nodeBytes); err != nil || nodeBytes != 0 {
		t.Fatalf("Node pressure after rollback = (%d,%v)", nodeBytes, err)
	}
}

func TestPutPeerInboxPageCommitsPrefixAndConflictButNotTrailingRows(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-conflict", 0)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "page-conflict-first", true),
		fixture.publication(t, 1, 2, "page-conflict-equivocation", true),
		fixture.publication(t, 2, 3, "page-conflict-trailing", true),
	}
	result, err := fixture.store.PutPeerInboxPage(context.Background(), fixture.pageSpec(t, publications,
		0, 3, fixture.at))
	if err != nil || !result.Quarantined || len(result.Items) != 2 ||
		result.Items[0].Disposition != PeerInboxStored ||
		result.Items[1].Disposition != PeerInboxConflicted ||
		result.Cursor.ContiguousChannelSequence != 2 || result.Cursor.ObservedChannelSequence != 2 {
		t.Fatalf("conflicted page = (%#v,%v)", result, err)
	}
	for table, want := range map[string]int{"peer_inbox": 1, "publication_conflicts": 1,
		"origin_quarantines": 1} {
		var count int
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
			t.Fatalf("%s count = (%d,%v), want %d", table, count, err, want)
		}
	}
	var trailing int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox WHERE event_id=?`,
		publications[2].Event().ID().String()).Scan(&trailing); err != nil || trailing != 0 {
		t.Fatalf("trailing Inbox count = (%d,%v)", trailing, err)
	}
}

func TestPutPeerInboxPageRepairsGossipGapWithoutDuplicatingEvidence(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-repair", 0)
	publications := []model.SignedPublication{
		fixture.publication(t, 1, 1, "repair-first", true),
		fixture.publication(t, 2, 2, "repair-second", true),
		fixture.publication(t, 3, 3, "repair-third", true),
	}
	gossip, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publications[2], TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalGossip, ReceivedAt: fixture.at})
	if err != nil || gossip.Cursor.ContiguousChannelSequence != 0 ||
		gossip.Cursor.ObservedChannelSequence != 3 {
		t.Fatalf("Gossip gap = (%#v,%v)", gossip, err)
	}
	page, err := fixture.store.PutPeerInboxPage(context.Background(), fixture.pageSpec(t, publications,
		0, 3, fixture.at.Add(time.Second)))
	if err != nil || page.Cursor.ContiguousChannelSequence != 3 ||
		page.Items[2].Disposition != PeerInboxDuplicate {
		t.Fatalf("repair page = (%#v,%v)", page, err)
	}
	var count int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("deduplicated Inbox count = (%d,%v)", count, err)
	}
}

func TestPutPeerInboxPageRejectsSparseOrMisdirectedPagesBeforeMutation(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "page-invalid", 0)
	first := fixture.publication(t, 1, 1, "invalid-first", true)
	third := fixture.publication(t, 3, 3, "invalid-third", true)
	cases := map[string]PutPeerInboxPageSpec{
		"sparse":       fixture.pageSpec(t, []model.SignedPublication{first, third}, 0, 3, fixture.at),
		"empty jump":   fixture.pageSpec(t, nil, 0, 1, fixture.at),
		"wrong origin": fixture.pageSpec(t, []model.SignedPublication{first}, 0, 1, fixture.at),
	}
	wrong, _ := model.ParsePeerID("peer-wrong-pull-origin")
	value := cases["wrong origin"]
	value.OriginPeerID, value.TransportPeerID = wrong, wrong
	cases["wrong origin"] = value
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := fixture.store.PutPeerInboxPage(context.Background(), spec); err == nil {
				t.Fatal("PutPeerInboxPage() unexpectedly succeeded")
			}
		})
	}
	var count int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("Inbox count after invalid pages = (%d,%v)", count, err)
	}
}

func (fixture peerInboxFixture) pageSpec(t *testing.T, publications []model.SignedPublication,
	after, scanned uint64, at time.Time,
) PutPeerInboxPageSpec {
	t.Helper()
	evidence := make([]model.PublicationEvidence, len(publications))
	for index, publication := range publications {
		var err error
		evidence[index], err = model.ParsePublicationEvidence(publication.WireJSON().Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}
	return PutPeerInboxPageSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID:    fixture.remote.Identity().PeerID(),
		OriginEpoch:     fixture.remote.Identity().OriginEpoch(),
		TransportPeerID: fixture.remote.Identity().PeerID(), AfterChannelSequence: after,
		ScannedChannelSeq: scanned, SourceFloor: 1, SourceHead: scanned,
		Publications: evidence, ReceivedAt: at}
}

func unsupportedPeerPublicationEvidence(t *testing.T, fixture peerInboxFixture,
	publication model.SignedPublication, mutate func(map[string]any),
) model.PublicationEvidence {
	t.Helper()
	var wire struct {
		OriginSignature   []byte          `json:"origin_signature"`
		Publication       json.RawMessage `json:"publication"`
		PublicationDigest string          `json:"publication_digest"`
	}
	if err := json.Unmarshal(publication.WireJSON().Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(wire.Publication, &body); err != nil {
		t.Fatal(err)
	}
	mutate(body)
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	digest := model.Sum(canonical.Bytes())
	message, err := model.PublicationSigningMessage(publication.Key().ChannelID(), digest)
	if err != nil {
		t.Fatal(err)
	}
	wire.Publication = canonical.Bytes()
	wire.PublicationDigest = digest.String()
	wire.OriginSignature = ed25519.Sign(ed25519Private(fixture.remote.Identity()), message)
	encoded, err := model.JSONFrom(wire)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := model.ParsePublicationEvidence(encoded.Bytes())
	if err != nil || evidence.IsSupported() {
		t.Fatalf("unsupported evidence = (%#v,%v)", evidence, err)
	}
	return evidence
}

func tamperPeerPublicationEvidenceSignature(t *testing.T,
	evidence model.PublicationEvidence,
) model.PublicationEvidence {
	t.Helper()
	var wire struct {
		OriginSignature   []byte          `json:"origin_signature"`
		Publication       json.RawMessage `json:"publication"`
		PublicationDigest string          `json:"publication_digest"`
	}
	if err := json.Unmarshal(evidence.WireJSON().Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	wire.OriginSignature[0] ^= 0xff
	encoded, err := model.JSONFrom(wire)
	if err != nil {
		t.Fatal(err)
	}
	tampered, err := model.ParsePublicationEvidence(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return tampered
}
