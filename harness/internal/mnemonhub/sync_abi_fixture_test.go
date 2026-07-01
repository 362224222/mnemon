package mnemonhub

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

type syncABIFixture struct {
	Name          string                 `json:"name"`
	Kind          string                 `json:"kind"`
	Principal     contract.ActorID       `json:"principal"`
	ReplicaID     string                 `json:"replica_id"`
	GrantScopes   []contract.ResourceRef `json:"grant_scopes"`
	SeedPrincipal contract.ActorID       `json:"seed_principal"`
	SeedReplicaID string                 `json:"seed_replica_id"`
	SeedEvents    []syncABIEventFixture  `json:"seed_events"`
	Events        []syncABIEventFixture  `json:"events"`
	Route         string                 `json:"route"`
	Method        string                 `json:"method"`
	Token         string                 `json:"token"`
	Want          syncABIWant            `json:"want"`
}

type syncABIEventFixture struct {
	OriginReplicaID string               `json:"origin_replica_id"`
	LocalDecisionID string               `json:"local_decision_id"`
	ResourceRef     contract.ResourceRef `json:"resource_ref"`
	Fields          map[string]any       `json:"fields"`
	CorruptDigest   bool                 `json:"corrupt_digest"`
}

type syncABIWant struct {
	Accepted          int `json:"accepted"`
	Rejected          int `json:"rejected"`
	Conflicts         int `json:"conflicts"`
	StoredEvents      int `json:"stored_events"`
	Events            int `json:"events"`
	AfterCursorEvents int `json:"after_cursor_events"`
	HubEventsReceived int `json:"hub_events_received"`
	StatusCode        int `json:"status_code"`
}

func TestSyncABIFixtures(t *testing.T) {
	for _, fixture := range loadSyncABIFixtures(t) {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			switch fixture.Kind {
			case "push":
				runSyncABIPushFixture(t, fixture)
			case "push_replay":
				runSyncABIReplayFixture(t, fixture)
			case "push_conflict":
				runSyncABIConflictFixture(t, fixture)
			case "pull":
				runSyncABIPullFixture(t, fixture)
			case "status":
				runSyncABIStatusFixture(t, fixture)
			case "http_auth":
				runSyncABIHTTPAuthFixture(t, fixture)
			default:
				t.Fatalf("unknown sync ABI fixture kind %q", fixture.Kind)
			}
		})
	}
}

func loadSyncABIFixtures(t *testing.T) []syncABIFixture {
	t.Helper()
	root := filepath.Join("..", "..", "testdata", "mnemonhub", "sync-abi")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read sync ABI fixtures: %v", err)
	}
	var fixtures []syncABIFixture
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			t.Fatalf("read fixture %s: %v", entry.Name(), err)
		}
		var fixture syncABIFixture
		if err := json.Unmarshal(raw, &fixture); err != nil {
			t.Fatalf("decode fixture %s: %v", entry.Name(), err)
		}
		if fixture.Name == "" {
			t.Fatalf("fixture %s has empty name", entry.Name())
		}
		fixtures = append(fixtures, fixture)
	}
	if len(fixtures) == 0 {
		t.Fatal("no sync ABI fixtures loaded")
	}
	return fixtures
}

func openFixtureHub(t *testing.T, fixture syncABIFixture) *Server {
	t.Helper()
	grants := GrantMap{}
	if fixture.Principal != "" {
		grants[fixture.Principal] = ReplicaGrant{Principal: fixture.Principal, Scopes: fixture.GrantScopes}
	}
	if fixture.SeedPrincipal != "" {
		grants[fixture.SeedPrincipal] = ReplicaGrant{Principal: fixture.SeedPrincipal, Scopes: fixture.GrantScopes}
	}
	hub, _ := openTestHub(t, grants)
	return hub
}

func runSyncABIPushFixture(t *testing.T, fixture syncABIFixture) {
	hub := openFixtureHub(t, fixture)
	resp, err := hub.Push(fixture.Principal, contract.SyncPushRequest{
		ReplicaID: fixture.ReplicaID,
		BatchID:   fixture.Name,
		Events:    fixtureEvents(t, fixture.Events),
	})
	if err != nil {
		t.Fatalf("push fixture %s: %v", fixture.Name, err)
	}
	assertExchangeCounts(t, fixture, resp)
}

func runSyncABIReplayFixture(t *testing.T, fixture syncABIFixture) {
	hub, st := openTestHub(t, GrantMap{
		fixture.Principal: {Principal: fixture.Principal, Scopes: fixture.GrantScopes},
	})
	req := contract.SyncPushRequest{ReplicaID: fixture.ReplicaID, BatchID: fixture.Name, Events: fixtureEvents(t, fixture.Events)}
	first, err := hub.Push(fixture.Principal, req)
	if err != nil {
		t.Fatalf("first replay fixture push: %v", err)
	}
	second, err := hub.Push(fixture.Principal, req)
	if err != nil {
		t.Fatalf("second replay fixture push: %v", err)
	}
	assertExchangeCounts(t, fixture, second)
	if len(first.Accepted) != len(second.Accepted) || first.NextCursor != second.NextCursor {
		t.Fatalf("replay fixture must return stable accepted ack: first=%+v second=%+v", first, second)
	}
	if fixture.Want.StoredEvents > 0 {
		got, _ := st.RemoteSyncedEventCount()
		if int(got) != fixture.Want.StoredEvents {
			t.Fatalf("stored events = %d, want %d", got, fixture.Want.StoredEvents)
		}
	}
}

func runSyncABIConflictFixture(t *testing.T, fixture syncABIFixture) {
	if len(fixture.Events) != 2 {
		t.Fatalf("conflict fixture requires exactly two events")
	}
	hub := openFixtureHub(t, fixture)
	first := contract.SyncPushRequest{ReplicaID: fixture.ReplicaID, BatchID: fixture.Name + "-first", Events: fixtureEvents(t, fixture.Events[:1])}
	if _, err := hub.Push(fixture.Principal, first); err != nil {
		t.Fatalf("seed conflict fixture: %v", err)
	}
	second := contract.SyncPushRequest{ReplicaID: fixture.ReplicaID, BatchID: fixture.Name + "-second", Events: fixtureEvents(t, fixture.Events[1:])}
	resp, err := hub.Push(fixture.Principal, second)
	if err != nil {
		t.Fatalf("conflict fixture push: %v", err)
	}
	assertExchangeCounts(t, fixture, resp)
}

func runSyncABIPullFixture(t *testing.T, fixture syncABIFixture) {
	hub := openFixtureHub(t, fixture)
	seedFixture(t, hub, fixture)
	resp, err := hub.Pull(fixture.Principal, contract.SyncPullRequest{ReplicaID: fixture.ReplicaID})
	if err != nil {
		t.Fatalf("pull fixture %s: %v", fixture.Name, err)
	}
	if len(resp.Events) != fixture.Want.Events {
		t.Fatalf("pull events = %d, want %d", len(resp.Events), fixture.Want.Events)
	}
	if fixture.Want.AfterCursorEvents >= 0 && resp.NextCursor != "" {
		after, err := hub.Pull(fixture.Principal, contract.SyncPullRequest{ReplicaID: fixture.ReplicaID, RemoteCursor: resp.NextCursor})
		if err != nil {
			t.Fatalf("pull after cursor: %v", err)
		}
		if len(after.Events) != fixture.Want.AfterCursorEvents {
			t.Fatalf("after-cursor events = %d, want %d", len(after.Events), fixture.Want.AfterCursorEvents)
		}
	}
}

func runSyncABIStatusFixture(t *testing.T, fixture syncABIFixture) {
	hub := openFixtureHub(t, fixture)
	seedFixture(t, hub, fixture)
	resp, err := hub.Status(fixture.Principal)
	if err != nil {
		t.Fatalf("status fixture %s: %v", fixture.Name, err)
	}
	if int(resp.HubEventsReceived) != fixture.Want.HubEventsReceived {
		t.Fatalf("HubEventsReceived = %d, want %d", resp.HubEventsReceived, fixture.Want.HubEventsReceived)
	}
}

func runSyncABIHTTPAuthFixture(t *testing.T, fixture syncABIFixture) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	hub, _ := openTestHub(t, GrantMap{
		"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}},
	})
	var audit bytes.Buffer
	srv := httptest.NewServer(NewHTTPHandler(hub, BearerAuthenticator{Tokens: map[string]contract.ActorID{
		"tok-a": "replica-a@team",
	}}, &audit))
	t.Cleanup(srv.Close)
	body := contract.SyncPushRequest{ReplicaID: "local-a", BatchID: fixture.Name, Events: fixtureEvents(t, []syncABIEventFixture{{
		OriginReplicaID: "local-a",
		LocalDecisionID: "dec-auth",
		ResourceRef:     mem,
		Fields:          map[string]any{"content": "auth"},
	}})}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(fixture.Method, srv.URL+fixture.Route, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if fixture.Token != "" {
		req.Header.Set("Authorization", "Bearer "+fixture.Token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != fixture.Want.StatusCode {
		t.Fatalf("status code = %d, want %d; audit=%s", resp.StatusCode, fixture.Want.StatusCode, audit.String())
	}
}

func seedFixture(t *testing.T, hub *Server, fixture syncABIFixture) {
	t.Helper()
	if len(fixture.SeedEvents) == 0 {
		return
	}
	_, err := hub.Push(fixture.SeedPrincipal, contract.SyncPushRequest{
		ReplicaID: fixture.SeedReplicaID,
		BatchID:   fixture.Name + "-seed",
		Events:    fixtureEvents(t, fixture.SeedEvents),
	})
	if err != nil {
		t.Fatalf("seed fixture %s: %v", fixture.Name, err)
	}
}

func assertExchangeCounts(t *testing.T, fixture syncABIFixture, resp contract.SyncPushResponse) {
	t.Helper()
	if len(resp.Accepted) != fixture.Want.Accepted || len(resp.Rejected) != fixture.Want.Rejected || len(resp.Conflicts) != fixture.Want.Conflicts {
		t.Fatalf("%s counts accepted/rejected/conflicts = %d/%d/%d, want %d/%d/%d: %+v",
			fixture.Name,
			len(resp.Accepted), len(resp.Rejected), len(resp.Conflicts),
			fixture.Want.Accepted, fixture.Want.Rejected, fixture.Want.Conflicts,
			resp)
	}
}

func fixtureEvents(t *testing.T, fixtures []syncABIEventFixture) []eventmodel.EventEnvelope {
	t.Helper()
	events := make([]eventmodel.EventEnvelope, 0, len(fixtures))
	for _, fixture := range fixtures {
		material := testMaterial(fixture.OriginReplicaID, fixture.LocalDecisionID, fixture.ResourceRef, fixture.Fields)
		if fixture.CorruptDigest {
			material.FieldsDigest = "corrupt"
		}
		events = append(events, testSyncEvent(t, material))
	}
	return events
}
