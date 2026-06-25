package mnemonhub

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// The T2 security baseline (locked decision 4 / v1.1), pinned at the wire the standalone hub
// serves: unauthenticated push -> 401; a principal without a replica grant (or without the verb)
// -> denied; an out-of-scope event -> rejected via the fail-closed clamp; a replayed batch is
// idempotent (zero duplicate rows, repeated accepted acks).

type verbGrants struct {
	grants map[contract.ActorID]ReplicaGrant
	verbs  map[contract.ActorID]map[string]bool
}

func (g verbGrants) Grant(p contract.ActorID, verb string) (ReplicaGrant, bool) {
	grant, ok := g.grants[p]
	if !ok || !g.verbs[p][verb] {
		return ReplicaGrant{}, false
	}
	return grant, ok
}

func newSecurityHub(t *testing.T) (*httptest.Server, *Server, *bytes.Buffer) {
	t.Helper()
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	grants := verbGrants{
		grants: map[contract.ActorID]ReplicaGrant{
			"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}},
			"pull-only@team": {Principal: "pull-only@team", Scopes: []contract.ResourceRef{mem}},
		},
		verbs: map[contract.ActorID]map[string]bool{
			"replica-a@team": {contract.SyncVerbPush: true, contract.SyncVerbPull: true, contract.SyncVerbStatus: true},
			"pull-only@team": {contract.SyncVerbPull: true},
		},
	}
	hub, _ := openTestHub(t, grants)
	var audit bytes.Buffer
	srv := httptest.NewServer(NewHTTPHandler(hub, BearerAuthenticator{Tokens: map[string]contract.ActorID{
		"tok-a":    "replica-a@team",
		"tok-pull": "pull-only@team",
	}}, &audit))
	t.Cleanup(srv.Close)
	return srv, hub, &audit
}

func postSync(t *testing.T, url, token string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestUnauthenticatedPushIs401(t *testing.T) {
	srv, _, audit := newSecurityHub(t)
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	body := contract.SyncPushRequest{ReplicaID: "local-a", Events: testSyncEvents(t, testMaterial("local-a", "d1", mem, map[string]any{"content": "x"}))}

	if resp := postSync(t, srv.URL+"/sync/push", "", body); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing token must 401, got %d", resp.StatusCode)
	}
	if resp := postSync(t, srv.URL+"/sync/push", "wrong-token", body); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown token must 401, got %d", resp.StatusCode)
	}
	if !strings.Contains(audit.String(), "principal=- verb=sync.push result=unauthorized") {
		t.Fatalf("401 must leave an audit line, got:\n%s", audit.String())
	}
}

func TestNonReplicaPrincipalAndWrongVerbAreRejected(t *testing.T) {
	srv, hub, audit := newSecurityHub(t)
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	body := contract.SyncPushRequest{ReplicaID: "local-a", Events: testSyncEvents(t, testMaterial("local-a", "d1", mem, map[string]any{"content": "x"}))}

	// pull-only credential: authenticated, but no sync.push grant -> denied at the grant seam.
	if resp := postSync(t, srv.URL+"/sync/push", "tok-pull", body); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong-verb push must be denied, got %d", resp.StatusCode)
	}
	if !strings.Contains(audit.String(), "principal=pull-only@team verb=sync.push result=denied") {
		t.Fatalf("denied push must leave an audit line, got:\n%s", audit.String())
	}
	// A principal the grants do not know at all (non-replica) is fail-closed on every verb.
	if _, err := hub.Push("host@project", contract.SyncPushRequest{ReplicaID: "local-a"}); err == nil || !strings.Contains(err.Error(), "no replica grant") {
		t.Fatalf("ungranted principal must be denied, got %v", err)
	}
	if _, err := hub.Status("host@project"); err == nil {
		t.Fatal("ungranted principal must not read status")
	}
}

func TestOutOfScopeEventRejectedByClamp(t *testing.T) {
	_, hub, _ := newSecurityHub(t)
	// replica-a's grant covers memory only; pushing a skill event must be rejected per-event by
	// the fail-closed clamp, with the clamp's diagnostic — never silently accepted.
	skill := contract.ResourceRef{Kind: "skill", ID: "project"}
	material := testMaterial("local-a", "dec-skill", skill, map[string]any{"name": "project"})
	resp, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b", Events: testSyncEvents(t, material)})
	if err != nil {
		t.Fatalf("out-of-scope event must reject per-event, not fail transport: %v", err)
	}
	if len(resp.Rejected) != 1 || !strings.Contains(resp.Rejected[0].Diagnostic, "outside principal") {
		t.Fatalf("out-of-scope event must carry the clamp diagnostic, got %+v", resp)
	}
	if st, _ := hub.Status("replica-a@team"); st.HubEventsReceived != 0 {
		t.Fatalf("a rejected event must not land in the log, got %+v", st)
	}
}

// LOW-10: the HTTP layer distinguishes a request-VALIDATION failure (400 / bad_request) from an
// AUTHORIZATION failure (403 / denied). A missing replica_id and malformed JSON are 400; a no-grant
// / out-of-scope refusal stays 403.
func TestErrorMappingSeparatesValidationFromAuthorization(t *testing.T) {
	srv, _, audit := newSecurityHub(t)

	// authenticated but missing replica_id -> request validation -> 400 bad_request.
	missing := postSync(t, srv.URL+"/sync/push", "tok-a", contract.SyncPushRequest{})
	if missing.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing replica_id must be 400, got %d", missing.StatusCode)
	}
	if !strings.Contains(audit.String(), "principal=replica-a@team verb=sync.push result=bad_request") {
		t.Fatalf("a validation failure must audit result=bad_request, got:\n%s", audit.String())
	}

	// malformed JSON body -> 400 bad_request (handled at decode).
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sync/push", bytes.NewReader([]byte("{not json")))
	req.Header.Set("Authorization", "Bearer tok-a")
	badJSON, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer badJSON.Body.Close()
	if badJSON.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed JSON must be 400, got %d", badJSON.StatusCode)
	}

	// no-grant push (wrong verb) stays 403 denied (authorization, not validation).
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	denied := postSync(t, srv.URL+"/sync/push", "tok-pull",
		contract.SyncPushRequest{ReplicaID: "local-a", Events: testSyncEvents(t, testMaterial("local-a", "d1", mem, map[string]any{"content": "x"}))})
	if denied.StatusCode != http.StatusForbidden {
		t.Fatalf("authorization refusal must stay 403, got %d", denied.StatusCode)
	}
}

// LOW-12: the frozen verb→method mapping is enforced — push/pull reject non-POST with 405, status
// accepts GET or POST, and a 405 carries an Allow header + a bad_request audit line.
func TestMethodEnforcement(t *testing.T) {
	srv, _, audit := newSecurityHub(t)

	getReq := func(url string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer tok-a")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	if r := getReq(srv.URL + "/sync/push"); r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /sync/push must be 405, got %d", r.StatusCode)
	} else if r.Header.Get("Allow") != http.MethodPost {
		t.Fatalf("405 must carry Allow: POST, got %q", r.Header.Get("Allow"))
	}
	if r := getReq(srv.URL + "/sync/pull"); r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /sync/pull must be 405, got %d", r.StatusCode)
	}
	// status accepts GET (and POST).
	if r := getReq(srv.URL + "/sync/status"); r.StatusCode != http.StatusOK {
		t.Fatalf("GET /sync/status must be allowed (200), got %d", r.StatusCode)
	}
	if !strings.Contains(audit.String(), "verb=sync.push result=bad_request") {
		t.Fatalf("a 405 must leave a bad_request audit line, got:\n%s", audit.String())
	}
}

func TestReplayedBatchIsIdempotentOverTheWire(t *testing.T) {
	srv, _, _ := newSecurityHub(t)
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	body := contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "replayed",
		Events: testSyncEvents(t, testMaterial("local-a", "dec-replay", mem, map[string]any{"content": "replayed"}))}

	var first, second contract.SyncPushResponse
	r1 := postSync(t, srv.URL+"/sync/push", "tok-a", body)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first push: %d", r1.StatusCode)
	}
	if err := json.NewDecoder(r1.Body).Decode(&first); err != nil {
		t.Fatal(err)
	}
	r2 := postSync(t, srv.URL+"/sync/push", "tok-a", body)
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("replayed push: %d", r2.StatusCode)
	}
	if err := json.NewDecoder(r2.Body).Decode(&second); err != nil {
		t.Fatal(err)
	}
	if len(first.Accepted) != 1 || len(second.Accepted) != 1 || first.Accepted[0] != second.Accepted[0] {
		t.Fatalf("replayed batch must repeat the accepted ack: first=%+v second=%+v", first, second)
	}
	var st contract.SyncStatusResponse
	r3 := postSync(t, srv.URL+"/sync/status", "tok-a", struct{}{})
	if err := json.NewDecoder(r3.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.HubEventsReceived != 1 {
		t.Fatalf("replayed batch must not duplicate rows: received=%d", st.HubEventsReceived)
	}
}
