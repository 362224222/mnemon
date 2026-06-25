package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

func TestAcceptedLocalProgressCreatesPendingSyncEvent(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "governed.db")
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	rt, err := OpenRuntime(storePath, localRuntimeConfigT([]access.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	srv := httptest.NewServer(NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	client := access.NewClient(srv.URL, "codex@project")
	if rec, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "sync-progress-1",
		Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
			"summary": "Sync should queue this local event entry.",
		}},
	}); err != nil || !rec.Ticked {
		t.Fatalf("observe progress candidate: rec=%+v err=%v", rec, err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("extra tick: %v", err)
	}

	pending, err := rt.store.PendingSyncedEvents()
	if err != nil {
		t.Fatalf("pending synced events: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want one pending synced event, got %+v", pending)
	}
	material, err := contract.SyncedEventMaterialFromEnvelope(pending[0])
	if err != nil {
		t.Fatalf("materialize pending synced event: %v", err)
	}
	if material.OriginReplicaID == "" || material.LocalDecisionID == "" || material.Status != "pending" {
		t.Fatalf("pending synced event missing identity/status: %+v", material)
	}
	if material.ResourceRef != ref || material.ResourceVersion != 1 {
		t.Fatalf("pending material has wrong resource: %+v", material)
	}
	if material.FieldsDigest == "" {
		t.Fatalf("pending material must include fields digest: %+v", material)
	}
	if content, _ := material.Fields["content"].(string); !strings.Contains(content, "Sync should queue") {
		t.Fatalf("pending material fields missing event content: %+v", material.Fields)
	}
	acceptedEvents, err := rt.store.EventEnvelopes(state.EventEnvelopeQuery{
		Phase:   eventmodel.PhaseAccepted,
		Subject: eventmodel.Subject("progress_digest", "project"),
	})
	if err != nil {
		t.Fatalf("accepted event envelopes: %v", err)
	}
	if len(acceptedEvents) != 1 {
		t.Fatalf("want one accepted event envelope, got %+v", acceptedEvents)
	}
	accepted := acceptedEvents[0].Envelope
	if accepted.Event.Type != "progress_digest.accepted" || accepted.Meta["decision_id"] != material.LocalDecisionID {
		t.Fatalf("accepted envelope should describe the accepted event decision: %+v", accepted)
	}
	if accepted.Meta["accepted_by"] == "" || accepted.Meta["ingest_seq"] == nil {
		t.Fatalf("accepted envelope must carry mnemond acceptance meta: %+v", accepted.Meta)
	}
	st, err := rt.Status("codex@project")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.SyncPending != 1 {
		t.Fatalf("status must report one pending synced event, got %+v", st)
	}

	replicaID := material.OriginReplicaID
	srv.Close()
	if err := rt.Close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
	rt2, err := OpenRuntime(storePath, localRuntimeConfigT([]access.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("reopen local runtime: %v", err)
	}
	defer rt2.Close()
	pending2, err := rt2.store.PendingSyncedEvents()
	if err != nil {
		t.Fatalf("pending synced events after reopen: %v", err)
	}
	if len(pending2) != 1 {
		t.Fatalf("pending synced event must survive restart without duplication:\n before=%+v\n after=%+v", pending, pending2)
	}
	commit2, err := contract.SyncedEventMaterialFromEnvelope(pending2[0])
	if err != nil {
		t.Fatalf("materialize reopened synced event: %v", err)
	}
	if commit2.OriginReplicaID != replicaID || commit2.LocalDecisionID != material.LocalDecisionID {
		t.Fatalf("pending synced event must survive restart without duplication:\n before=%+v\n after=%+v", pending, pending2)
	}
}
