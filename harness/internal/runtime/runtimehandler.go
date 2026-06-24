package runtime

import (
	"encoding/json"
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// NewRuntimeHandler is the Local Mnemon HTTP channel endpoint over a Runtime.
// It differs from the api-only access.NewHTTPHandler in two ways the Runtime makes possible:
//
//   - P2.2 synchronous local mode: after a successful NEW observation, /ingest drives ONE Tick on the
//     runtime's single driver, so a lone observe closes the governed loop. The Tick is serialized by
//     the ControlServer's tickMu — no surface drives Tick independently. A duplicate observation is
//     not re-ticked. A Tick error is reported in the receipt, never folded into the ingest result
//     (the observation is durable regardless).
//   - P2.3 /status: channel evidence (principal, digest, binding actor kind, store ref, mode).
//
// Auth resolves the principal; the request body never names identity (D7/S9).
func NewRuntimeHandler(rt *Runtime, auth access.Authenticator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events/observed", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, access.MaxIngestBytes)
		var env eventmodel.EventEnvelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seq, dup, err := rt.IngestObservedEnvelope(principal, env)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rec := access.IngestReceipt{Seq: seq, Dup: dup}
		if !dup {
			rec.Ticked = true
			if _, terr := rt.Tick(); terr != nil {
				rec.ProcessingError = terr.Error()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rec)
	})
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, access.MaxIngestBytes)
		var env contract.ObservationEnvelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seq, dup, err := rt.API().Ingest(principal, env)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rec := access.IngestReceipt{Seq: seq, Dup: dup}
		// Synchronous local mode: a NEW observation is processed by one Tick now. A duplicate was
		// already processed on its first ingest, so it is not re-ticked.
		if !dup {
			rec.Ticked = true
			if _, terr := rt.Tick(); terr != nil {
				rec.ProcessingError = terr.Error()
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rec)
	})
	mux.HandleFunc("/presentation-view", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		var sub contract.Subscription
		if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		proj, err := rt.API().PullPresentationView(principal, sub)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proj)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		st, err := rt.Status(principal)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(st)
	})
	mux.HandleFunc("/sync/push", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		var req contract.SyncPushRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := rt.SyncPush(principal, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/sync/pull", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		var req contract.SyncPullRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := rt.SyncPull(principal, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/sync/status", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		resp, err := rt.SyncStatus(principal)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return mux
}
