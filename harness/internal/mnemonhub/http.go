package mnemonhub

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// maxSyncBodyBytes caps a sync request body so an oversize batch is rejected at the edge rather
// than buffered into memory (mirrors the channel's ingest cap; a 100-event pull page fits easily).
const maxSyncBodyBytes = 8 << 20

// Authenticator resolves the authenticated principal from a request. mnemonhub carries its OWN
// seam (not channel's) so the standalone hub never imports channel; mnemon-hub plugs in
// BearerAuthenticator, tests may plug fakes.
type Authenticator interface {
	Authenticate(r *http.Request) (contract.ActorID, error)
}

// BearerAuthenticator resolves the principal from a static bearer-token map — the mnemon-hub
// authenticator built from replicas.json credential_refs. A missing, empty, or unknown token is
// rejected; the request body never names identity.
type BearerAuthenticator struct {
	Tokens map[string]contract.ActorID
}

func (a BearerAuthenticator) Authenticate(r *http.Request) (contract.ActorID, error) {
	tok := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if tok == "" {
		return "", fmt.Errorf("unrecognized bearer token")
	}
	// Constant-time compare against the (small) registered token set: tokens are high-entropy so
	// this is hardening, but a map-lookup-only check leaks a length/match timing signal. Scan ALL
	// entries and accumulate the match (no early return on a hit) so the work is token-independent.
	tokBytes := []byte(tok)
	var matched contract.ActorID
	for known, principal := range a.Tokens {
		if subtle.ConstantTimeCompare(tokBytes, []byte(known)) == 1 && principal != "" {
			matched = principal
		}
	}
	if matched != "" {
		return matched, nil
	}
	return "", fmt.Errorf("unrecognized bearer token")
}

// NewHTTPHandler serves the three sync verbs over the hub Server. Every request emits ONE audit
// line to audit (ts, principal, verb, result): result is "unauthorized" (401, principal "-"),
// "bad_request" (400 — bad JSON / missing field / wrong method), "denied" (403 — no grant /
// out-of-scope), or "ok". The "result" is the REQUEST-level outcome (auth/parse/method); the
// per-event accept/reject/conflict verdicts ride the 200 response body, NOT the audit line — a
// request whose every event is rejected still audits result=ok (LOW-13). nil audit discards.
//
// Method gates pin the frozen verb→method mapping (sync-abi-v1 §1): push/pull are POST, status is
// GET or POST. A violation is 405 before auth (the method is request-shape, not identity).
func NewHTTPHandler(hub *Server, auth Authenticator, audit io.Writer) http.Handler {
	if audit == nil {
		audit = io.Discard
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/sync/push", func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, audit, contract.SyncVerbPush, http.MethodPost) {
			return
		}
		principal, ok := authenticate(w, r, auth, audit, contract.SyncVerbPush)
		if !ok {
			return
		}
		var req contract.SyncPushRequest
		if !decodeBody(w, r, &req, audit, principal, contract.SyncVerbPush) {
			return
		}
		resp, err := hub.Push(principal, req)
		respond(w, resp, err, audit, principal, contract.SyncVerbPush)
	})
	mux.HandleFunc("/sync/pull", func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, audit, contract.SyncVerbPull, http.MethodPost) {
			return
		}
		principal, ok := authenticate(w, r, auth, audit, contract.SyncVerbPull)
		if !ok {
			return
		}
		var req contract.SyncPullRequest
		if !decodeBody(w, r, &req, audit, principal, contract.SyncVerbPull) {
			return
		}
		resp, err := hub.Pull(principal, req)
		respond(w, resp, err, audit, principal, contract.SyncVerbPull)
	})
	mux.HandleFunc("/sync/status", func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, audit, contract.SyncVerbStatus, http.MethodGet, http.MethodPost) {
			return
		}
		principal, ok := authenticate(w, r, auth, audit, contract.SyncVerbStatus)
		if !ok {
			return
		}
		resp, err := hub.Status(principal)
		respond(w, resp, err, audit, principal, contract.SyncVerbStatus)
	})
	return mux
}

// allowMethods enforces the frozen per-route verb→method mapping (sync-abi-v1 §1): a disallowed
// method is 405 with an Allow header, audited as bad_request (request-shape, not identity — so the
// principal is "-", auth has not run yet).
func allowMethods(w http.ResponseWriter, r *http.Request, audit io.Writer, verb string, allowed ...string) bool {
	for _, m := range allowed {
		if r.Method == m {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	auditLine(audit, "-", verb, "bad_request")
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func authenticate(w http.ResponseWriter, r *http.Request, auth Authenticator, audit io.Writer, verb string) (contract.ActorID, bool) {
	principal, err := auth.Authenticate(r)
	if err != nil {
		auditLine(audit, "-", verb, "unauthorized")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return "", false
	}
	return principal, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, into any, audit io.Writer, principal contract.ActorID, verb string) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxSyncBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		auditLine(audit, string(principal), verb, "bad_request")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, resp any, err error, audit io.Writer, principal contract.ActorID, verb string) {
	if err != nil {
		// LOW-10: distinguish a request-validation failure (400 / bad_request) from an
		// authorization/grant failure (403 / denied). Everything not marked BadRequestError is an
		// authorization refusal (no grant, out-of-scope clamp).
		if IsBadRequest(err) {
			auditLine(audit, string(principal), verb, "bad_request")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		auditLine(audit, string(principal), verb, "denied")
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	auditLine(audit, string(principal), verb, "ok")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func auditLine(audit io.Writer, principal, verb, result string) {
	fmt.Fprintf(audit, "%s principal=%s verb=%s result=%s\n",
		time.Now().UTC().Format(time.RFC3339), principal, verb, result)
}
