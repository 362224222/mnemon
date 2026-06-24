package channel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

// principalHeader carries the AUTHENTICATED edge identity. The server trusts THIS, never the request body
// (D7/S9). In production an auth layer (mTLS/OIDC) sets it; httptest sets it from the edge's bound credential.
const PrincipalHeader = "X-Mnemon-Principal"

const principalHeader = PrincipalHeader

// MaxIngestBytes caps an observation request body, so an oversize payload is rejected at the edge
// rather than buffered into memory. interim Phase-1 default; superseded by Phase-2 per-capability
// max_payload_bytes.
const MaxIngestBytes = 1 << 20

// Authenticator resolves the authenticated edge principal from a request — the P3 seam that
// replaces the bare trusted X-Mnemon-Principal header. A production transport binds it to
// mTLS / OIDC / a local-socket peer credential; the default (HeaderAuthenticator) trusts the
// header, which is correct for a local/trusted transport and for httptest. The server still
// trusts ONLY the resolved principal, never the request body (D7/S9).
type Authenticator interface {
	Authenticate(r *http.Request) (contract.ActorID, error)
}

// HeaderAuthenticator trusts the X-Mnemon-Principal header. It is the default seam impl for a
// local/trusted transport (and what httptest uses).
type HeaderAuthenticator struct{}

func (HeaderAuthenticator) Authenticate(r *http.Request) (contract.ActorID, error) {
	p := contract.ActorID(r.Header.Get(principalHeader))
	if p == "" {
		return "", fmt.Errorf("missing authenticated principal")
	}
	return p, nil
}

// TokenAuthenticator resolves the principal from a bearer token via a static token->principal
// map — a minimal non-header seam implementation (mTLS/OIDC slot in the same way). An unknown
// or empty token is rejected, so the body's claimed actor is never trusted.
type TokenAuthenticator struct {
	Tokens map[string]contract.ActorID
}

// bindingAuthenticator resolves each request per the principal's binding auth mode: a principal with a
// token credential MUST present its bearer token; a principal with no credential authenticates via the
// trusted principal header. This lets token-auth and header-auth principals coexist on ONE server
// without it falling into a single global mode (which 401s the header-auth principals when any one
// binding carries a token).
type bindingAuthenticator struct {
	tokens          map[string]contract.ActorID
	tokenPrincipals map[contract.ActorID]bool
}

// NewBindingAuthenticator builds the per-principal authenticator from the loaded bindings' tokens. With
// no tokens it is a plain header authenticator; with all-token it requires bearers; mixed is resolved
// per principal.
func NewBindingAuthenticator(loaded LoadedBindings) Authenticator {
	tp := make(map[contract.ActorID]bool, len(loaded.Tokens))
	for _, p := range loaded.Tokens {
		if p != "" {
			tp[p] = true
		}
	}
	return bindingAuthenticator{tokens: loaded.Tokens, tokenPrincipals: tp}
}

func (a bindingAuthenticator) Authenticate(r *http.Request) (contract.ActorID, error) {
	if tok := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")); tok != "" {
		if p, ok := a.tokens[tok]; ok && p != "" {
			return p, nil
		}
		return "", fmt.Errorf("unrecognized bearer token")
	}
	p := contract.ActorID(r.Header.Get(principalHeader))
	if p == "" {
		return "", fmt.Errorf("missing authenticated principal")
	}
	if a.tokenPrincipals[p] {
		return "", fmt.Errorf("principal %q requires a bearer token", p)
	}
	return p, nil
}

func (a TokenAuthenticator) Authenticate(r *http.Request) (contract.ActorID, error) {
	tok := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if p, ok := a.Tokens[tok]; ok && p != "" {
		return p, nil
	}
	return "", fmt.Errorf("unrecognized bearer token")
}

// IngestReceipt is the channel's reply to an observe: it tells the client the observation was
// recorded (Seq), whether it was a duplicate (Dup), whether the runtime attempted to process it with
// a synchronous Tick (Ticked, P2.2), and any processing error (the observation is durable regardless
// — a Tick failure is reported, not folded into the ingest result).
type IngestReceipt struct {
	Seq             int64  `json:"seq"`
	Dup             bool   `json:"dup"`
	Ticked          bool   `json:"ticked"`
	ProcessingError string `json:"processing_error,omitempty"`
}

// NewHTTPHandler exposes a ServerAPI over net/http with the default HeaderAuthenticator (D5:
// production HTTP/gRPC+mTLS is a thin adapter; this is the thin adapter, gated by httptest).
func NewHTTPHandler(api ServerAPI) http.Handler {
	return NewHTTPHandlerWithAuth(api, HeaderAuthenticator{})
}

// NewHTTPHandlerWithAuth is NewHTTPHandler with an explicit Authenticator seam. The principal is
// resolved by auth; the body carries only the observation. The one ControlServer behind it stays
// the sole serializer — multi-execution-surface, never multi-writer.
func NewHTTPHandlerWithAuth(api ServerAPI, auth Authenticator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxIngestBytes)
		var env contract.ObservationEnvelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seq, dup, err := api.Ingest(principal, env)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(IngestReceipt{Seq: seq, Dup: dup})
	})
	mux.HandleFunc("/event-view", func(w http.ResponseWriter, r *http.Request) {
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
		proj, err := api.PullEventView(principal, sub)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden) // identity/scope violation
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proj)
	})
	return mux
}

// Client is a thin edge-side HTTP client bound to one authenticated credential. It satisfies
// ServerAPI so an edge can speak to a remote server exactly as to an in-process one. The
// credential is either the trusted principal header (NewClient, local/trusted transport) or a
// bearer token resolved by a TokenAuthenticator (NewClientWithToken) — the P3 channel client.
type Client struct {
	baseURL   string
	principal contract.ActorID
	token     string
	http      *http.Client
}

func NewClient(baseURL string, principal contract.ActorID) *Client {
	return &Client{baseURL: baseURL, principal: principal, http: http.DefaultClient}
}

// NewClientWithToken binds the client to a bearer token (resolved server-side by a
// TokenAuthenticator) instead of the trusted principal header.
func NewClientWithToken(baseURL, token string) *Client {
	return &Client{baseURL: baseURL, token: token, http: http.DefaultClient}
}

// setAuth stamps the request with the client's credential: a bearer token when set, else the
// trusted principal header.
func (c *Client) setAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
		return
	}
	req.Header.Set(principalHeader, string(c.principal))
}

var _ ServerAPI = (*Client)(nil)

// IngestObserve POSTs the observation and returns the full channel receipt (seq, dup, processing
// metadata). The principal argument is ignored: the client's identity is its bound credential (the
// trusted header / bearer token), never a per-call claim — an edge cannot forge another's id.
func (c *Client) IngestObserve(_ contract.ActorID, env contract.ObservationEnvelope) (IngestReceipt, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return IngestReceipt{}, err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/ingest", bytes.NewReader(body))
	if err != nil {
		return IngestReceipt{}, err
	}
	c.setAuth(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return IngestReceipt{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return IngestReceipt{}, fmt.Errorf("ingest failed: %s: %s", resp.Status, string(b))
	}
	var out IngestReceipt
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return IngestReceipt{}, err
	}
	return out, nil
}

// Ingest satisfies ServerAPI by delegating to IngestObserve and dropping the processing metadata.
func (c *Client) Ingest(principal contract.ActorID, env contract.ObservationEnvelope) (int64, bool, error) {
	r, err := c.IngestObserve(principal, env)
	return r.Seq, r.Dup, err
}

// Status fetches the channel status evidence for the client's bound principal (P2.3). The principal
// argument is ignored: identity is the bound credential, sent as the trusted header / bearer token.
func (c *Client) Status(_ contract.ActorID) (contract.ChannelStatus, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/status", nil)
	if err != nil {
		return contract.ChannelStatus{}, err
	}
	c.setAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return contract.ChannelStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return contract.ChannelStatus{}, fmt.Errorf("status failed: %s: %s", resp.Status, string(b))
	}
	var st contract.ChannelStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return contract.ChannelStatus{}, err
	}
	return st, nil
}

// PullEventView fetches the actor's scoped view from the server. The principal argument is ignored: the
// subscription's actor is sent in the body and the server cross-checks it against the bound credential header,
// so an edge cannot pull another actor's scope (D7/S9).
func (c *Client) PullEventView(_ contract.ActorID, sub contract.Subscription) (eventview.EventView, error) {
	body, err := json.Marshal(sub)
	if err != nil {
		return eventview.EventView{}, err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/event-view", bytes.NewReader(body))
	if err != nil {
		return eventview.EventView{}, err
	}
	c.setAuth(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return eventview.EventView{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return eventview.EventView{}, fmt.Errorf("pull failed: %s: %s", resp.Status, string(b))
	}
	var proj eventview.EventView
	if err := json.NewDecoder(resp.Body).Decode(&proj); err != nil {
		return eventview.EventView{}, err
	}
	return proj, nil
}

func (c *Client) SyncPush(reqBody contract.SyncPushRequest) (contract.SyncPushResponse, error) {
	var out contract.SyncPushResponse
	if err := c.postJSON("/sync/push", reqBody, &out); err != nil {
		return contract.SyncPushResponse{}, err
	}
	return out, nil
}

func (c *Client) SyncPull(reqBody contract.SyncPullRequest) (contract.SyncPullResponse, error) {
	var out contract.SyncPullResponse
	if err := c.postJSON("/sync/pull", reqBody, &out); err != nil {
		return contract.SyncPullResponse{}, err
	}
	return out, nil
}

func (c *Client) postJSON(path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.setAuth(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s failed: %s: %s", strings.TrimPrefix(path, "/"), resp.Status, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
