package channel

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// The T2 downgrade gate (v1.1 #3): plaintext is allowed only inside the loopback boundary; a
// non-loopback http endpoint is fail-closed unless explicitly overridden; https always passes.
func TestValidateSyncEndpointFailClosesPlaintextNonLoopback(t *testing.T) {
	allowed := []string{
		"https://hub.example.test:9787",
		"http://127.0.0.1:9787",
		"http://localhost:9787",
		"http://[::1]:9787",
	}
	for _, ep := range allowed {
		if err := ValidateSyncEndpoint(ep, false); err != nil {
			t.Fatalf("%s must be allowed: %v", ep, err)
		}
	}
	denied := []string{
		"http://hub.example.test:9787",
		"http://10.0.0.7:9787",
		"http://",
		"ftp://hub.example.test",
		"",
	}
	for _, ep := range denied {
		if err := ValidateSyncEndpoint(ep, false); err == nil {
			t.Fatalf("%s must be refused without the explicit override", ep)
		}
	}
	if err := ValidateSyncEndpoint("http://hub.example.test:9787", true); err != nil {
		t.Fatalf("the explicit insecure override must pass plaintext: %v", err)
	}
	if err := ValidateSyncEndpoint("ftp://hub.example.test", true); err == nil {
		t.Fatal("a non-http(s) scheme must be refused even with the override")
	}
	if _, err := NewSyncClient("http://hub.example.test:9787", SyncClientConfig{Token: "t"}); err == nil {
		t.Fatal("client construction must apply the same gate")
	}
}

// Every sync client is bounded (v1.1 #10) and a configured ca_file becomes the ONLY trusted root.
func TestNewSyncClientBoundedTimeoutAndCAFile(t *testing.T) {
	c, err := NewSyncClient("http://127.0.0.1:9787", SyncClientConfig{Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if c.http.Timeout != DefaultSyncTimeout {
		t.Fatalf("default timeout must be bounded, got %v", c.http.Timeout)
	}
	c, err = NewSyncClient("http://127.0.0.1:9787", SyncClientConfig{Token: "t", Timeout: 3 * time.Second})
	if err != nil || c.http.Timeout != 3*time.Second {
		t.Fatalf("explicit timeout must stick: %v err=%v", c.http.Timeout, err)
	}
	if _, err := NewSyncClient("https://hub.test", SyncClientConfig{Token: "t", CAFile: filepath.Join(t.TempDir(), "missing.pem")}); err == nil {
		t.Fatal("a missing ca_file must fail client construction")
	}
	junk := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSyncClient("https://hub.test", SyncClientConfig{Token: "t", CAFile: junk}); err == nil {
		t.Fatal("a ca_file with no usable PEM must fail client construction")
	}
}

// Full TLS round trip: a server presenting a self-signed cert is trusted ONLY when its cert is the
// client's ca_file; without it the handshake fails (no silent fallback to the system pool).
func TestSyncClientTLSRoundTripWithCAFile(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sync/status" {
			http.NotFound(w, r)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "no bearer", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(contract.SyncStatusResponse{Principal: "replica-a@team", RemoteWorkspace: "connected", HubEventsReceived: 7})
	}))
	defer srv.Close()

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	trusted, err := NewSyncClient(srv.URL, SyncClientConfig{Token: "tok", CAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	st, err := trusted.SyncStatus()
	if err != nil {
		t.Fatalf("status over pinned TLS: %v", err)
	}
	if st.HubEventsReceived != 7 || st.Principal != "replica-a@team" {
		t.Fatalf("unexpected status payload: %+v", st)
	}

	untrusted, err := NewSyncClient(srv.URL, SyncClientConfig{Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := untrusted.SyncStatus(); err == nil {
		t.Fatal("an unpinned client must refuse the self-signed server")
	}
}
