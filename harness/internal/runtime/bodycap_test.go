package runtime

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// An oversize ingest body must be rejected at the edge (a 400), not buffered into memory and decoded.
func TestIngestBodyCapRejectsOversize(t *testing.T) {
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	srv := httptest.NewServer(NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	t.Cleanup(srv.Close)

	body := `{"event":{"type":"memory.observed","payload":{"x":"` + strings.Repeat("a", 2<<20) + `"}}}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/ingest", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Mnemon-Principal", "agent")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversize ingest body must be rejected with 400; got %d", resp.StatusCode)
	}
}
