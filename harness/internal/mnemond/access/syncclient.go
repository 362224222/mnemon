package access

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// DefaultSyncTimeout bounds every sync transport call (v1.1 #10): a hung remote can never wedge the
// caller — the in-process worker stays off the Tick path, but even its own goroutine must converge.
const DefaultSyncTimeout = 10 * time.Second

// SyncClientConfig configures the replica sync client (sync-abi-v1 §8): the bearer credential, a
// bounded transport budget, an optional CA bundle pinning the remote's TLS root, and the explicit
// plaintext override.
type SyncClientConfig struct {
	Token         string
	Timeout       time.Duration // <= 0 defaults to DefaultSyncTimeout
	CAFile        string        // PEM bundle for the remote's TLS root (e.g. mnemon-hub --dev-selfsigned cert)
	AllowInsecure bool          // explicit T2 downgrade override for a plaintext non-loopback endpoint
}

// NewSyncClient builds the sync client over endpoint: fail-closed on a plaintext non-loopback
// endpoint (the T2 downgrade gate, v1.1 #3) and always bounded by a timeout. With a CAFile the
// transport trusts exactly that root — nothing else.
func NewSyncClient(endpoint string, cfg SyncClientConfig) (*Client, error) {
	if err := ValidateSyncEndpoint(endpoint, cfg.AllowInsecure); err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultSyncTimeout
	}
	hc := &http.Client{Timeout: timeout}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read sync ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("sync ca_file %s holds no usable PEM certificate", cfg.CAFile)
		}
		hc.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	}
	return &Client{baseURL: strings.TrimRight(strings.TrimSpace(endpoint), "/"), token: cfg.Token, http: hc}, nil
}

// ValidateSyncEndpoint is the client half of the TLS downgrade gate: an http:// endpoint with a
// non-loopback host is refused unless explicitly overridden (mirroring the stage-4 listen-side
// loopback-only posture). https always passes; loopback plaintext stays allowed (same-machine
// hubs and tests are inside the T1 boundary). `sync connect` applies the same check at write time.
func ValidateSyncEndpoint(endpoint string, allowInsecure bool) error {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return fmt.Errorf("parse sync endpoint: %w", err)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if host == "localhost" {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		if allowInsecure {
			return nil
		}
		return fmt.Errorf("refusing plaintext sync endpoint %q with non-loopback host (T2 downgrade); use https with ca_file, or pass --allow-insecure-remote to override explicitly", endpoint)
	default:
		return fmt.Errorf("sync endpoint %q must be http(s)", endpoint)
	}
}

// SyncStatus fetches the hub's sync status evidence (counters) for the bound credential.
func (c *Client) SyncStatus() (contract.SyncStatusResponse, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/sync/status", nil)
	if err != nil {
		return contract.SyncStatusResponse{}, err
	}
	c.setAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return contract.SyncStatusResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return contract.SyncStatusResponse{}, fmt.Errorf("sync/status failed: %s: %s", resp.Status, string(b))
	}
	var out contract.SyncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return contract.SyncStatusResponse{}, err
	}
	return out, nil
}
