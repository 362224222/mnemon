// mnemon-hub is the standalone Remote Workspace hub: the mnemonhub wire (sync.push / sync.pull /
// sync.status) over its own store, authenticated by bearer tokens from an operator-supplied
// replicas.json. It is a SEPARATE trust domain from the local runtime: it imports contract/store/
// mnemonhub only — never channel / runtime / app / hostagent (pinned by the mnemonhub boundary
// test). One mnemon-hub per hub store (the store's single-writer flock enforces it).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "mnemon-hub: %v\n", err)
		os.Exit(1)
	}
}

// run is the whole binary behind a testable seam: parse flags, handle the --dev-selfsigned
// generator exit, load replicas.json (fail-closed), take the hub store's single-writer lock, and
// serve the three sync verbs (TLS when both cert+key are set) until ctx cancels.
func run(ctx context.Context, args []string, out, errw io.Writer) error {
	fs := flag.NewFlagSet("mnemon-hub", flag.ContinueOnError)
	fs.SetOutput(errw)
	addr := fs.String("addr", "127.0.0.1:9787", "listen address")
	storePath := fs.String("store", "", "hub store path (sqlite; mnemon-hub takes its single-writer lock)")
	replicasPath := fs.String("replicas", "", "replicas.json path (operator-supplied; 0600 file in a 0700 dir)")
	tlsCert := fs.String("tls-cert", "", "TLS certificate file (TLS is served when --tls-cert and --tls-key are both set)")
	tlsKey := fs.String("tls-key", "", "TLS private key file")
	devSelfsigned := fs.String("dev-selfsigned", "", "generate a self-signed dev/e2e cert+key pair into this directory, print their paths, and exit")
	fs.Usage = func() {
		fmt.Fprintln(errw, "mnemon-hub is the remote event exchange backend: it handles authenticated replica push, pull, status, cursors, and tenant boundaries.")
		fmt.Fprintln(errw)
		fmt.Fprintln(errw, "Usage of mnemon-hub:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *devSelfsigned != "" {
		certPath, keyPath, err := generateSelfSigned(*devSelfsigned)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "mnemon-hub: dev TLS cert %s\n", certPath)
		fmt.Fprintf(out, "mnemon-hub: dev TLS key %s\n", keyPath)
		return nil
	}
	if *storePath == "" || *replicasPath == "" {
		return fmt.Errorf("--store and --replicas are required")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return fmt.Errorf("--tls-cert and --tls-key must be set together")
	}
	grants, tokens, err := loadReplicas(*replicasPath)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(*storePath); dir != "" && dir != "." {
		// T1 floor: the hub store dir is private state — owner-only, like every local creation site.
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create hub store dir: %w", err)
		}
	}
	st, err := state.OpenStore(*storePath)
	if err != nil {
		return fmt.Errorf("open hub store: %w", err)
	}
	defer st.Close()
	now := func() string { return time.Now().UTC().Format(time.RFC3339) }
	// Audit goes to out (stdout in main): one line per request — ts, principal, verb, result.
	handler := mnemonhub.NewHTTPHandler(mnemonhub.New(st, grants, now), mnemonhub.BearerAuthenticator{Tokens: tokens}, out)
	return serveHub(ctx, *addr, handler, *tlsCert, *tlsKey, *storePath, out)
}

// serveHub listens (so the bound address is printable before any request) and serves until ctx
// cancels, then shuts down cleanly. With cert+key it serves TLS natively.
func serveHub(ctx context.Context, addr string, handler http.Handler, certFile, keyFile, storePath string, out io.Writer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	// Timeouts harden the FIRST network-facing daemon against slowloris (a slow/idle peer holding a
	// connection open indefinitely). The loopback-only local control server is left unchanged.
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	scheme := "http"
	if certFile != "" {
		scheme = "https"
	}
	errc := make(chan error, 1)
	go func() {
		if certFile != "" {
			errc <- srv.ServeTLS(ln, certFile, keyFile)
			return
		}
		errc <- srv.Serve(ln)
	}()
	fmt.Fprintf(out, "mnemon-hub: listening on %s://%s (store %s)\n", scheme, ln.Addr(), storePath)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		fmt.Fprintln(out, "mnemon-hub: shut down")
		return nil
	case serveErr := <-errc:
		if serveErr == http.ErrServerClosed {
			return nil
		}
		return serveErr
	}
}
