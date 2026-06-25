package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// DefaultStorePath is the canonical Local Mnemon kernel-store path under the
// project's `.mnemon/harness` tree. Tests and dev may override it with an
// explicit path.
const DefaultStorePath = ".mnemon/harness/local/governed.db"

// RunHTTPServerWithBindings boots the server from a loaded channel-binding manifest (P3.2): the
// runtime enforces the bindings (access.BindingSet authorizer) and serves only the subscription scopes the
// bindings declare, and — when the bindings carry credential refs — a access.TokenAuthenticator resolves the
// principal from the bearer token (trusted-header auth remains the local/dev/httptest default when no
// tokens are configured). The store path is still the canonical project store.
func RunHTTPServerWithBindings(ctx context.Context, addr, storePath string, loaded access.LoadedBindings, out io.Writer) error {
	rt, err := OpenRuntime(storePath, RuntimeConfig{
		Bindings: loaded.Bindings,
		Subs:     access.SubsFromBindings(loaded.Bindings),
	})
	if err != nil {
		return err
	}
	defer rt.Close()
	return ServeRuntime(ctx, addr, rt, access.NewBindingAuthenticator(loaded), out)
}

// ServeRuntime serves the runtime's channel over httpapi until ctx is cancelled. It is the shared
// boot loop for the bare and binding-configured server front doors.
func ServeRuntime(ctx context.Context, addr string, rt *Runtime, auth access.Authenticator, out io.Writer) error {
	srv := &http.Server{Addr: addr, Handler: NewRuntimeHandler(rt, auth)}
	errc := make(chan error, 1)
	go func() {
		fmt.Fprintf(out, "Local Mnemon: listening on %s (store %s)\n", addr, rt.StorePath())
		if serveErr := srv.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			errc <- serveErr
			return
		}
		errc <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		fmt.Fprintln(out, "Local Mnemon: shut down")
		return nil
	case serveErr := <-errc:
		return serveErr
	}
}
