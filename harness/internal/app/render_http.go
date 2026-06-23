package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/render"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

const renderAuditRelPath = ".mnemon/harness/local/render-audit.jsonl"

// NewLocalHTTPHandler adds the R1 read-only render endpoint at the app wiring layer. Runtime/channel
// still own observe/pull/status/sync; render reads only the authenticated actor's scoped projection.
func NewLocalHTTPHandler(rt *runtime.Runtime, auth channel.Authenticator, bindings *channel.BindingSet, renderer render.Renderer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/render", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		principal, err := auth.Authenticate(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		if bindings != nil {
			b, ok := bindings.Binding(principal)
			if !ok {
				http.Error(w, fmt.Sprintf("no channel binding for principal %q", principal), http.StatusForbidden)
				return
			}
			if !b.Allows(channel.VerbRender) {
				http.Error(w, fmt.Sprintf("principal %q is not bound to render", principal), http.StatusForbidden)
				return
			}
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		var req render.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req.Principal = principal
		proj, err := rt.API().PullProjection(principal, contract.Subscription{Actor: principal})
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		resp, err := renderer.RenderCue(r.Context(), req, proj)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.Handle("/", runtime.NewRuntimeHandler(rt, auth))
	return mux
}

func ServeLocalHTTP(ctx context.Context, addr string, rt *runtime.Runtime, auth channel.Authenticator, loaded channel.LoadedBindings, projectRoot string, out io.Writer) error {
	bindings, err := channel.NewBindingSet(loaded.Bindings...)
	if err != nil {
		return err
	}
	auditPath := ""
	if projectRoot != "" {
		auditPath = filepath.Join(projectRoot, renderAuditRelPath)
	}
	renderer := render.Renderer{AuditSink: &render.JSONLAuditSink{Path: auditPath}}
	srv := &http.Server{Addr: addr, Handler: NewLocalHTTPHandler(rt, auth, bindings, renderer)}
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
