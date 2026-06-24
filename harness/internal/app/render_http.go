package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

const renderAuditRelPath = ".mnemon/harness/local/render-audit.jsonl"

// NewLocalHTTPHandler adds the R1 read-only render endpoint at the app wiring layer. Runtime/channel
// still own observe/pull/status/sync; render reads only the authenticated actor's scoped view.
func NewLocalHTTPHandler(rt *runtime.Runtime, auth access.Authenticator, bindings *access.BindingSet, renderer presentation.Renderer) http.Handler {
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
		var binding access.ChannelBinding
		haveBinding := false
		if bindings != nil {
			b, ok := bindings.Binding(principal)
			if !ok {
				http.Error(w, fmt.Sprintf("no channel binding for principal %q", principal), http.StatusForbidden)
				return
			}
			if !b.Allows(access.VerbRender) {
				http.Error(w, fmt.Sprintf("principal %q is not bound to render", principal), http.StatusForbidden)
				return
			}
			binding = b
			haveBinding = true
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		var req presentation.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req.Principal = principal
		proj, err := rt.API().PullEventView(principal, contract.Subscription{Actor: principal})
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		if haveBinding {
			proj = budgetShapeEventView(proj, policy.EmbeddedCatalog(), binding.Budget)
			if req.Budget.EventViewTier == "" {
				req.Budget.EventViewTier = binding.Budget
			}
		}
		resp, err := renderer.RenderPresentation(r.Context(), req, proj)
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

func ServeLocalHTTP(ctx context.Context, addr string, rt *runtime.Runtime, auth access.Authenticator, loaded access.LoadedBindings, projectRoot string, out io.Writer) error {
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		return err
	}
	auditPath := ""
	if projectRoot != "" {
		auditPath = filepath.Join(projectRoot, renderAuditRelPath)
	}
	renderer := presentation.Renderer{AuditSink: &presentation.JSONLAuditSink{Path: auditPath}}
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
