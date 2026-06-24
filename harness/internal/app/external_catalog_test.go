package app

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

const goalPackageSpec = `{"schema_version":1,"name":"goal","observed_type":"goal.write_candidate.observed",
"proposed_type":"goal.write.proposed","resource_kind":"goal","items_field":"items",
"fields":[{"name":"statement","validators":[{"id":"required","params":{"missing_style":"empty"}},{"id":"safety:unsafe"}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# Goals","field":"statement"}},"static":{"statement":"project"}}}`

func writeExternalGoalPackage(t *testing.T, projectRoot, name, spec string) string {
	t.Helper()
	dir := filepath.Join(projectRoot, ".mnemon", "loops", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "capability.json")
	if err := os.WriteFile(file, []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	return file
}

// Boot fail-closed: a bad external package REFUSES catalog resolution — the directory's presence
// is a contract, not a hint.
func TestResolveBootCatalogFailClosedOnBadExternalPackage(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "bad", `{nope`)
	if _, _, err := resolveBootCatalog(root, false, io.Discard); err == nil || !strings.Contains(err.Error(), ".mnemon/loops/bad") {
		t.Fatalf("bad external package must refuse boot and name its path, got %v", err)
	}
}

// The operator escape hatch: --ignore-external boots the embedded-only catalog, names every
// ignored package on stderr (one line each), and returns the ignored names so the serve path can
// disable the matching loops.
func TestResolveBootCatalogIgnoreExternalNamesIgnoredPackages(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "bad", `{nope`)
	writeExternalGoalPackage(t, root, "goal", goalPackageSpec)
	var errw bytes.Buffer
	catalog, ignored, err := resolveBootCatalog(root, true, &errw)
	if err != nil {
		t.Fatalf("--ignore-external must boot embedded-only even with a bad package present: %v", err)
	}
	if _, ok := catalog["goal"]; ok {
		t.Fatal("--ignore-external must NOT load the external goal capability")
	}
	if len(catalog) != len(policy.EmbeddedCatalog()) {
		t.Fatalf("--ignore-external catalog must be embedded-only (%d), got %d", len(policy.EmbeddedCatalog()), len(catalog))
	}
	if len(ignored) != 2 || ignored[0] != "bad" || ignored[1] != "goal" {
		t.Fatalf("ignored names must carry both packages [bad goal], got %v", ignored)
	}
	lines := strings.Split(strings.TrimSpace(errw.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want one stderr line PER ignored package (2), got %d:\n%s", len(lines), errw.String())
	}
	for _, name := range []string{".mnemon/loops/bad", ".mnemon/loops/goal"} {
		if !strings.Contains(errw.String(), name) {
			t.Fatalf("stderr must name ignored package %s:\n%s", name, errw.String())
		}
	}
}

// The serve path resolves the catalog ONCE at boot and refuses to start on a resolve error —
// before any listener exists.
func TestRunLocalServerRefusesToStartOnBadExternalPackage(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "bad", `{nope`)
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787",
		[]contract.ResourceRef{{Kind: "progress_digest", ID: "project"}})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	err := RunLocalHTTPServerWithBindings(context.Background(), "127.0.0.1:0",
		filepath.Join(t.TempDir(), "governed.db"),
		access.LoadedBindings{Bindings: []access.ChannelBinding{binding}},
		ServeOptions{Loops: []string{"progress_digest"}, ProjectRoot: root}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), ".mnemon/loops/bad") {
		t.Fatalf("local serve must refuse to start on a bad external package, got %v", err)
	}
}

// firstWriteNotifier closes ready on the first write: the serve path's "listening" banner is the
// boot-succeeded signal for the test below.
type firstWriteNotifier struct {
	once  sync.Once
	ready chan struct{}
}

func (n *firstWriteNotifier) Write(p []byte) (int, error) {
	n.once.Do(func() { close(n.ready) })
	return len(p), nil
}

// The PRIMARY --ignore-external scenario: the operator ENABLED an external loop (config.loops
// carries its name) and the package then went bad. Ignoring only the catalog would still sink
// boot on `unknown rule_ref "native:goal"` — the serve path must also DISABLE the ignored
// package's loop, name it on stderr, and serve on the embedded loops.
func TestRunLocalServerIgnoreExternalDisablesEnabledExternalLoop(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "goal", `{nope`)
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787",
		[]contract.ResourceRef{{Kind: "progress_digest", ID: "project"}})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}

	// Both ignore lines are product stderr surface (the serve path hardcodes os.Stderr), so the
	// test captures os.Stderr through a pipe for the duration of the boot.
	oldStderr := os.Stderr
	t.Cleanup(func() { os.Stderr = oldStderr })
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = pw

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	errc := make(chan error, 1)
	go func() {
		errc <- RunLocalHTTPServerWithBindings(ctx, "127.0.0.1:0",
			filepath.Join(t.TempDir(), "governed.db"),
			access.LoadedBindings{Bindings: []access.ChannelBinding{binding}},
			ServeOptions{Loops: []string{"progress_digest", "goal"}, ProjectRoot: root, IgnoreExternal: true},
			&firstWriteNotifier{ready: ready})
	}()
	select {
	case <-ready: // boot reached the listening banner
	case bootErr := <-errc:
		os.Stderr = oldStderr
		t.Fatalf("--ignore-external boot with an enabled-then-corrupted external loop must succeed, got %v", bootErr)
	case <-time.After(10 * time.Second):
		os.Stderr = oldStderr
		t.Fatal("server never reported listening")
	}
	cancel()
	if serveErr := <-errc; serveErr != nil {
		os.Stderr = oldStderr
		t.Fatalf("serve must shut down cleanly, got %v", serveErr)
	}
	pw.Close()
	os.Stderr = oldStderr
	captured, err := io.ReadAll(pr)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"mnemon-harness: --ignore-external: ignoring external package .mnemon/loops/goal",
		"mnemon-harness: --ignore-external: disabling loop goal",
	} {
		if !strings.Contains(string(captured), want) {
			t.Fatalf("stderr must carry %q, got:\n%s", want, captured)
		}
	}
}

// Equal admission rights: the resolved catalog threads through the SAME select-only assembly the
// embedded loops use — an external goal package admits a candidate end to end.
func TestExternalGoalCapabilityAdmitsThroughResolvedCatalog(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "goal", goalPackageSpec)
	catalog, err := policy.ResolveCatalog(root, state.DefaultSchemaGuard().Required)
	if err != nil {
		t.Fatalf("resolve catalog: %v", err)
	}
	ref := contract.ResourceRef{Kind: "goal", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"goal.write_candidate.observed"}

	rc, err := LocalRuntimeConfigFromBindings([]access.ChannelBinding{binding}, catalog)
	if err != nil {
		t.Fatalf("boot config with external catalog: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "g1",
		Event:      contract.Event{Type: "goal.write_candidate.observed", Payload: map[string]any{"statement": "ship stage five"}},
	}); err != nil {
		t.Fatalf("ingest goal: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil || v == 0 {
		t.Fatalf("external goal capability must admit (v=%d err=%v)", v, err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "ship stage five") {
		t.Fatalf("goal content missing the candidate: %q", content)
	}
}

func TestSetupAcceptsExternalCapabilityLoop(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "goal", goalPackageSpec)
	var out, errw bytes.Buffer
	res, err := New(root).Setup(context.Background(), &out, &errw, SetupOptions{
		Host: "codex", Loops: []string{"goal"}, Principal: "codex@project", ProjectRoot: root,
	})
	if err != nil {
		t.Fatalf("setup --loop goal must enable an external capability package: %v\nstderr=%s", err, errw.String())
	}
	if config := string(mustRead(t, res.ConfigFile)); !strings.Contains(config, `"goal"`) {
		t.Fatalf("setup config must record the external loop:\n%s", config)
	}
	binding := string(mustRead(t, res.BindingFile))
	if !strings.Contains(binding, "goal.write_candidate.observed") || !strings.Contains(binding, `"kind": "goal"`) {
		t.Fatalf("binding must grant the external capability scope:\n%s", binding)
	}

	// A loop that is neither embedded nor an external package keeps the original diagnosis.
	_, err = New(root).Setup(context.Background(), &out, &errw, SetupOptions{
		Host: "codex", Loops: []string{"nope"}, Principal: "codex@project", ProjectRoot: root,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported event package") {
		t.Fatalf("an unknown loop must keep the unsupported-event-package error, got %v", err)
	}
}

// Uninstall is zero-impact on external packages: the package is channel/boot surface, not host
// projection surface.
func TestUninstallLeavesExternalPackagesUntouched(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out bytes.Buffer
	opts := SetupOptions{Host: "codex", Loops: []string{"progress_digest"}, Principal: "codex@project", ProjectRoot: root}
	if _, err := h.Setup(context.Background(), &out, &out, opts); err != nil {
		t.Fatalf("setup: %v", err)
	}
	pkgFile := writeExternalGoalPackage(t, root, "goal", goalPackageSpec)
	before, err := os.ReadFile(pkgFile)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.SetupUninstall(context.Background(), &out, &out, opts); err != nil {
		t.Fatalf("uninstall with an external package present must succeed: %v", err)
	}
	if after, err := os.ReadFile(pkgFile); err != nil || !bytes.Equal(after, before) {
		t.Fatalf("uninstall must not touch the external package (err=%v)", err)
	}
}

// loop validate reports each external capability package with a source-labelled OK line and goes
// red on any loader failure — the same fail-closed resolution boot uses.
func TestLoopValidateReportsExternalCapabilityPackages(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "goal", goalPackageSpec)
	lines, err := New(root).LoopValidate()
	if err != nil {
		t.Fatalf("loop validate with a well-formed external package: %v", err)
	}
	found := false
	for _, l := range lines {
		if l == "external capability goal: OK" {
			found = true
		}
	}
	if !found {
		t.Fatalf("loop validate must report `external capability goal: OK`; got %v", lines)
	}

	badRoot := t.TempDir()
	writeExternalGoalPackage(t, badRoot, "bad", `{nope`)
	if _, err := New(badRoot).LoopValidate(); err == nil || !strings.Contains(err.Error(), ".mnemon/loops/bad") {
		t.Fatalf("loop validate must go red on a bad external package, got %v", err)
	}
}
