package coreguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type mainAxisOwner string

const (
	ownerEvent     mainAxisOwner = "event"
	ownerHostAgent mainAxisOwner = "hostagent"
	ownerMnemond   mainAxisOwner = "mnemond"
	ownerMnemonhub mainAxisOwner = "mnemonhub"
	ownerGuard     mainAxisOwner = "guard"
)

type packageMainAxis struct {
	owner  mainAxisOwner
	role   string
	target string
}

// packageMainAxisInventory is the executable inventory for the main-axis convergence goal. It does
// not claim the current package names are final; it prevents new top-level implementation concepts
// from appearing without an explicit owner under hostagent, mnemond, mnemonhub, or event.
var packageMainAxisInventory = map[string]packageMainAxis{
	"app":        {owner: ownerMnemond, role: "daemon wiring and local mnemond boot", target: "mnemond"},
	"assembler":  {owner: ownerMnemond, role: "mnemond policy/runtime assembly", target: "mnemond"},
	"assets":     {owner: ownerHostAgent, role: "hostagent integration and event-policy assets", target: "hostagent/assets"},
	"capability": {owner: ownerMnemond, role: "event type schema and admission policy", target: "mnemond/policy"},
	"channel":    {owner: ownerMnemond, role: "hostagent to mnemond access layer", target: "mnemond/access"},
	"codexapp":   {owner: ownerHostAgent, role: "Codex hostagent appserver adapter", target: "hostagent/codexapp"},
	"config":     {owner: ownerMnemond, role: "mnemond local configuration", target: "mnemond/config"},
	"contract":   {owner: ownerEvent, role: "event and mnemond boundary DTOs", target: "event/contract"},
	"coreguard":  {owner: ownerGuard, role: "architecture guard tests", target: "coreguard"},
	"driver":     {owner: ownerMnemond, role: "mnemond tick driver", target: "mnemond/daemon"},
	"event":      {owner: ownerEvent, role: "canonical event and envelope model", target: "event"},
	"eventstore": {owner: ownerEvent, role: "event envelope append/read facade", target: "event/store"},
	"eventview":  {owner: ownerMnemond, role: "hostagent-facing derived event read model", target: "mnemond/presentation"},
	"hostagent":  {owner: ownerHostAgent, role: "hostagent setup and thin shims", target: "hostagent"},
	"kernel":     {owner: ownerMnemond, role: "materialized event state applier", target: "mnemond/state"},
	"mnemonhub":  {owner: ownerMnemonhub, role: "remote accepted event exchange server and exchange mechanics", target: "mnemonhub"},
	"reconcile":  {owner: ownerMnemond, role: "event admission/materialization driver", target: "mnemond/admission"},
	"replay":     {owner: ownerMnemond, role: "mnemond determinism verification", target: "mnemond/replay"},
	"rule":       {owner: ownerMnemond, role: "event admission policy primitive", target: "mnemond/admission"},
	"runtime":    {owner: ownerMnemond, role: "local mnemond event runtime", target: "mnemond"},
	"store":      {owner: ownerMnemond, role: "local mnemond event/state store", target: "mnemond/state"},
	"ui":         {owner: ownerMnemond, role: "read-only mnemond operator observability", target: "mnemond/observe"},
}

var demotedMainAxisPackages = map[string]bool{
	"capability": true,
	"channel":    true,
	"eventview":  true,
	"kernel":     true,
	"reconcile":  true,
	"rule":       true,
	"store":      true,
}

var nestedMainAxisInventory = map[string]packageMainAxis{
	"mnemond/presentation": {owner: ownerMnemond, role: "derived event presentation for hostagents", target: "mnemond/presentation"},
	"mnemonhub/exchange":   {owner: ownerMnemonhub, role: "mnemonhub event exchange client, cursors, and local ledger acknowledgements", target: "mnemonhub/exchange"},
}

var retiredTopLevelImplementationPackages = []string{
	"hostsurface",
	"render",
	"remotesync",
}

func TestInternalPackagesHaveMainAxisOwner(t *testing.T) {
	for _, pkg := range topLevelInternalPackages(t) {
		inv, ok := packageMainAxisInventory[pkg]
		if !ok {
			t.Errorf("harness/internal/%s has no main-axis owner; classify it under hostagent, mnemond, mnemonhub, or event before adding a new top-level concept", pkg)
			continue
		}
		if inv.owner == "" || strings.TrimSpace(inv.role) == "" || strings.TrimSpace(inv.target) == "" {
			t.Errorf("harness/internal/%s has incomplete main-axis inventory: %+v", pkg, inv)
		}
	}
	for pkg := range packageMainAxisInventory {
		if !hasNonTestGoFiles(filepath.Join("..", pkg)) {
			t.Errorf("main-axis inventory names missing/non-source package %q; remove stale classifications instead of preserving dead concepts", pkg)
		}
	}
}

func TestDemotedPackagesDoNotRemainUnownedConcepts(t *testing.T) {
	for pkg := range demotedMainAxisPackages {
		inv, ok := packageMainAxisInventory[pkg]
		if !ok {
			t.Fatalf("demoted package %q is missing from the main-axis inventory", pkg)
		}
		if inv.owner == ownerGuard {
			t.Fatalf("demoted package %q must be assigned to a runtime axis, not guard", pkg)
		}
		if inv.target == pkg || !strings.Contains(inv.target, "/") {
			t.Fatalf("demoted package %q target %q must name its owning axis/subsystem", pkg, inv.target)
		}
	}
}

func TestNestedPackagesHaveMainAxisOwner(t *testing.T) {
	for pkg, inv := range nestedMainAxisInventory {
		if inv.owner == "" || strings.TrimSpace(inv.role) == "" || strings.TrimSpace(inv.target) == "" {
			t.Errorf("harness/internal/%s has incomplete nested main-axis inventory: %+v", pkg, inv)
		}
		if !hasNonTestGoFiles(filepath.Join("..", filepath.FromSlash(pkg))) {
			t.Errorf("nested main-axis inventory names missing/non-source package %q", pkg)
		}
	}
}

func TestRetiredTopLevelImplementationPackagesStayRetired(t *testing.T) {
	for _, pkg := range retiredTopLevelImplementationPackages {
		if hasNonTestGoFiles(filepath.Join("..", pkg)) {
			t.Errorf("harness/internal/%s still has non-test source; keep it under its main-axis owner instead of reviving a top-level concept", pkg)
		}
	}
}

func topLevelInternalPackages(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("..")
	if err != nil {
		t.Fatalf("read harness/internal: %v", err)
	}
	var pkgs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
			continue
		}
		if hasNonTestGoFiles(filepath.Join("..", name)) {
			pkgs = append(pkgs, name)
		}
	}
	return pkgs
}

func hasNonTestGoFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			return true
		}
	}
	return false
}
