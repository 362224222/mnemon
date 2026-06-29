package coreguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type mainAxisOwner string

const (
	ownerEvent             mainAxisOwner = "event"
	ownerHostAgent         mainAxisOwner = "hostagent"
	ownerMnemond           mainAxisOwner = "mnemond"
	ownerMnemonhub         mainAxisOwner = "mnemonhub"
	ownerInteractionPoint  mainAxisOwner = "interaction-point"
	ownerDriveSource       mainAxisOwner = "drive-source"
	ownerProjectionSurface mainAxisOwner = "projection-surface"
	ownerProductConfig     mainAxisOwner = "product-config"
	ownerGuard             mainAxisOwner = "guard"
)

type packageMainAxis struct {
	owner  mainAxisOwner
	role   string
	target string
}

// packageMainAxisInventory is the executable inventory for the main-axis convergence goal. It does
// not claim the current package names are final; it prevents new top-level implementation concepts
// from appearing without an explicit owner under the R2 architecture layers.
var packageMainAxisInventory = map[string]packageMainAxis{
	"activationtrace": {owner: ownerProjectionSurface, role: "runtime activation trace material for external run displays", target: "projection/activationtrace"},
	"app":             {owner: ownerMnemond, role: "daemon wiring and local mnemond boot", target: "mnemond"},
	"assembler":       {owner: ownerMnemond, role: "mnemond policy/runtime assembly", target: "mnemond"},
	"assets":          {owner: ownerHostAgent, role: "hostagent integration and event-policy assets", target: "hostagent/assets"},
	"codexapp":        {owner: ownerHostAgent, role: "Codex hostagent appserver adapter", target: "hostagent/codexapp"},
	"config":          {owner: ownerMnemond, role: "mnemond local configuration", target: "mnemond/config"},
	"contract":        {owner: ownerEvent, role: "event and mnemond boundary DTOs", target: "event/contract"},
	"coreguard":       {owner: ownerGuard, role: "architecture guard tests", target: "coreguard"},
	"daemon":          {owner: ownerMnemond, role: "harness daemon worker supervision for local event node operation", target: "mnemond/daemon"},
	"drive":           {owner: ownerDriveSource, role: "managed local agent drive source and wake loop primitives", target: "drive/managed-local"},
	"driver":          {owner: ownerMnemond, role: "legacy facade for mnemond and adapter helper code during R2 cleanup", target: "mnemond/daemon"},
	"event":           {owner: ownerEvent, role: "canonical event and envelope model", target: "event"},
	"eventstore":      {owner: ownerEvent, role: "event envelope append/read facade", target: "event/store"},
	"hostagent":       {owner: ownerHostAgent, role: "hostagent setup and thin shims", target: "hostagent"},
	"interaction":     {owner: ownerInteractionPoint, role: "external stimulus material separated into rule/narrative/refs", target: "interaction/event-material"},
	"mnemonhub":       {owner: ownerMnemonhub, role: "remote accepted event exchange server and exchange mechanics", target: "mnemonhub"},
	"productconfig":   {owner: ownerProductConfig, role: "harness product configuration for agents, connections, daemon workers, and surfaces", target: "product/config"},
	"projection":      {owner: ownerProjectionSurface, role: "accepted/derived state material prepared for external projection surfaces", target: "projection"},
	"replay":          {owner: ownerMnemond, role: "mnemond determinism verification", target: "mnemond/replay"},
	"runtime":         {owner: ownerMnemond, role: "local mnemond event runtime", target: "mnemond"},
	"ui":              {owner: ownerMnemond, role: "read-only mnemond operator observability", target: "mnemond/observe"},
}

var demotedMainAxisPackages = map[string]bool{}

var nestedMainAxisInventory = map[string]packageMainAxis{
	"mnemond/admission":    {owner: ownerMnemond, role: "event admission rules and materialization driver", target: "mnemond/admission"},
	"mnemond/access":       {owner: ownerMnemond, role: "hostagent, replica, and control access to mnemond", target: "mnemond/access"},
	"mnemond/policy":       {owner: ownerMnemond, role: "event type schema, admission policy, risk, and default enablement", target: "mnemond/policy"},
	"mnemond/presentation": {owner: ownerMnemond, role: "derived event presentation for hostagents", target: "mnemond/presentation"},
	"mnemond/presentation/view": {
		owner:  ownerMnemond,
		role:   "scoped read model for derived event presentation",
		target: "mnemond/presentation/view",
	},
	"mnemond/state":      {owner: ownerMnemond, role: "local event/state store and materialized-state applier", target: "mnemond/state"},
	"mnemonhub/exchange": {owner: ownerMnemonhub, role: "mnemonhub event exchange client, cursors, and local ledger acknowledgements", target: "mnemonhub/exchange"},
	"surface/multica":    {owner: ownerProjectionSurface, role: "Multica surface registry, metadata, and projection ledger primitives", target: "projection/surface/multica"},
}

var retiredTopLevelImplementationPackages = []string{
	"capability",
	"channel",
	"eventview",
	"hostsurface",
	"kernel",
	"reconcile",
	"render",
	"remotesync",
	"rule",
	"store",
}

func TestInternalPackagesHaveMainAxisOwner(t *testing.T) {
	for _, pkg := range topLevelInternalPackages(t) {
		inv, ok := packageMainAxisInventory[pkg]
		if !ok {
			t.Errorf("harness/internal/%s has no main-axis owner; classify it under an accepted R2 layer before adding a new top-level concept", pkg)
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
