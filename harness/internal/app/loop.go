package app

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

// LoopValidate validates the resolved event package registry through the same fail-closed resolution boot
// uses. R1 host setup no longer projects per-loop assets, so validate reports event packages only:
// standard descriptors plus external packages under .mnemon/loops.
func (h *Harness) LoopValidate() ([]string, error) {
	merged, err := policy.ResolveRegistry(h.root, state.DefaultSchemaGuard().Required)
	if err != nil {
		return nil, err
	}
	standard := policy.StandardRegistry()
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		source := "external"
		if _, ok := standard[name]; ok {
			source = "standard"
		}
		lines = append(lines, fmt.Sprintf("%s event package %s: OK", source, name))
	}
	return lines, nil
}

// EventPackageInfo is the read-only view of a resolved event package — the discoverability answer to "what
// kinds can the agents work with and what does each expect" (P2). It is a projection of the descriptor
// (policy.EventPackage), never the runtime's internal rule state, so this query resolves the project
// registry from disk rather than coupling the kernel to package shapes.
type EventPackageInfo struct {
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	ObservedType string   `json:"observed_type"`
	ProposedType string   `json:"proposed_type"`
	ItemsField   string   `json:"items_field"`
	Required     []string `json:"required"`
	Importable   bool     `json:"importable"`
	Merge        string   `json:"merge,omitempty"`
	Source       string   `json:"source"` // "standard" | "external" (.mnemon/loops package)
}

// LoopEventPackages resolves the project registry (standard descriptors + every external package under
// .mnemon/loops, via the SAME fail-closed boot resolution) and returns one EventPackageInfo per kind,
// sorted by kind. It is a LOCAL read — no running server is contacted; the registry is a disk fact.
func (h *Harness) LoopEventPackages() ([]EventPackageInfo, error) {
	catalog, err := policy.ResolveRegistry(h.root, state.DefaultSchemaGuard().Required)
	if err != nil {
		return nil, err
	}
	standard := policy.StandardRegistry()
	infos := make([]EventPackageInfo, 0, len(catalog))
	for _, cap := range catalog {
		source := "external"
		if _, ok := standard[cap.Name]; ok {
			source = "standard"
		}
		infos = append(infos, EventPackageInfo{
			Name:         cap.Name,
			Kind:         string(cap.ResourceKind),
			ObservedType: cap.ObservedType,
			ProposedType: cap.ProposedType,
			ItemsField:   cap.ItemsField,
			Required:     cap.RequiredHeader,
			Importable:   cap.Sync.Importable,
			Merge:        cap.Sync.Merge,
			Source:       source,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Kind < infos[j].Kind })
	return infos, nil
}

// LoopSchema returns the EventPackageInfo for one resource kind (the `control schema --type T` answer),
// resolved from the same project catalog. An unknown kind is an error (fail-closed — never an empty
// success that reads as "no required fields").
func (h *Harness) LoopSchema(kind string) (EventPackageInfo, error) {
	infos, err := h.LoopEventPackages()
	if err != nil {
		return EventPackageInfo{}, err
	}
	for _, info := range infos {
		if info.Kind == kind {
			return info, nil
		}
	}
	return EventPackageInfo{}, fmt.Errorf("unknown event package kind %q (run `mnemon-harness loop packages` to list)", kind)
}

// observeSkillJudgment is the HAND-WRITTEN half of the mnemon-observe skill (decision F): the
// when/why a HostAgent records an observation, the part no spec can render. The mechanism half (which
// kinds exist, how to submit) is generated from the catalog by RenderObserveSkill.
const observeSkillJudgment = `# mnemon-observe

Record a governed observation when you learn a concrete, durable fact worth keeping. The platform
admits or denies each observation through its rules and leaves a durable diagnostic either way — you
never write a resource directly, and a denied observation is a signal, not a failure.

## When to record (judgment — yours to apply)

- Record a specific, reusable fact or decision — something a future session would benefit
  from. Prefer the concrete over the vague ("the deploy step needs FOO=1" beats "deploys are tricky").
- One observation per distinct fact; do not batch unrelated facts into one.
- Never record secrets, credentials, tokens, or transient state — the safety rules will deny them,
  and the denial is durable.
- If you are unsure a fact is durable, it probably is not. Skip it.
`

const observeSkillRead = `## How to read governed context

Use the current binding environment when it is available:

    . .mnemon/harness/local/env.sh

Then read the scoped view or a rendered context packet:

    mnemon-harness control pull \
      --addr "$MNEMON_CONTROL_ADDR" \
      --principal "$MNEMON_CONTROL_PRINCIPAL" \
      --token-file "$MNEMON_CONTROL_TOKEN_FILE"

    mnemon-harness control render \
      --addr "$MNEMON_CONTROL_ADDR" \
      --principal "$MNEMON_CONTROL_PRINCIPAL" \
      --token-file "$MNEMON_CONTROL_TOKEN_FILE" \
      --intent context.packet
`

// observeSkillSubmit is the static submit/discovery footer (mechanism that does not vary by kind).
const observeSkillSubmit = `## How to submit

    mnemon-harness control observe \
      --type <kind>.write_candidate.observed \
      --payload '{ "<field>": "<value>", ... }' \
      --external-id <unique-id>

The exact payload fields for a kind are discoverable — never guess:

    mnemon-harness loop packages             # list every kind you can record
    mnemon-harness loop schema --type <kind>  # one kind's required fields + sync
`

// RenderObserveSkill generates the mnemon-observe skill (decision F: a directory-level generated
// skill). The judgment half is hand-written (observeSkillJudgment); the mechanism half — which kinds
// this project enables and the event type to observe for each — is RENDERED from the resolved
// registry, so the skill never drifts from the live event package set and never hardcodes per-kind fields
// (it points the agent at `loop schema` for those). It is the generic counterpart to per-loop skills:
// one skill teaches recording an observation for ANY kind.
func (h *Harness) RenderObserveSkill() (string, error) {
	infos, err := h.LoopEventPackages()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString(observeSkillJudgment)
	b.WriteString("\n")
	b.WriteString(observeSkillRead)
	b.WriteString("\n## What you can record (generated from this project's catalog)\n\n")
	b.WriteString("| kind | observe this event type | source |\n")
	b.WriteString("|------|-------------------------|--------|\n")
	for _, info := range infos {
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n", info.Kind, info.ObservedType, info.Source))
	}
	b.WriteString("\n")
	b.WriteString(observeSkillSubmit)
	return b.String(), nil
}

// LoopAdd registers an external event package from srcDir into the project's external loop root
// (<root>/.mnemon/loops/<name>). It is the "write a directory -> register it" front door (P2 minimal
// onboarding): the author writes a package dir, `loop add` places it under the canonical name and
// validates it through the SAME fail-closed boot resolution `local run` uses (policy.ResolveRegistry
// — symlink screen + LoadExternal + four-axis shadowing merge). A package that would refuse boot is
// rejected here and the copy is rolled back, so a half-added package never lingers. The canonical name
// is the spec's `name` (the external loader requires the directory name to equal it); an existing
// target is NOT overwritten (remove it first to replace). Returns the registered name.
func (h *Harness) LoopAdd(srcDir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(srcDir, "capability.json"))
	if err != nil {
		return "", fmt.Errorf("read %s/capability.json: %w", srcDir, err)
	}
	var spec struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		return "", fmt.Errorf("parse %s/capability.json: %w", srcDir, err)
	}
	if spec.Name == "" {
		return "", fmt.Errorf("%s/capability.json has no name", srcDir)
	}
	target := filepath.Join(h.root, ".mnemon", "loops", spec.Name)
	srcAbs, _ := filepath.Abs(srcDir)
	tgtAbs, _ := filepath.Abs(target)
	if srcAbs == tgtAbs {
		return "", fmt.Errorf("loop %q is already in place at %s", spec.Name, target)
	}
	if _, err := os.Stat(target); err == nil {
		return "", fmt.Errorf("loop %q already added (%s exists); remove it first to replace", spec.Name, target)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return "", err
	}
	if err := copyTree(srcDir, target); err != nil {
		_ = os.RemoveAll(target)
		return "", fmt.Errorf("copy package: %w", err)
	}
	// Validate through the exact boot resolution; roll the copy back on any refusal so a rejected
	// package never lingers as a half-added, boot-sinking directory.
	if _, err := policy.ResolveRegistry(h.root, state.DefaultSchemaGuard().Required); err != nil {
		_ = os.RemoveAll(target)
		return "", fmt.Errorf("loop %q rejected (fail-closed): %w", spec.Name, err)
	}
	return spec.Name, nil
}

// copyTree copies a package directory tree, rejecting symlinks (fail-closed: the external loader
// screens them anyway, so refuse at copy rather than place a tree that cannot boot). Regular files
// and directories only; file modes are preserved.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("symlink not allowed in a loop package: %s", path)
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(out, info.Mode().Perm()|0o700)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("not a regular file in a loop package: %s", path)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(out, data, info.Mode().Perm())
	})
}
