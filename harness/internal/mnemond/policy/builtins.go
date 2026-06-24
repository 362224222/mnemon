package policy

import (
	"fmt"
	"io/fs"
	"path"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

// embeddedCatalog is the embedded event-policy catalog, compiled from the embedded specs
// (assets/capabilities/*.json) by the same FromSpec machinery as external packages. Embedded specs
// are compile-time artifacts: a corrupt one is a build defect, caught by the load test and the gates
// before merge, hence the panic at package init rather than an error path. External packages
// (LoadExternal/ResolveCatalog) take the error path.
var embeddedCatalog = mustLoadBuiltins()

// EmbeddedCatalog returns the embedded event-policy catalog: the embedded half of ResolveCatalog
// and the default when no resolved catalog is supplied.
func EmbeddedCatalog() map[string]Capability { return embeddedCatalog }

func mustLoadBuiltins() map[string]Capability {
	b, err := loadBuiltins(assets.FS)
	if err != nil {
		panic(fmt.Sprintf("embedded capability specs are a build artifact and must compile: %v", err))
	}
	return b
}

// loadBuiltins parses every capabilities/*.json under fsys and compiles it via FromSpec
// (fail-closed). Cross-spec uniqueness is enforced: duplicate names, observed types, or proposed
// types are rejected — two capabilities must never claim the same event family.
func loadBuiltins(fsys fs.FS) (map[string]Capability, error) {
	entries, err := fs.ReadDir(fsys, "capabilities")
	if err != nil {
		return nil, fmt.Errorf("read capabilities dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && path.Ext(e.Name()) == ".json" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := map[string]Capability{}
	reg := newSpecRegistry()
	for _, name := range names {
		raw, err := fs.ReadFile(fsys, path.Join("capabilities", name))
		if err != nil {
			return nil, fmt.Errorf("read capability spec %s: %w", name, err)
		}
		spec, err := decodeSpec(raw)
		if err != nil {
			return nil, fmt.Errorf("parse capability spec %s: %w", name, err)
		}
		cap, err := FromSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("compile capability spec %s: %w", name, err)
		}
		if err := reg.claim("capability spec "+name, cap); err != nil {
			return nil, err
		}
		out[cap.Name] = cap
	}
	return out, nil
}

// specRegistry enforces cross-spec uniqueness on the three event-family axes EVERY loader must
// hold — no two capabilities may claim the same name, observed type, or proposed type. Shared by
// the embedded loader, the external loader, and the catalog merge (which adds the fourth,
// resource-kind axis on top).
type specRegistry struct {
	names    map[string]bool
	observed map[string]string
	proposed map[string]string
}

func newSpecRegistry() *specRegistry {
	return &specRegistry{names: map[string]bool{}, observed: map[string]string{}, proposed: map[string]string{}}
}

func (r *specRegistry) claim(source string, c Capability) error {
	if r.names[c.Name] {
		return fmt.Errorf("%s: duplicate capability name %q", source, c.Name)
	}
	if prev, dup := r.observed[c.ObservedType]; dup {
		return fmt.Errorf("%s: observed type %q already claimed by %q", source, c.ObservedType, prev)
	}
	if prev, dup := r.proposed[c.ProposedType]; dup {
		return fmt.Errorf("%s: proposed type %q already claimed by %q", source, c.ProposedType, prev)
	}
	r.names[c.Name] = true
	r.observed[c.ObservedType], r.proposed[c.ProposedType] = c.Name, c.Name
	return nil
}
