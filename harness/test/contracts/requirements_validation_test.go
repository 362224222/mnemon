package contracts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

func TestR7RegistryRejectsUnknownNullAndUnsortedBindings(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(corecontract.RegistryPath)))
	if err != nil {
		t.Fatal(err)
	}
	contract, err := corecontract.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		edit func(corecontract.Registry) corecontract.Registry
		want string
	}{
		{
			name: "unknown invariant",
			edit: func(registry corecontract.Registry) corecontract.Registry {
				registry.Invariants[0].ID = "P-99"
				return registry
			},
			want: "invariant IDs",
		},
		{
			name: "null invariant oracles",
			edit: func(registry corecontract.Registry) corecontract.Registry {
				registry.Invariants[0].Oracles = nil
				return registry
			},
			want: "no non-null oracle",
		},
		{
			name: "different shared step",
			edit: func(registry corecontract.Registry) corecontract.Registry {
				for index := range registry.Gates {
					if registry.Gates[index].ID == "G-R7-ROOT-ISOLATION" {
						registry.Gates[index].Steps[0].Argv = append(
							registry.Gates[index].Steps[0].Argv, "-run=TestReleaseBoundary")
					}
				}
				return registry
			},
			want: "shared step contract differs",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry, err := corecontract.DecodeRegistry(data)
			if err != nil {
				t.Fatal(err)
			}
			registry = test.edit(registry)
			if err := corecontract.ValidateBindings(root, contract, registry); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestR7RegistryStrictJSONRejectsUnknownFields(t *testing.T) {
	data := []byte(`{"schema_version":1,"invariants":[],"gates":[],"requirements":[]}`)
	if _, err := corecontract.DecodeRegistry(data); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v, want unknown field", err)
	}
}
