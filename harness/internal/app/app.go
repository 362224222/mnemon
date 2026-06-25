// Package app is the small facade used by mnemon-harness product commands.
//
// It keeps setup/status/validate command code out of declaration and host
// presentation-view internals without reintroducing the older lifecycle command model.
package app

// Harness is the facade handle. It carries the project root and constructs inner
// stores per operation, mirroring the original per-command behavior.
type Harness struct {
	root string
}

// New returns a facade bound to the given project root ("." for the cwd).
func New(root string) *Harness {
	if root == "" {
		root = "."
	}
	return &Harness{root: root}
}
