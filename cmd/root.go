// Package cmd composes the single Mnemon product command.
package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/mnemon-dev/mnemon/cmd/memory"
	"github.com/spf13/cobra"
)

var version = "dev"

// Execute runs one Mnemon command. Process signal handling and exit remain the
// root main package's responsibility.
func Execute(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if ctx == nil || stdin == nil || stdout == nil || stderr == nil {
		return 1
	}
	agencyExit := 0
	root := productRoot(&agencyExit)
	root.SetArgs(args)
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return agencyExit
}

func productRoot(agencyExit *int) *cobra.Command {
	root := memory.New(version)
	root.Short = "Memory and durable agency for LLM agents"
	root.Long = "Mnemon gives LLM agents persistent memory and a local authority for durable, peer-to-peer work."
	// Memory's current command tree is process-global. Remove a prior adapter
	// so focused tests can construct the product root more than once without
	// changing the production command set.
	for _, child := range root.Commands() {
		if child.Name() == "agency" {
			root.RemoveCommand(child)
		}
	}
	addAgencyCommand(root, version, agencyExit)
	return root
}
