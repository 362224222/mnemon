// Package cmd composes the single Mnemon product command.
package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/mnemon-dev/mnemon/cmd/agency"
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
	root := productRoot()
	if command, _, findErr := root.Find(args); findErr == nil {
		for current := command; current != nil; current = current.Parent() {
			if current.Name() == "agency" {
				root.SilenceErrors = true
				root.SilenceUsage = true
				break
			}
		}
	}
	root.SetArgs(args)
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)
	executed, err := root.ExecuteContextC(ctx)
	if err == nil {
		return 0
	}
	if err.Error() != "" {
		_, _ = fmt.Fprintln(stderr, err)
	}
	if code, ok := agency.ExitCode(err); ok {
		return code
	}
	for command := executed; command != nil; command = command.Parent() {
		if command.Name() == "agency" {
			return 2
		}
	}
	return 1
}

func productRoot() *cobra.Command {
	root := memory.New(version)
	root.Short = "Memory and durable agency for LLM agents"
	root.Long = "Mnemon gives LLM agents persistent memory and a local authority for durable, peer-to-peer work."
	root.SilenceErrors = false
	root.SilenceUsage = false
	// Memory's current command tree is process-global. Remove a prior command
	// so focused tests can construct the product root more than once without
	// changing the production command set.
	for _, child := range root.Commands() {
		if child.Name() == "agency" {
			root.RemoveCommand(child)
		}
	}
	root.AddCommand(agency.New(version))
	return root
}
