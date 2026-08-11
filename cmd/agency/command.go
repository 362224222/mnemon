// Package agency declares the mnemon agency command tree.
//
// Commands compose existing Agency services. Canonical state and admission
// remain owned by internal packages.
package agency

import (
	"errors"

	"github.com/mnemon-dev/mnemon/internal/agency/client"
	"github.com/mnemon-dev/mnemon/internal/daemon"
	"github.com/spf13/cobra"
)

type commandFailure struct {
	code int
	err  error
}

func (failure commandFailure) Error() string {
	if failure.err == nil {
		return ""
	}
	return failure.err.Error()
}

// ExitCode reports the process status carried by an Agency command failure.
// Ordinary Cobra validation errors are intentionally not classified here.
func ExitCode(err error) (int, bool) {
	var failure commandFailure
	if !errors.As(err, &failure) {
		return 0, false
	}
	return failure.code, true
}

// New returns a fresh Agency command tree for the Mnemon product root.
func New(version string) *cobra.Command {
	command := &cobra.Command{
		Use:     "agency",
		Short:   "Manage durable Agent work and peer collaboration",
		Long:    "Mnemon Agency adds durable project-local responsibility and admitted effects to an existing Agent Runtime.",
		Version: version,
		Args:    cobra.NoArgs,
		RunE:    showCommandHelp,
	}
	command.SetVersionTemplate("mnemon agency version {{.Version}}\n")
	command.AddCommand(setupCommand(), peerCommand(), serveCommand())

	// These machine surfaces keep the exact grammar owned by agencyclient.
	for _, name := range []string{"hook", "agent", "artifact"} {
		command.AddCommand(&cobra.Command{
			Use:                name,
			Hidden:             true,
			DisableFlagParsing: true,
			RunE:               runTerminal,
		})
	}
	return command
}

func showCommandHelp(command *cobra.Command, _ []string) error {
	if err := command.Help(); err != nil {
		return commandFailure{code: 1, err: err}
	}
	return nil
}

func runTerminal(command *cobra.Command, args []string) error {
	code := agencyclient.Run(command.Context(), append([]string{command.Name()}, args...),
		command.InOrStdin(), command.OutOrStdout(), command.ErrOrStderr(), daemon.Ensure)
	if code != 0 {
		// agencyclient has already emitted the bounded machine diagnostic.
		return commandFailure{code: code}
	}
	return nil
}
