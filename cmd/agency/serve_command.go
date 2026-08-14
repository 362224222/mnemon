package agency

import "github.com/spf13/cobra"

func serveCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "serve",
		Short: "Serve one already-provisioned Agency authority",
		Args:  cobra.NoArgs,
		RunE:  runServe,
	}
	command.Flags().Var(new(singleString), "state-dir",
		"already-provisioned Agency state directory")
	return command
}
