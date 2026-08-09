package cmd

import (
	"github.com/mnemon-dev/mnemon/cmd/agency"
	"github.com/spf13/cobra"
)

func addAgencyCommand(root *cobra.Command, buildVersion string, exitCode *int) {
	command := &cobra.Command{
		Use:                "agency",
		Short:              "Manage durable Agent work and peer collaboration",
		DisableFlagParsing: true,
		Run: func(command *cobra.Command, args []string) {
			*exitCode = agency.Run(command.Context(), args, command.InOrStdin(),
				command.OutOrStdout(), command.ErrOrStderr(), buildVersion)
		},
	}
	root.AddCommand(command)
}
