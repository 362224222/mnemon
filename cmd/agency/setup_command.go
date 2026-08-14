package agency

import "github.com/spf13/cobra"

const setupRuntimePi = "pi"

func setupCommand() *cobra.Command {
	runtime := &singleString{value: setupRuntimePi}
	projectRoot := new(singleString)
	command := &cobra.Command{
		Use:   "setup",
		Short: "Set up Agency for this project",
		Long:  "Provision project-local Agency state, ensure its daemon, and install the Pi integration.",
		Args:  cobra.NoArgs,
		RunE:  runSetup,
	}
	command.Flags().Var(runtime, "runtime", "Agent Runtime to integrate (pi)")
	command.Flags().Var(projectRoot, "project-root", "project root (default: current directory)")
	return command
}
