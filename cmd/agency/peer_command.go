package agency

import "github.com/spf13/cobra"

func peerCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "peer",
		Short: "Configure explicit peer exchange",
		Args:  cobra.NoArgs,
		RunE:  showCommandHelp,
	}

	prepare := &cobra.Command{
		Use:   "prepare",
		Short: "Prepare this project's peer identity and addresses",
		Args:  cobra.NoArgs,
		RunE:  runPeerPrepare,
	}
	prepare.Flags().Var(new(singleString), "listen", "local HOST:PORT to listen on")
	prepare.Flags().Var(new(singleString), "advertise", "reachable HOST:PORT advertised to peers")
	prepare.Flags().Var(new(singleString), "project-root", "project root (default: current directory)")

	enroll := &cobra.Command{
		Use:   "enroll",
		Short: "Enroll one peer from its Peer Card on stdin",
		Args:  cobra.NoArgs,
		RunE:  runPeerEnroll,
	}
	enroll.Flags().Var(new(singleString), "alias", "stable local alias for the peer")
	enroll.Flags().Var(new(singleString), "project-root", "project root (default: current directory)")

	command.AddCommand(prepare, enroll)
	return command
}
