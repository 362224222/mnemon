package cmd

import (
	"errors"

	"github.com/spf13/cobra"
)

// updateCommand explains why this build cannot replace itself.
//
// Self-update only exists for an npm-managed installation: the npm launcher
// intercepts `update` before the native binary runs and delegates to
// npm/cli/lib/update.js. Anything else — Homebrew, `go install`, a source
// build, or this fork's copied binary — reaches this stub, so it fails closed
// rather than pretending to succeed.
//
// The message deliberately does not recommend installing the npm package.
// `@mnemon-dev/mnemon` is upstream's, and this fork is not published there;
// installing it would swap in a binary that ignores embed.yml and therefore
// cannot reach the configured embedding endpoint, which silently disables
// vector memory.
func updateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "update",
		Short: "Update the CLI binary (npm installations only)",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("this build is not managed by npm, so it cannot replace itself\n" +
				"  do NOT run: npm install --global @mnemon-dev/mnemon@latest\n" +
				"    that package is upstream's, not this fork; it would replace this binary with\n" +
				"    one that ignores embed.yml, leaving embeddings unavailable\n" +
				"  update this fork instead: git fetch upstream && git merge upstream/master,\n" +
				"    then rebuild and redeploy the binary\n" +
				"  note: this command updates the CLI, not memory; to refresh vectors run\n" +
				"    mnemon embed --all")
		},
	}
}
