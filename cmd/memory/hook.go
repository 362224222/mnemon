package memory

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/internal/memory/store"
	"github.com/spf13/cobra"
)

// hookCmd exposes Mnemon's host-lifecycle entry points as a single top-level
// command. Hosts (WorkBuddy, Claude Code, etc.) register one of its
// subcommands as a hook so the agent receives a bounded recall/remember
// reminder at the right moment. The command only emits bounded natural-language
// guidance or a closed stop-hook JSON; it owns no durable state.
var hookCmd = &cobra.Command{
	Use:   "hook",
	Short: "Host lifecycle hook entry points",
	Long:  "Emit Mnemon lifecycle reminders for LLM host hooks (session-start, prompt-submit, session-stop).",
}

var hookSessionStartCmd = &cobra.Command{
	Use:   "session-start",
	Short: "Print session-start guidance for host hook injection",
	RunE:  runHookSessionStart,
}

var hookPromptSubmitCmd = &cobra.Command{
	Use:   "prompt-submit",
	Short: "Print a recall/remember reminder before each prompt",
	RunE:  runHookPromptSubmit,
}

var hookSessionStopCmd = &cobra.Command{
	Use:   "session-stop",
	Short: "Print a remember reminder and stop-hook JSON on session stop",
	RunE:  runHookSessionStop,
}

func init() {
	hookCmd.AddCommand(hookSessionStartCmd, hookPromptSubmitCmd, hookSessionStopCmd)
	rootCmd.AddCommand(hookCmd)
}

// runHookSessionStart mirrors the former `mnemon prime` lifecycle command:
// it reports memory activity and injects the optional guidance document so the
// host can surface it at session start.
func runHookSessionStart(cmd *cobra.Command, args []string) error {
	out := cmd.OutOrStdout()
	var b strings.Builder
	if stats, err := loadStats(); err == nil && stats != nil {
		fmt.Fprintf(&b, "[mnemon] Memory active (%d insights, %d edges).\n", stats.Total, stats.EdgeCount)
	} else {
		b.WriteString("[mnemon] Memory active.\n")
	}
	if guide, ok := readPromptGuide(); ok {
		b.WriteString("\n")
		b.WriteString(guide)
		b.WriteString("\n")
	}
	_, err := fmt.Fprint(out, b.String())
	return err
}

// runHookPromptSubmit prints a one-line reminder prompting the agent to
// evaluate recall before responding and remember after responding.
func runHookPromptSubmit(cmd *cobra.Command, args []string) error {
	_, err := fmt.Fprintln(cmd.OutOrStdout(),
		"[mnemon] Evaluate: recall needed? After responding, evaluate: remember needed?")
	return err
}

// runHookSessionStop emits the closed stop-hook JSON the host uses to decide
// whether to end the session. If the host signals stop_hook_active, the prior
// stop hook already ran, so this invocation stays silent to break the loop.
func runHookSessionStop(cmd *cobra.Command, args []string) error {
	if stopHookAlreadyActive(cmd.InOrStdin()) {
		return nil
	}
	out := cmd.OutOrStdout()
	enc := json.NewEncoder(out)
	return enc.Encode(map[string]string{
		"continue": "false",
		"reason": "[mnemon] Before stopping, evaluate whether this exchange contains durable preferences, " +
			"decisions, insights, facts, or context worth remembering. If yes, run mnemon remember/link; " +
			"if no, state that no memory update is needed, then finish.",
	})
}

// loadStats opens the active store read-only and returns aggregate stats. It
// fails closed: a missing or unavailable store yields (nil, err) and the caller
// degrades to a status-free line rather than erroring a host hook.
func loadStats() (*store.InsightStats, error) {
	db, err := openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return db.GetStats()
}

// readPromptGuide returns the optional guidance document, checking the resolved
// data dir first and falling back to the default data dir.
func readPromptGuide() (string, bool) {
	candidates := []string{
		filepath.Join(dataDir, "prompt", "guide.md"),
		filepath.Join(store.DefaultDataDir(), "prompt", "guide.md"),
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		guide := strings.TrimSpace(string(data))
		if guide != "" {
			return guide, true
		}
	}
	return "", false
}

// stopHookAlreadyActive reports whether the host set stop_hook_active in the
// piped hook payload. It never blocks: an interactive terminal stdin is treated
// as "not active" so a manual invocation cannot hang waiting for EOF.
func stopHookAlreadyActive(r io.Reader) bool {
	if f, ok := r.(*os.File); ok {
		if fi, err := f.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
			return false
		}
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return false
	}
	var payload struct {
		StopHookActive bool `json:"stop_hook_active"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	return payload.StopHookActive
}
