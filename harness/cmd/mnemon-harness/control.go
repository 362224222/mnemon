package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/spf13/cobra"
)

// The control verbs are the host/control agent's view of the channel (D6): observe pushes an
// observation IN, pull reads the scoped event view OUT, status checks reachability. They reach
// the engine ONLY through channel.ServerAPI (the channel client), never kernel/reconcile — the
// same channel a HostAgent and a ControlAgent both speak, differing only by binding/credential.

var (
	controlAddr            string
	controlPrincipal       string
	controlToken           string
	controlType            string
	controlPayload         string
	controlExtID           string
	controlActor           string
	controlTokenFile       string
	controlPullJSON        bool
	controlStatusJSON      bool
	controlRenderIntent    string
	controlRenderLifecycle string
	controlRenderSurface   string
	controlRenderMaxChars  int
	controlRenderJSON      bool
)

// controlClient builds the channel client from the resolved credential: a bearer token (from
// --token or, preferring it, --token-file so projected hooks keep the token out of prompt-visible
// command lines), else the trusted principal header.
func controlClient() (*channel.Client, error) {
	token := controlToken
	if controlTokenFile != "" {
		data, err := os.ReadFile(controlTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read --token-file: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	if token != "" {
		return channel.NewClientWithToken(controlAddr, token), nil
	}
	return channel.NewClient(controlAddr, contract.ActorID(controlPrincipal)), nil
}

var controlCmd = &cobra.Command{
	Use:    "control",
	Short:  "Channel client verbs (observe / pull / status) over a running Local Mnemon service",
	Hidden: true,
}

var controlObserveCmd = &cobra.Command{
	Use:   "observe",
	Short: "Push an observation into the channel (ServerAPI.Ingest)",
	RunE: func(cmd *cobra.Command, args []string) error {
		var payload map[string]any
		if strings.TrimSpace(controlPayload) != "" {
			if err := json.Unmarshal([]byte(controlPayload), &payload); err != nil {
				return fmt.Errorf("decode --payload: %w", err)
			}
		}
		client, err := controlClient()
		if err != nil {
			return err
		}
		rec, err := client.IngestObserve(contract.ActorID(controlPrincipal), contract.ObservationEnvelope{
			ExternalID: controlExtID,
			Event:      contract.Event{Type: controlType, Payload: payload},
		})
		if err != nil {
			return fmt.Errorf("channel observe failed (service unreachable or rejected): %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "observed seq=%d dup=%v ticked=%v\n", rec.Seq, rec.Dup, rec.Ticked)
		if rec.ProcessingError != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "processing error: %s\n", rec.ProcessingError)
		}
		return nil
	},
}

var controlPullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Pull the principal's scoped event view (ServerAPI.PullEventView)",
	RunE: func(cmd *cobra.Command, args []string) error {
		actor := controlActor
		if actor == "" {
			actor = controlPrincipal
		}
		client, err := controlClient()
		if err != nil {
			return err
		}
		proj, err := client.PullEventView(contract.ActorID(controlPrincipal), contract.Subscription{Actor: contract.ActorID(actor)})
		if err != nil {
			return fmt.Errorf("channel pull failed (service unreachable or unauthorized): %w", err)
		}
		if controlPullJSON {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(proj)
		}
		// Count WRITTEN event subjects (version > 0), not every scoped ref: a host's scope now includes the
		// default-enabled coordination kinds (P3b), so an unwritten coordination ref must not inflate
		// the status line. proj.Resources lists the full scope; the written ones carry a version.
		written := 0
		for _, r := range proj.Resources {
			if r.Version > 0 {
				written++
			}
		}
		fmt.Fprintf(cmd.OutOrStdout(), "event-view ref=%s digest=%s event_subjects=%d\n", proj.Ref, proj.Digest, written)
		return nil
	},
}

var controlStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Report channel status evidence for the principal (digest, actor kind, store ref, mode)",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := controlClient()
		if err != nil {
			return err
		}
		st, err := client.Status(contract.ActorID(controlPrincipal))
		if err != nil {
			return fmt.Errorf("channel unreachable or unauthorized: %w", err)
		}
		if controlStatusJSON {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(st)
		}
		// No Remote Workspace line here: channel status has no remote data source (no --root,
		// ServerAPI only) — `mnemon-harness status` owns that report.
		fmt.Fprintf(cmd.OutOrStdout(), "Agent Integration: %s\n", st.Principal)
		fmt.Fprintf(cmd.OutOrStdout(), "Local Mnemon: ready (governed_rows=%d, digest=%s)\n", st.Resources, st.Digest)
		fmt.Fprintf(cmd.OutOrStdout(), "Sync: %d pending, %d synced, %d conflicts (local accepted, remote pending)\n", st.SyncPending, st.SyncSynced, st.SyncConflicts)
		// FIELD section (P3d, the minimal Control Tower seed): the coordination entry counts derived
		// client-side from a pull. The runtime stays capability-free, so kind-aware counts live here,
		// over the default-enabled coordination kinds. Best-effort: a principal not bound to pull just
		// omits the line rather than failing the status report. (agents / pending / diagnostics =
		// server-side aggregation, deferred to the P6 Control Tower.)
		fmt.Fprintln(cmd.OutOrStdout(), coordinationFieldLine(client, contract.ActorID(controlPrincipal)))
		return nil
	},
}

var controlRenderCmd = &cobra.Command{
	Use:   "render",
	Short: "Render read-only derived-event presentation for the authenticated principal",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := controlRender(presentation.Request{
			RenderIntent: controlRenderIntent,
			Lifecycle:    controlRenderLifecycle,
			Surface:      controlRenderSurface,
			Budget:       presentation.Budget{MaxChars: controlRenderMaxChars},
		})
		if err != nil {
			return err
		}
		if controlRenderJSON {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(resp)
		}
		switch resp.Status {
		case presentation.StatusOK, presentation.StatusFallback:
			if strings.TrimSpace(resp.Body) != "" {
				fmt.Fprintln(cmd.OutOrStdout(), resp.Body)
			}
		case presentation.StatusEmpty:
			return nil
		case presentation.StatusDenied:
			return fmt.Errorf("render denied for %s", controlPrincipal)
		default:
			return fmt.Errorf("render returned status %q", resp.Status)
		}
		return nil
	},
}

func controlRender(reqBody presentation.Request) (presentation.Response, error) {
	token := controlToken
	if controlTokenFile != "" {
		data, err := os.ReadFile(controlTokenFile)
		if err != nil {
			return presentation.Response{}, fmt.Errorf("read --token-file: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return presentation.Response{}, err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(controlAddr, "/")+"/render", bytes.NewReader(body))
	if err != nil {
		return presentation.Response{}, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else {
		req.Header.Set(channel.PrincipalHeader, controlPrincipal)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return presentation.Response{}, fmt.Errorf("channel render failed (service unreachable): %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return presentation.Response{}, fmt.Errorf("channel render failed: %s: %s", resp.Status, string(b))
	}
	var out presentation.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return presentation.Response{}, err
	}
	return out, nil
}

// coordinationFieldLine renders "Field: <kind>=<n>, …" over the default-enabled coordination kinds,
// counting each kind's entries in the principal's pulled eventview.
func coordinationFieldLine(client *channel.Client, principal contract.ActorID) string {
	proj, err := client.PullEventView(principal, contract.Subscription{Actor: principal})
	if err != nil {
		return "Field: (unavailable)"
	}
	var caps []capability.Capability
	for _, c := range capability.EmbeddedCatalog() {
		if c.DefaultEnabled {
			caps = append(caps, c)
		}
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i].ResourceKind < caps[j].ResourceKind })
	var parts []string
	for _, c := range caps {
		count := 0
		for _, rc := range proj.Content {
			if rc.Ref.Kind == c.ResourceKind {
				if items, ok := rc.Fields[c.ItemsField].([]any); ok {
					count = len(items)
				}
			}
		}
		parts = append(parts, fmt.Sprintf("%s=%d", strings.ReplaceAll(string(c.ResourceKind), "_", " "), count))
	}
	return "Field: " + strings.Join(parts, ", ")
}

func init() {
	for _, c := range []*cobra.Command{controlObserveCmd, controlPullCmd, controlStatusCmd, controlRenderCmd} {
		c.Flags().StringVar(&controlAddr, "addr", "http://127.0.0.1:8787", "server base URL")
		c.Flags().StringVar(&controlPrincipal, "principal", "", "authenticated principal (trusted-header transport)")
		c.Flags().StringVar(&controlToken, "token", "", "bearer token (TokenAuthenticator transport)")
		c.Flags().StringVar(&controlTokenFile, "token-file", "", "read the bearer token from a file (keeps tokens out of prompt-visible command lines)")
	}
	controlObserveCmd.Flags().StringVar(&controlType, "type", "", "observed event type")
	controlObserveCmd.Flags().StringVar(&controlPayload, "payload", "", "observation payload as JSON")
	controlObserveCmd.Flags().StringVar(&controlExtID, "external-id", "", "idempotency external id")
	controlPullCmd.Flags().StringVar(&controlActor, "actor", "", "subscription actor (defaults to principal)")
	controlPullCmd.Flags().BoolVar(&controlPullJSON, "json", false, "emit scoped event view as JSON")
	controlStatusCmd.Flags().BoolVar(&controlStatusJSON, "json", false, "emit channel status as JSON")
	controlRenderCmd.Flags().StringVar(&controlRenderIntent, "intent", presentation.IntentTeamworkEvents, "render intent")
	controlRenderCmd.Flags().StringVar(&controlRenderLifecycle, "lifecycle", "remind", "host lifecycle")
	controlRenderCmd.Flags().StringVar(&controlRenderSurface, "surface", "hook", "host surface")
	controlRenderCmd.Flags().IntVar(&controlRenderMaxChars, "max-chars", 6000, "maximum rendered body chars")
	controlRenderCmd.Flags().BoolVar(&controlRenderJSON, "json", false, "emit full render response as JSON")
	controlCmd.AddCommand(controlObserveCmd, controlPullCmd, controlStatusCmd, controlRenderCmd)
	controlCmd.GroupID = groupSpine
	rootCmd.AddCommand(controlCmd)
}
