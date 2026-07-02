package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "", "push, pull, or status")
	endpoint := flag.String("endpoint", "", "MnemonHub endpoint")
	token := flag.String("token", "", "replica bearer token")
	replicaID := flag.String("replica-id", "", "local replica id")
	decisionID := flag.String("decision-id", "docker-probe", "local decision id")
	content := flag.String("content", "docker probe", "event content")
	allowInsecure := flag.Bool("allow-insecure", false, "allow plaintext non-loopback endpoint for Docker local tests")
	flag.Parse()

	if strings.TrimSpace(*mode) == "" || strings.TrimSpace(*endpoint) == "" || strings.TrimSpace(*token) == "" || strings.TrimSpace(*replicaID) == "" {
		return fmt.Errorf("--mode, --endpoint, --token, and --replica-id are required")
	}
	client, err := access.NewSyncClient(*endpoint, access.SyncClientConfig{Token: *token, AllowInsecure: *allowInsecure})
	if err != nil {
		return err
	}
	if err := waitStatus(client, 30*time.Second); err != nil {
		return err
	}
	switch *mode {
	case "push":
		return push(client, *replicaID, *decisionID, *content)
	case "pull":
		return pull(client, *replicaID)
	case "status":
		return status(client)
	default:
		return fmt.Errorf("unknown --mode %q", *mode)
	}
}

func waitStatus(client *access.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var lastErr error
	for ctx.Err() == nil {
		if _, err := client.SyncStatus(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("hub did not become ready: %w", lastErr)
}

func push(client *access.Client, replicaID, decisionID, content string) error {
	fields := map[string]any{"content": content}
	fieldsJSON, _ := json.Marshal(fields)
	sum := sha256.Sum256(fieldsJSON)
	env, err := contract.SyncedEventEnvelopeFromMaterial(contract.SyncedEventMaterial{
		OriginReplicaID: replicaID,
		LocalDecisionID: decisionID,
		LocalIngestSeq:  1,
		Actor:           contract.ActorID(replicaID + "@docker"),
		ResourceRef:     contract.ResourceRef{Kind: "memory", ID: "project"},
		ResourceVersion: 1,
		FieldsDigest:    hex.EncodeToString(sum[:]),
		Fields:          fields,
		DecidedAt:       time.Now().UTC().Format(time.RFC3339),
		Status:          "pending",
	})
	if err != nil {
		return err
	}
	resp, err := client.SyncPush(contract.SyncPushRequest{ReplicaID: replicaID, BatchID: decisionID, Events: []eventmodel.EventEnvelope{env}})
	if err != nil {
		return err
	}
	if len(resp.Accepted) != 1 || len(resp.Rejected) != 0 || len(resp.Conflicts) != 0 {
		return fmt.Errorf("push mismatch: accepted=%d rejected=%d conflicts=%d %+v", len(resp.Accepted), len(resp.Rejected), len(resp.Conflicts), resp)
	}
	return printJSON(map[string]any{"mode": "push", "accepted": len(resp.Accepted), "next_cursor": resp.NextCursor})
}

func pull(client *access.Client, replicaID string) error {
	resp, err := client.SyncPull(contract.SyncPullRequest{ReplicaID: replicaID})
	if err != nil {
		return err
	}
	if len(resp.Events) < 1 {
		return fmt.Errorf("pull returned no events")
	}
	return printJSON(map[string]any{"mode": "pull", "events": len(resp.Events), "next_cursor": resp.NextCursor})
}

func status(client *access.Client) error {
	resp, err := client.SyncStatus()
	if err != nil {
		return err
	}
	if resp.HubEventsReceived < 1 {
		return fmt.Errorf("status received count = %d, want >= 1", resp.HubEventsReceived)
	}
	return printJSON(map[string]any{"mode": "status", "received": resp.HubEventsReceived, "served": resp.HubEventsServed, "principal": resp.Principal})
}

func printJSON(value any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}
