package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestInitializeNodeCreatesDisabledIdentityAndReplays(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-local", "principal-local", "/workspace/project")

	first, err := st.InitializeNode(context.Background(), node, profile)
	if err != nil {
		t.Fatalf("InitializeNode() error = %v", err)
	}
	if !first.Created || first.Profile.Enabled() || first.Node.PeerID() != node.PeerID() {
		t.Fatalf("first InitializeNode() = %#v", first)
	}

	nodeSpec := node.Spec()
	nodeSpec.NextOriginSequence = 99
	nodeSpec.CreatedAt = nodeSpec.CreatedAt.Add(time.Hour)
	nodeSpec.UpdatedAt = nodeSpec.UpdatedAt.Add(time.Hour)
	nodeSpec.ActiveAssetRevision = "asset-staged"
	replayedNode, err := model.NewNode(nodeSpec)
	if err != nil {
		t.Fatal(err)
	}
	profileSpec := profile.Spec()
	profileSpec.ActiveAssetRevision = "asset-staged"
	profileSpec.CreatedAt = profileSpec.CreatedAt.Add(time.Hour)
	profileSpec.UpdatedAt = profileSpec.UpdatedAt.Add(time.Hour)
	replayedProfile, err := model.NewProfile(profileSpec)
	if err != nil {
		t.Fatal(err)
	}

	second, err := st.InitializeNode(context.Background(), replayedNode, replayedProfile)
	if err != nil {
		t.Fatalf("replayed InitializeNode() error = %v", err)
	}
	if second.Created || second.Node.NextOriginSequence() != 1 || second.Node.ActiveAssetRevision() != "asset-r5" ||
		second.Profile.Host() != model.HostCodex || second.Profile.Enabled() {
		t.Fatalf("replayed InitializeNode() changed durable authority: %#v", second)
	}
}

func TestInitializeNodeFailsClosedAndRollsBack(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-local", "principal-local", "/workspace/project")

	enabledSpec := profile.Spec()
	enabledSpec.Enabled = true
	enabled, err := model.NewProfile(enabledSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), node, enabled); !errors.Is(err, ErrInitializationConflict) {
		t.Fatalf("enabled initialization error = %v", err)
	}
	driftedSpec := profile.Spec()
	driftedSpec.ActiveAssetRevision = "asset-other"
	drifted, err := model.NewProfile(driftedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), node, drifted); !errors.Is(err, ErrInitializationConflict) {
		t.Fatalf("asset drift error = %v", err)
	}
	var count int
	if err := st.db.QueryRow("SELECT COUNT(*) FROM node").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed initialization left Node rows: count=%d err=%v", count, err)
	}

	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	otherNode, _ := bootstrapValues(t, "peer-other", "principal-local", "/workspace/project")
	if _, err := st.InitializeNode(context.Background(), otherNode, profile); !errors.Is(err, ErrInitializationConflict) {
		t.Fatalf("Node identity drift error = %v", err)
	}
	otherProfileSpec := profile.Spec()
	otherProfileSpec.WorkspaceRoot = "/workspace/other"
	otherProfile, err := model.NewProfile(otherProfileSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), node, otherProfile); !errors.Is(err, ErrInitializationConflict) {
		t.Fatalf("Profile identity drift error = %v", err)
	}
}

func TestInitializeNodeSelfCheckFailureRemainsDisabledAcrossRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	node, profile := bootstrapValues(t, "peer-restart", "principal-restart", "/workspace/restart")
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	// A projection/self-check failure returns without calling ActivateProfile.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	result, err := st.InitializeNode(context.Background(), node, profile)
	if err != nil || result.Created || result.Profile.Enabled() {
		t.Fatalf("restart InitializeNode() = (%#v, %v)", result, err)
	}
}

func TestInitializeNodeRejectsNonCanonicalDurableEncoding(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		id     string
		tamper func(*testing.T, *Store, model.Profile)
	}{
		{name: "time offset", id: "time", tamper: func(t *testing.T, st *Store, _ model.Profile) {
			if _, err := st.db.Exec("UPDATE node SET created_at = '2026-07-16T20:00:00.000000123+08:00'"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "JSON whitespace", id: "json", tamper: func(t *testing.T, st *Store, profile model.Profile) {
			encoded := append([]byte{' '}, profile.HandlingBudget().Bytes()...)
			if _, err := st.db.Exec("UPDATE profiles SET handling_budget_json = ?", encoded); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, profile := bootstrapValues(t, "peer-tamper-"+tc.id, "principal-tamper-"+tc.id,
				"/workspace/tamper/"+tc.id)
			if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
				t.Fatal(err)
			}
			tc.tamper(t, st, profile)
			if _, err := st.InitializeNode(context.Background(), node, profile); err == nil {
				t.Fatal("InitializeNode() accepted non-canonical durable encoding")
			}
		})
	}
}

func bootstrapValues(t *testing.T, peerText, principal, workspace string) (model.Node, model.Profile) {
	t.Helper()
	peer, err := model.ParsePeerID(peerText)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := model.ParseOriginEpoch("epoch-" + peerText)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 12, 0, 0, 123, time.UTC)
	node, err := model.NewNode(model.NodeSpec{PeerID: peer, OriginEpoch: epoch, NextOriginSequence: 1,
		ActiveAssetRevision: "asset-r5", CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(), Principal: principal,
		WorkspaceRoot: workspace, Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash: model.Sum([]byte("credential-" + peerText)), ActiveAssetRevision: "asset-r5",
		HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: false, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return node, profile
}

func activateTestNode(t *testing.T, st *Store, node model.Node, profile model.Profile) (model.Node, model.Profile) {
	t.Helper()
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	spec := profile.Spec()
	spec.Enabled = true
	spec.UpdatedAt = profile.UpdatedAt().Add(time.Second)
	desired, err := model.NewProfile(spec)
	if err != nil {
		t.Fatal(err)
	}
	result, err := st.ActivateProfile(context.Background(), desired, profile.UpdatedAt(), spec.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	return result.Node, result.Profile
}
