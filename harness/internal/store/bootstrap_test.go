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
	profileSpec.Host = model.HostClaudeCode
	profileSpec.Runtime = model.RuntimeClaudeCLI
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

func TestClassifyNodeInitializationDistinguishesFreshAndExistingAuthority(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	state, err := st.ClassifyNodeInitialization(context.Background())
	if err != nil || state != NodeInitializationFresh {
		t.Fatalf("fresh ClassifyNodeInitialization() = (%d, %v)", state, err)
	}
	node, profile := bootstrapValues(t, "peer-classify", "principal-classify", "/workspace/classify")
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	state, err = st.ClassifyNodeInitialization(context.Background())
	if err != nil || state != NodeInitializationExisting {
		t.Fatalf("existing ClassifyNodeInitialization() = (%d, %v)", state, err)
	}
}

func TestClassifyNodeInitializationRejectsPartialAndCorruptAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		tamper func(*testing.T, *Store)
	}{
		{name: "partial", tamper: func(t *testing.T, st *Store) {
			if _, err := st.db.Exec("DROP TRIGGER profiles_no_delete"); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.Exec("DELETE FROM profiles"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "corrupt", tamper: func(t *testing.T, st *Store) {
			if _, err := st.db.Exec("DROP TRIGGER node_identity_immutable"); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.Exec("UPDATE node SET peer_id = 'not a PeerID'"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, profile := bootstrapValues(t, "peer-classify-"+tc.name,
				"principal-classify-"+tc.name, "/workspace/classify/"+tc.name)
			if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
				t.Fatal(err)
			}
			tc.tamper(t, st)
			state, err := st.ClassifyNodeInitialization(context.Background())
			if state != 0 || !errors.Is(err, ErrInitializationConflict) {
				t.Fatalf("ClassifyNodeInitialization() = (%d, %v)", state, err)
			}
		})
	}
}

func TestClassifyNodeInitializationRejectsHiddenAndExtraRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		id     string
		tamper func(*testing.T, *Store, model.Node, model.Profile)
	}{
		{name: "wrong canonical keys", id: "keys", tamper: func(t *testing.T, st *Store, _ model.Node, _ model.Profile) {
			mustBootstrapExec(t, st, "PRAGMA ignore_check_constraints = ON")
			mustBootstrapExec(t, st, "UPDATE node SET singleton=2")
			mustBootstrapExec(t, st, "DROP TRIGGER profiles_identity_immutable")
			mustBootstrapExec(t, st, "UPDATE profiles SET profile_id='hidden-profile'")
		}},
		{name: "extra Node row", id: "node", tamper: func(t *testing.T, st *Store, _ model.Node, _ model.Profile) {
			mustBootstrapExec(t, st, "PRAGMA ignore_check_constraints = ON")
			extra, _ := bootstrapValues(t, "peer-extra-node", "principal-extra-node", "/workspace/extra-node")
			_, err := st.db.Exec(`INSERT INTO node(singleton,peer_id,origin_epoch,next_origin_seq,
				active_asset_rev,created_at,updated_at) VALUES(2,?,?,?,?,?,?)`, extra.PeerID().String(),
				extra.OriginEpoch().String(), extra.NextOriginSequence(), extra.ActiveAssetRevision(),
				storeTime(extra.CreatedAt()), storeTime(extra.UpdatedAt()))
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "extra Profile row", id: "profile", tamper: func(t *testing.T, st *Store, _ model.Node, _ model.Profile) {
			mustBootstrapExec(t, st, "PRAGMA ignore_check_constraints = ON")
			_, extra := bootstrapValues(t, "peer-extra-profile", "principal-extra-profile", "/workspace/extra-profile")
			_, err := st.db.Exec(`INSERT INTO profiles(profile_id,principal,workspace_root,host,
				runtime_kind,credential_hash,active_asset_rev,handling_budget_json,enabled,created_at,updated_at)
				VALUES('hidden-profile',?,?,?,?,?,?,?,?,?,?)`, extra.Principal(), extra.WorkspaceRoot(),
				string(extra.Host()), string(extra.Runtime()), extra.CredentialHash().Bytes(),
				extra.ActiveAssetRevision(), extra.HandlingBudget().Bytes(), 0,
				storeTime(extra.CreatedAt()), storeTime(extra.UpdatedAt()))
			if err != nil {
				t.Fatal(err)
			}
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, profile := bootstrapValues(t, "peer-hidden-"+tc.id,
				"principal-hidden-"+tc.id, "/workspace/hidden/"+tc.id)
			if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
				t.Fatal(err)
			}
			tc.tamper(t, st, node, profile)
			state, err := st.ClassifyNodeInitialization(context.Background())
			if state != 0 || !errors.Is(err, ErrInitializationConflict) {
				t.Fatalf("ClassifyNodeInitialization() = (%d, %v)", state, err)
			}
		})
	}
}

func TestClassifyNodeInitializationPreservesOperationalQueryFailure(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	if _, err := st.db.Exec("ALTER TABLE node RENAME TO unavailable_node"); err != nil {
		t.Fatal(err)
	}
	state, err := st.ClassifyNodeInitialization(context.Background())
	if state != 0 || err == nil || errors.Is(err, ErrInitializationConflict) {
		t.Fatalf("ClassifyNodeInitialization() = (%d, %v)", state, err)
	}
}

func TestClassifyNodeInitializationRejectsInvalidStorageClasses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		query  string
		column string
	}{
		{name: "Node sequence TEXT", query: "UPDATE node SET next_origin_seq='not-an-integer'", column: "Node"},
		{name: "Profile enabled TEXT", query: "UPDATE profiles SET enabled='not-an-integer'", column: "Profile"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, profile := bootstrapValues(t, "peer-storage-"+tc.column,
				"principal-storage-"+tc.column, "/workspace/storage/"+tc.column)
			if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
				t.Fatal(err)
			}
			mustBootstrapExec(t, st, "PRAGMA ignore_check_constraints = ON")
			mustBootstrapExec(t, st, tc.query)
			state, err := st.ClassifyNodeInitialization(context.Background())
			if state != 0 || !errors.Is(err, ErrInitializationConflict) {
				t.Fatalf("ClassifyNodeInitialization() = (%d, %v)", state, err)
			}
		})
	}
}

func mustBootstrapExec(t *testing.T, st *Store, query string) {
	t.Helper()
	if _, err := st.db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

func TestClassifyNodeInitializationPreservesCancellationCause(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	cause := errors.New("bootstrap classification stopped")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	state, err := st.ClassifyNodeInitialization(ctx)
	if state != 0 || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrInitializationConflict) {
		t.Fatalf("ClassifyNodeInitialization() = (%d, %v)", state, err)
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
