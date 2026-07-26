package node

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestActivatePublishesOnlyVerifiedInstallationAndReplaysExactly(t *testing.T) {
	t.Parallel()
	workspace := newProvisionWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(provisioned.NodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, provisioned.NodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	at := provisioned.Profile.UpdatedAt().Add(time.Second)
	options := ActivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision: bundle.Manifest().AssetRevision, ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:   controllerTestClock{at},
		Install: testInstallationVerifier(workspace, provisioned.NodeState, bundle)}
	first, err := Activate(context.Background(), options)
	if err != nil || !first.Changed || !first.Profile.Enabled() || first.Profile.Host() != model.HostCodex ||
		first.Node.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
		t.Fatalf("first Activate() = (%#v, %v)", first, err)
	}
	options.Clock = controllerTestClock{at.Add(time.Hour)}
	options.ExpectedUpdatedAt = first.Profile.UpdatedAt()
	second, err := Activate(context.Background(), options)
	if err != nil || second.Changed || !second.Profile.UpdatedAt().Equal(first.Profile.UpdatedAt()) ||
		!second.Node.UpdatedAt().Equal(first.Node.UpdatedAt()) {
		t.Fatalf("replayed Activate() = (%#v, %v)", second, err)
	}
}

func TestActivateRecoversDeadHandlingsAfterVerifiedSelfCheck(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	databasePath := filepath.Join(provisioned.NodeState, "node.db")
	seedDeadActivationHandling(t, databasePath, provisioned.Node, provisioned.Profile)

	activated, err := Activate(context.Background(),
		activeTestOptions(workspace, provisioned, bundle, model.HostCodex))
	if err != nil || !activated.Changed || !activated.Profile.Enabled() {
		t.Fatalf("Activate() = (%#v, %v)", activated, err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var status, disposition string
	var attempts, recovery uint32
	var deadAt *string
	if err := db.QueryRow(`SELECT status,attempts,recovery_count,last_disposition,dead_at
		FROM agent_handlings WHERE handling_id='handling-activate-dead'`).Scan(
		&status, &attempts, &recovery, &disposition, &deadAt); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 0 || recovery != 1 ||
		disposition != "setup_recovered" || deadAt != nil {
		t.Fatalf("recovered handling = (%q,%d,%d,%q,%v)",
			status, attempts, recovery, disposition, deadAt)
	}
}

func TestActivateRecoveryInvariantLeavesProfileDisabled(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	databasePath := filepath.Join(provisioned.NodeState, "node.db")
	seedDeadActivationHandling(t, databasePath, provisioned.Node, provisioned.Profile)
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DELETE FROM agent_runs WHERE run_id='run-activate-dead'"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Activate(context.Background(),
		activeTestOptions(workspace, provisioned, bundle, model.HostCodex)); !errors.Is(err, ErrActivate) {
		t.Fatalf("Activate() error = %v", err)
	}
	st, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	authority, readErr := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if readErr != nil || closeErr != nil || authority.Profile.Enabled() ||
		!authority.Profile.UpdatedAt().Equal(provisioned.Profile.UpdatedAt()) {
		t.Fatalf("failed recovery changed activation authority = (%#v, %v, close %v)",
			authority, readErr, closeErr)
	}
}

func seedDeadActivationHandling(t *testing.T, databasePath string, node model.Node, profile model.Profile) {
	t.Helper()
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}
	encodedAt := profile.UpdatedAt().Round(0).UTC().Format("2006-01-02T15:04:05.000000000Z")
	recordHash := model.Sum([]byte("activate-dead-member")).Bytes()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,owner_public_key,
		descriptor_json,descriptor_digest,descriptor_signature,member_limit,roster_head_revision,
		roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES('channel-activate-dead','Activation recovery','activate-dead',?,'key','descriptor',
		'descriptor-digest','descriptor-signature',2,1,?,'active','joined',?,?)`,
		node.PeerID().String(), recordHash, encodedAt, encodedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,member_peer_id,
		origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,status,
		signed_record_json,owner_signature,created_at)
		VALUES('channel-activate-dead',1,?,?,?,'local','key','[]','[]','{}','active','{}','sig',?)`,
		recordHash, node.PeerID().String(), node.OriginEpoch().String(), encodedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events(event_id,schema_version,channel_id,origin_peer_id,origin_epoch,
		origin_seq,channel_seq,origin_member_revision,origin_member_record_hash,publication_roster_revision,
		publication_roster_hash,source,actor_principal,event_type,audience_json,resource_json,work_home_peer_id,
		work_id,summary,payload_json,artifact_roots_json,caused_by_json,canonical_event_json,event_digest,
		canonical_publication_json,publication_digest,origin_signature,created_at,accepted_at)
		VALUES('event-activate-dead',1,'channel-activate-dead',?,?,1,1,1,?,1,?,'local',?,
		'review.offered','[]','{}',?,'work-activate-dead','activation recovery','{}','[]','[]','{}',
		'event-digest','{}','publication-digest','signature',?,?)`, node.PeerID().String(),
		node.OriginEpoch().String(), recordHash, recordHash, profile.Principal(), node.PeerID().String(),
		encodedAt, encodedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO agent_handlings(
		handling_id,profile_id,event_id,status,priority,available_at,attempts,last_disposition,
		last_error,recovery_count,dead_at,created_at,updated_at)
		VALUES(?,?,?,'dead',1,?,1,'attempt_budget_exhausted',?,0,?,?,?)`,
		"handling-activate-dead", model.TeamworkProfileID().String(), "event-activate-dead",
		encodedAt, "maximum handling attempts exhausted", encodedAt, encodedAt, encodedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO agent_runs(
		run_id,profile_id,handling_id,cause_json,handling_attempt,handling_recovery,
		claim_fence_hash,lease_until,launcher,runtime_kind,launcher_diagnostic_json,
		runtime_ids_json,status,started_at,finished_at,error)
		VALUES(?,?,?,X'7B7D',1,0,?,?,'external-hook','codex',X'7B7D',X'7B7D',
		'dead',?,?,?)`, "run-activate-dead", model.TeamworkProfileID().String(),
		"handling-activate-dead", bytes.Repeat([]byte{0x42}, 32), encodedAt, encodedAt,
		encodedAt, "maximum handling attempts exhausted"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestActivateRejectsMissingActionAuthorityBeforeStoreOrProfileMutation(t *testing.T) {
	t.Parallel()
	workspace := newProvisionWorkspace(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 8, 30, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := store.OpenExisting(context.Background(), filepath.Join(provisioned.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	called := false
	install := InstallationVerifierFunc(func(context.Context, model.Profile) error {
		called = true
		return nil
	})
	_, activateErr := Activate(context.Background(), ActivateOptions{
		Workspace:         workspace,
		Host:              model.HostCodex,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             controllerTestClock{provisioned.Profile.UpdatedAt().Add(time.Second)},
		Install:           install,
	})
	if !errors.Is(activateErr, ErrActivate) || errors.Is(activateErr, store.ErrWriterActive) {
		t.Fatalf("Activate() error = %v", activateErr)
	}
	if called {
		t.Fatal("action authority failure reached installation verification")
	}
	authority, err := writer.ReadLocalAuthority(context.Background())
	if err != nil || authority.Profile.Enabled() {
		t.Fatalf("failed activation authority = (%#v, %v)", authority, err)
	}
}

func TestActivateFailureLeavesProfileDisabled(t *testing.T) {
	t.Parallel()
	workspace := newProvisionWorkspace(t)
	bundle, _ := assets.Load()
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	failed := errors.New("Host projection is not installed")
	install := testInstallationWithActions(InstallationVerifierFunc(func(context.Context, model.Profile) error {
		return failed
	}), bundle)
	options := ActivateOptions{Workspace: workspace, Host: model.HostCodex,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             controllerTestClock{provisioned.Profile.UpdatedAt().Add(time.Second)},
		Install:           install}
	if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) || !errors.Is(err, failed) {
		t.Fatalf("Activate() error = %v", err)
	}
	st, err := store.Open(context.Background(), filepath.Join(provisioned.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if err != nil || closeErr != nil || authority.Profile.Enabled() {
		t.Fatalf("failed activation authority = (%#v, %v, close %v)", authority, err, closeErr)
	}
}

func TestActivateRejectsIdentityCredentialAndHostDrift(t *testing.T) {
	t.Parallel()
	t.Run("identity", func(t *testing.T) {
		workspace, provisioned, bundle := activeTestProvision(t)
		if err := os.Remove(filepath.Join(provisioned.NodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(provisioned.NodeState); err != nil {
			t.Fatal(err)
		}
		options := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
		if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
			t.Fatalf("Activate() error = %v", err)
		}
	})
	t.Run("credential", func(t *testing.T) {
		workspace, provisioned, bundle := activeTestProvision(t)
		replacement := append([]byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))), '\n')
		credentialPath := filepath.Join(provisioned.NodeState, "profiles", model.TeamworkProfileID().String()+".token")
		if err := os.WriteFile(credentialPath, replacement, 0o600); err != nil {
			t.Fatal(err)
		}
		options := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
		if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
			t.Fatalf("Activate() error = %v", err)
		}
	})
}

func activeTestProvision(t *testing.T) (string, ProvisionResult, assets.Bundle) {
	t.Helper()
	workspace := newProvisionWorkspace(t)
	bundle, _ := assets.Load()
	provisioned, err := Provision(context.Background(), ProvisionOptions{Workspace: workspace,
		Host: model.HostCodex, AssetRevision: bundle.Manifest().AssetRevision,
		Clock: controllerTestClock{time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(provisioned.NodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, provisioned.NodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	return workspace, provisioned, bundle
}

func activeTestOptions(workspace string, provisioned ProvisionResult, bundle assets.Bundle,
	host model.HostKind,
) ActivateOptions {
	return ActivateOptions{Workspace: workspace, Host: host,
		AssetRevision:     bundle.Manifest().AssetRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             controllerTestClock{provisioned.Profile.UpdatedAt().Add(time.Second)},
		Install:           testInstallationVerifier(workspace, provisioned.NodeState, bundle)}
}

func TestActivateRejectsStaleOrInvalidExpectedGenerationBeforeMutation(t *testing.T) {
	t.Parallel()
	workspace, provisioned, bundle := activeTestProvision(t)
	for name, expected := range map[string]time.Time{
		"stale": provisioned.Profile.UpdatedAt().Add(-time.Nanosecond),
		"zero":  {},
	} {
		t.Run(name, func(t *testing.T) {
			options := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
			options.ExpectedUpdatedAt = expected
			if _, err := Activate(context.Background(), options); !errors.Is(err, ErrActivate) {
				t.Fatalf("Activate() error = %v", err)
			}
		})
	}
	equalClock := activeTestOptions(workspace, provisioned, bundle, model.HostCodex)
	equalClock.Clock = controllerTestClock{provisioned.Profile.UpdatedAt()}
	if _, err := Activate(context.Background(), equalClock); !errors.Is(err, ErrActivate) {
		t.Fatalf("equal-clock Activate() error = %v", err)
	}
	st, err := store.Open(context.Background(), filepath.Join(provisioned.NodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	authority, readErr := st.ReadLocalAuthority(context.Background())
	closeErr := st.Close()
	if readErr != nil || closeErr != nil || authority.Profile.Enabled() ||
		!authority.Profile.UpdatedAt().Equal(provisioned.Profile.UpdatedAt()) {
		t.Fatalf("failed generation fences changed authority = (%#v, %v, close %v)",
			authority, readErr, closeErr)
	}
}
