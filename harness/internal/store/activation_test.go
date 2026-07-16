package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestActivateProfilePublishesAuthorityAtomicallyAndReplays(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, disabled := bootstrapValues(t, "peer-activate", "principal-activate", "/workspace/activate")
	if _, err := st.InitializeNode(context.Background(), node, disabled); err != nil {
		t.Fatal(err)
	}
	desired := activationProfile(t, disabled, model.HostCodex, model.RuntimeCodexAppServer,
		"asset-r5", disabled.HandlingBudget(), disabled.UpdatedAt().Add(time.Minute))
	at := disabled.UpdatedAt().Add(2 * time.Minute)

	first, err := st.ActivateProfile(context.Background(), desired, disabled.UpdatedAt(), at)
	if err != nil {
		t.Fatalf("ActivateProfile() error = %v", err)
	}
	if !first.Changed || !first.Profile.Enabled() || first.Node.ActiveAssetRevision() != "asset-r5" ||
		first.Node.PeerID() != node.PeerID() || first.Node.NextOriginSequence() != node.NextOriginSequence() ||
		!first.Node.UpdatedAt().Equal(at) || !first.Profile.UpdatedAt().Equal(at) {
		t.Fatalf("ActivateProfile() = %#v", first)
	}

	replaySpec := desired.Spec()
	replaySpec.CreatedAt = replaySpec.CreatedAt.Add(time.Hour)
	replaySpec.UpdatedAt = replaySpec.UpdatedAt.Add(time.Hour)
	replay, err := model.NewProfile(replaySpec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.ActivateProfile(context.Background(), replay, first.Profile.UpdatedAt(), at.Add(time.Hour))
	if err != nil || second.Changed || !second.Profile.UpdatedAt().Equal(at) || !second.Node.UpdatedAt().Equal(at) {
		t.Fatalf("replayed ActivateProfile() = (%#v, %v)", second, err)
	}

	stagedNodeSpec := node.Spec()
	stagedNodeSpec.ActiveAssetRevision = "asset-staged"
	stagedNode, _ := model.NewNode(stagedNodeSpec)
	stagedProfile := activationProfile(t, disabled, model.HostClaudeCode, model.RuntimeClaudeCLI,
		"asset-staged", disabled.HandlingBudget(), disabled.UpdatedAt())
	stagedSpec := stagedProfile.Spec()
	stagedSpec.Enabled = false
	stagedProfile, _ = model.NewProfile(stagedSpec)
	initialized, err := st.InitializeNode(context.Background(), stagedNode, stagedProfile)
	if err != nil || initialized.Profile.Host() != model.HostCodex || !initialized.Profile.Enabled() ||
		initialized.Profile.ActiveAssetRevision() != "asset-r5" {
		t.Fatalf("InitializeNode() downgraded enabled state: (%#v, %v)", initialized, err)
	}
}

func TestActivateProfileHostSwitchAndAuthorityUpgradeRules(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, disabled := bootstrapValues(t, "peer-switch", "principal-switch", "/workspace/switch")
	if _, err := st.InitializeNode(context.Background(), node, disabled); err != nil {
		t.Fatal(err)
	}
	changedBudget := changedHandlingBudget(t)
	claude := activationProfile(t, disabled, model.HostClaudeCode, model.RuntimeClaudeCLI,
		"asset-claude", changedBudget, disabled.UpdatedAt().Add(time.Minute))
	activatedAt := disabled.UpdatedAt().Add(2 * time.Minute)
	result, err := st.ActivateProfile(context.Background(), claude, disabled.UpdatedAt(), activatedAt)
	if err != nil || result.Profile.Host() != model.HostClaudeCode ||
		result.Node.ActiveAssetRevision() != "asset-claude" || !result.Profile.Enabled() {
		t.Fatalf("disabled Host switch = (%#v, %v)", result, err)
	}

	codex := activationProfile(t, result.Profile, model.HostCodex, model.RuntimeCodexAppServer,
		"asset-codex", changedBudget, activatedAt.Add(time.Minute))
	if _, err := st.ActivateProfile(context.Background(), codex, result.Profile.UpdatedAt(), activatedAt.Add(time.Minute)); !errors.Is(err, ErrProfileHostMismatch) {
		t.Fatalf("enabled Host switch error = %v", err)
	}

	upgradedBudget := model.DefaultHandlingBudget().JSON()
	upgrade := activationProfile(t, result.Profile, model.HostClaudeCode, model.RuntimeClaudeCLI,
		"asset-claude-next", upgradedBudget, activatedAt.Add(time.Minute))
	upgraded, err := st.ActivateProfile(context.Background(), upgrade, result.Profile.UpdatedAt(), activatedAt.Add(time.Minute))
	if err != nil || !upgraded.Changed || upgraded.Node.ActiveAssetRevision() != "asset-claude-next" ||
		upgraded.Profile.HandlingBudget().String() != upgradedBudget.String() {
		t.Fatalf("idle authority upgrade = (%#v, %v)", upgraded, err)
	}
}

func TestActivateProfileRejectsIncompleteOrChangedIdentity(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	_, disabled := bootstrapValues(t, "peer-identity", "principal-identity", "/workspace/identity")
	desired := activationProfile(t, disabled, model.HostCodex, model.RuntimeCodexAppServer,
		"asset-r5", disabled.HandlingBudget(), disabled.UpdatedAt().Add(time.Minute))
	if _, err := st.ActivateProfile(context.Background(), desired, disabled.UpdatedAt(), desired.UpdatedAt()); !errors.Is(err, ErrProfileActivationConflict) {
		t.Fatalf("uninitialized activation error = %v", err)
	}

	node, disabled := bootstrapValues(t, "peer-identity", "principal-identity", "/workspace/identity")
	if _, err := st.InitializeNode(context.Background(), node, disabled); err != nil {
		t.Fatal(err)
	}
	driftedSpec := desired.Spec()
	driftedSpec.Principal = "principal-other"
	drifted, err := model.NewProfile(driftedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ActivateProfile(context.Background(), drifted, disabled.UpdatedAt(), desired.UpdatedAt()); !errors.Is(err, ErrProfileActivationConflict) {
		t.Fatalf("identity drift error = %v", err)
	}
	if _, err := st.ActivateProfile(context.Background(), desired,
		disabled.UpdatedAt().Add(-time.Nanosecond), desired.UpdatedAt()); !errors.Is(err, ErrProfileActivationConflict) {
		t.Fatalf("generation drift error = %v", err)
	}
	if _, err := st.ActivateProfile(context.Background(), desired,
		disabled.UpdatedAt(), disabled.UpdatedAt()); !errors.Is(err, ErrProfileActivationConflict) {
		t.Fatalf("equal-time activation error = %v", err)
	}
	if _, err := st.ActivateProfile(context.Background(), desired, disabled.UpdatedAt(), disabled.UpdatedAt().Add(-time.Second)); !errors.Is(err, ErrProfileActivationConflict) {
		t.Fatalf("regressed activation time error = %v", err)
	}
}

func TestActivateProfileBusyAuthorityFailsWithoutPartialUpdate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		id   string
		busy func(*testing.T, *Store, model.Node, model.Profile)
	}{
		{name: "claimed handling", id: "handling", busy: insertActivationClaimedHandling},
		{name: "starting Agent run", id: "run", busy: insertActivationAgentRun},
		{name: "runtime-finished Agent run", id: "runtime-finished", busy: insertActivationRuntimeFinishedRun},
		{name: "started operation", id: "operation", busy: insertActivationStartedOperation},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, disabled := bootstrapValues(t, "peer-busy-"+tc.id, "principal-busy-"+tc.id, "/workspace/busy/"+tc.id)
			node, active := activateTestNode(t, st, node, disabled)
			tc.busy(t, st, node, active)

			exact, err := st.ActivateProfile(context.Background(), active, active.UpdatedAt(), active.UpdatedAt().Add(time.Minute))
			if err != nil || exact.Changed {
				t.Fatalf("exact busy replay = (%#v, %v)", exact, err)
			}
			upgrade := activationProfile(t, active, active.Host(), active.Runtime(),
				"asset-blocked", changedHandlingBudget(t), active.UpdatedAt().Add(time.Minute))
			if _, err := st.ActivateProfile(context.Background(), upgrade, active.UpdatedAt(), upgrade.UpdatedAt()); !errors.Is(err, ErrProfileActivationBusy) {
				t.Fatalf("busy upgrade error = %v", err)
			}
			durableNode, err := readNode(context.Background(), st.db)
			if err != nil || durableNode.ActiveAssetRevision() != "asset-r5" {
				t.Fatalf("busy failure partially updated Node: (%#v, %v)", durableNode, err)
			}
		})
	}
}

func TestActivateProfileDisabledButBusyRemainsDisabled(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		id   string
		busy func(*testing.T, *Store, model.Node, model.Profile)
	}{
		{name: "claimed handling", id: "disabled-handling", busy: insertActivationClaimedHandling},
		{name: "starting Agent", id: "disabled-run", busy: insertActivationAgentRun},
		{name: "runtime-finished Agent", id: "disabled-runtime-finished", busy: insertActivationRuntimeFinishedRun},
		{name: "started operation", id: "disabled-operation", busy: insertActivationStartedOperation},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, disabled := bootstrapValues(t, "peer-"+tc.id, "principal-"+tc.id, "/workspace/"+tc.id)
			if _, err := st.InitializeNode(context.Background(), node, disabled); err != nil {
				t.Fatal(err)
			}
			tc.busy(t, st, node, disabled)
			desired := activationProfile(t, disabled, disabled.Host(), disabled.Runtime(),
				disabled.ActiveAssetRevision(), disabled.HandlingBudget(), disabled.UpdatedAt().Add(time.Minute))
			if _, err := st.ActivateProfile(context.Background(), desired, disabled.UpdatedAt(), desired.UpdatedAt()); !errors.Is(err, ErrProfileActivationBusy) {
				t.Fatalf("disabled busy activation error = %v", err)
			}
			var enabled int
			var asset string
			if err := st.db.QueryRow("SELECT enabled, active_asset_rev FROM profiles WHERE profile_id = ?",
				disabled.ID().String()).Scan(&enabled, &asset); err != nil {
				t.Fatal(err)
			}
			if enabled != 0 || asset != disabled.ActiveAssetRevision() {
				t.Fatalf("failed activation changed Profile: enabled=%d asset=%q", enabled, asset)
			}
		})
	}
}

func activationProfile(t *testing.T, base model.Profile, host model.HostKind, runtime model.RuntimeKind,
	asset string, budget model.JSON, updated time.Time,
) model.Profile {
	t.Helper()
	spec := base.Spec()
	spec.Host, spec.Runtime = host, runtime
	spec.ActiveAssetRevision = asset
	spec.HandlingBudget = budget
	spec.Enabled = true
	spec.UpdatedAt = updated
	profile, err := model.NewProfile(spec)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func changedHandlingBudget(t *testing.T) model.JSON {
	t.Helper()
	spec := model.DefaultHandlingBudget().Spec()
	spec.MaxAttempts++
	budget, err := model.NewHandlingBudget(spec)
	if err != nil {
		t.Fatal(err)
	}
	return budget.JSON()
}

func insertActivationAgentRun(t *testing.T, st *Store, _ model.Node, profile model.Profile) {
	t.Helper()
	_, err := st.db.Exec(`INSERT INTO agent_runs(run_id, profile_id, cause_json, launcher, runtime_kind,
		launcher_diagnostic_json, runtime_ids_json, status, started_at)
		VALUES('run-activation', ?, '{}', 'test', ?, '{}', '{}', 'starting', ?)`,
		profile.ID().String(), string(profile.Runtime()), storeTime(profile.UpdatedAt()))
	if err != nil {
		t.Fatal(err)
	}
}

func insertActivationRuntimeFinishedRun(t *testing.T, st *Store, _ model.Node, profile model.Profile) {
	t.Helper()
	insertActivationRunWithStatus(t, st, profile, "run-activation-runtime-finished", "runtime_finished")
}

func insertActivationStartedOperation(t *testing.T, st *Store, _ model.Node, profile model.Profile) {
	t.Helper()
	runID := "run-activation-operation"
	insertActivationRunWithStatus(t, st, profile, runID, "outcome_accepted")
	now := profile.UpdatedAt()
	_, err := st.db.Exec(`INSERT INTO operations(operation_id,profile_id,agent_run_id,client_key_hash,
		kind,request_digest,status,lease_owner,lease_until,created_at)
		VALUES('operation-activation',?,?,?,'teamwork.offer',?,'started','owner-activation',?,?)`,
		profile.ID().String(), runID, model.Sum([]byte("activation-operation-key")).Bytes(),
		model.Sum([]byte("activation-operation-request")).Bytes(), storeTime(now.Add(time.Hour)), storeTime(now))
	if err != nil {
		t.Fatal(err)
	}
}

func insertActivationRunWithStatus(t *testing.T, st *Store, profile model.Profile, runID, status string) {
	t.Helper()
	finishedAt := any(nil)
	if status != "starting" && status != "running" {
		finishedAt = storeTime(profile.UpdatedAt())
	}
	_, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at,finished_at)
		VALUES(?,?,'{}','test',?,'{}','{}',?,?,?)`, runID, profile.ID().String(), string(profile.Runtime()),
		status, storeTime(profile.UpdatedAt()), finishedAt)
	if err != nil {
		t.Fatal(err)
	}
}

func insertActivationClaimedHandling(t *testing.T, st *Store, node model.Node, profile model.Profile) {
	t.Helper()
	now := storeTime(profile.UpdatedAt())
	recordHash := model.Sum([]byte("activation-member")).Bytes()
	if _, err := st.db.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,owner_public_key,
		member_limit,roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES('channel-activation','Activation','activation',?,'key',2,1,?,'active','joined',?,?)`,
		node.PeerID().String(), recordHash, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,member_peer_id,
		origin_epoch,display_label,public_key,multiaddrs_json,status,signed_record_json,owner_signature,created_at)
		VALUES('channel-activation',1,?,?,?,'local','key','[]','active','{}','sig',?)`,
		recordHash, node.PeerID().String(), node.OriginEpoch().String(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO events(event_id,schema_version,channel_id,origin_peer_id,origin_epoch,
		origin_seq,channel_seq,origin_member_revision,origin_member_record_hash,publication_roster_revision,
		publication_roster_hash,source,actor_principal,event_type,audience_json,resource_json,work_home_peer_id,
		work_id,summary,payload_json,artifact_roots_json,caused_by_json,canonical_event_json,event_digest,
		canonical_publication_json,publication_digest,origin_signature,created_at,accepted_at)
		VALUES('event-activation',1,'channel-activation',?,?,1,1,1,?,1,?,'local',?,'review.offered',
		'[]','{}',?,'work-activation','activation','{}','[]','[]','{}','event-digest','{}',
		'publication-digest','signature',?,?)`, node.PeerID().String(), node.OriginEpoch().String(),
		recordHash, recordHash, profile.Principal(), node.PeerID().String(), now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO agent_handlings(handling_id,profile_id,event_id,status,priority,
		available_at,claim_owner,claim_token_hash,lease_until,attempts,created_at,updated_at)
		VALUES('handling-activation',?,'event-activation','claimed',0,?,'owner','token',?,1,?,?)`,
		profile.ID().String(), now, now, now, now); err != nil {
		t.Fatal(err)
	}
}
