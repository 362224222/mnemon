package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestJoinedChannelInstallFreshPlanCannotAuthorizeAdvancedRoster(t *testing.T) {
	t.Parallel()
	owner, joinerStore, spec := newJoinedChannelInstallFixture(t,
		"join-plan-stale-roster", "evidence-team")
	fresh, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	baseRoster, err := model.NewVerifiedRoster(spec.Descriptor, spec.Members)
	if err != nil {
		t.Fatal(err)
	}
	additional := testkit.NewIdentity(t, "join-plan-stale-roster-additional")
	_, advancedRoster := appendRosterMemberWithLabel(t, spec.Descriptor, owner.signer,
		baseRoster, additional, additional.DisplayName())
	advanced := spec
	advanced.Members = advancedRoster.Members()
	advanced.At = spec.At.Add(time.Second)
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), advanced); err != nil ||
		result.Roster.Head() != advancedRoster.Head() {
		t.Fatalf("advance joined roster = (%#v,%v)", result, err)
	}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), fresh); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveJoinedChannelInstall(stale fresh plan) = (%q,%v)", resolution, err)
	}
	if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), fresh); !errors.Is(err,
		ErrChannelAuthorityPlanDiverged) {
		t.Fatalf("CommitJoinedChannelInstall(stale fresh plan) error = %v", err)
	}
	current, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil || current.ChangesAuthority() || current.Result().Roster.Head() != advancedRoster.Head() {
		t.Fatalf("PrepareJoinedChannelInstall(current replay) = (%#v,%v)", current.Result(), err)
	}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), current); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveJoinedChannelInstall(current replay) = (%q,%v)", resolution, err)
	}
}

func TestJoinedChannelInstallNoopPlanFailsClosedOnPublicationEvidenceDrift(t *testing.T) {
	t.Parallel()
	_, joinerStore, spec := newJoinedChannelInstallFixture(t,
		"join-plan-publication-drift", "evidence-team")
	initial, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	noop, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil || noop.ChangesAuthority() {
		t.Fatalf("PrepareJoinedChannelInstall(no-op) = (%#v,%v)", noop.Result(), err)
	}
	if _, err := joinerStore.db.Exec(`UPDATE publication_epochs SET updated_at=? WHERE channel_id=?`,
		storeTime(spec.Descriptor.Descriptor().CreatedAt().Add(-time.Second)),
		spec.Descriptor.Descriptor().ID().String()); err != nil {
		t.Fatal(err)
	}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), noop); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveJoinedChannelInstall(evidence drift) = (%q,%v)", resolution, err)
	}
	if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), noop); !errors.Is(err,
		ErrChannelAuthorityPlanDiverged) {
		t.Fatalf("CommitJoinedChannelInstall(evidence drift) error = %v", err)
	}
}

func TestJoinedChannelInstallNoopPlanFailsClosedOnLocalAliasDrift(t *testing.T) {
	t.Parallel()
	_, joinerStore, spec := newJoinedChannelInstallFixture(t,
		"join-plan-alias-drift", "evidence-team")
	initial, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinerStore.CommitJoinedChannelInstall(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	noop, err := joinerStore.PrepareJoinedChannelInstall(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := joinerStore.db.Exec(`UPDATE channels SET local_alias=? WHERE channel_id=?`,
		"drifted-local-alias", spec.Descriptor.Descriptor().ID().String()); err != nil {
		t.Fatal(err)
	}
	if resolution, err := joinerStore.ResolveJoinedChannelInstall(context.Background(), noop); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveJoinedChannelInstall(alias drift) = (%q,%v)", resolution, err)
	}
}
