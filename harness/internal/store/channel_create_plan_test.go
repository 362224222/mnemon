package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestCreateChannelPlanFreezesSecretFreeCandidateAndCommitsExactlyOnce(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "create-authority-plan")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-create-authority-plan")
	secret := model.Sum([]byte("create-authority-plan-secret-canary")).Bytes()
	token := storeTestEnrollmentTokenWithSecret(t, fixture.Descriptor(), fixture.Owner(), grantID,
		secret, fixture.Channel().CreatedAt(), fixture.Channel().MemberLimit()-1)
	spec := CreateChannelSpec{Channel: fixture.Channel(), Genesis: fixture.OwnerMember().Member(),
		Token: token}

	plan, err := st.PrepareCreateChannel(context.Background(), spec)
	if err != nil || plan.Result().Created || !plan.ChangesAuthority() ||
		len(plan.Candidate().Channels()) != 1 {
		t.Fatalf("PrepareCreateChannel() = (%#v,%v)", plan.Result(), err)
	}
	assertEnrollmentTableCounts(t, st, map[string]int{"channels": 0, "channel_members": 0,
		"publication_epochs": 0, "enrollment_grants": 0})
	for _, canary := range []string{string(secret), token.Reveal()} {
		if strings.Contains(fmt.Sprintf("%#v", plan), canary) {
			t.Fatal("opaque create plan retained printable bearer credential")
		}
	}
	if resolution, err := st.ResolveCreateChannel(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanUnchanged {
		t.Fatalf("ResolveCreateChannel(before commit) = (%q,%v)", resolution, err)
	}

	created, err := st.CommitCreateChannel(context.Background(), plan)
	if err != nil || !created.Created || created.GrantID != grantID ||
		created.Channel.ID() != fixture.Channel().ID() {
		t.Fatalf("CommitCreateChannel() = (%#v,%v)", created, err)
	}
	if resolution, err := st.ResolveCreateChannel(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveCreateChannel(after commit) = (%q,%v)", resolution, err)
	}
	replayed, err := st.CommitCreateChannel(context.Background(), plan)
	if err != nil || replayed.Created || replayed.GrantID != grantID {
		t.Fatalf("CommitCreateChannel(replay) = (%#v,%v)", replayed, err)
	}
}

func TestCreateChannelPlanIsStoreBoundAndFailsClosedOnNonFingerprintEvidenceDrift(t *testing.T) {
	t.Parallel()
	prepared := prepareCreateChannelPlanFixture(t, "create-authority-plan-drift")
	other := openTestStore(t)
	insertChannelTestNode(t, other.db, prepared.fixture.Owner(), prepared.fixture.Channel().CreatedAt())
	if _, err := other.CommitCreateChannel(context.Background(), prepared.plan); !errors.Is(err,
		ErrChannelAuthorityPlan) {
		t.Fatalf("cross-Store commit error = %v", err)
	}
	if _, err := prepared.store.CommitCreateChannel(context.Background(), prepared.plan); err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.store.db.Exec(`UPDATE channels SET local_alias=? WHERE channel_id=?`,
		"different-local-alias", prepared.fixture.Channel().ID().String()); err != nil {
		t.Fatal(err)
	}
	if resolution, err := prepared.store.ResolveCreateChannel(context.Background(), prepared.plan); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveCreateChannel(alias drift) = (%q,%v)", resolution, err)
	}
}

func TestCreateChannelNoopPlanFailsClosedWhenExactEvidenceDrifts(t *testing.T) {
	t.Parallel()
	prepared := prepareCreateChannelPlanFixture(t, "create-authority-plan-evidence-drift")
	if _, err := prepared.store.CommitCreateChannel(context.Background(), prepared.plan); err != nil {
		t.Fatal(err)
	}
	plan, err := prepared.store.PrepareCreateChannel(context.Background(), prepared.spec)
	if err != nil || plan.ChangesAuthority() {
		t.Fatalf("PrepareCreateChannel(no-op) = (%#v,%v)", plan.Result(), err)
	}
	if _, err := prepared.store.db.Exec(`UPDATE publication_epochs SET updated_at=? WHERE channel_id=?`,
		storeTime(prepared.fixture.Channel().CreatedAt().Add(-time.Second)),
		prepared.fixture.Channel().ID().String()); err != nil {
		t.Fatal(err)
	}
	if resolution, err := prepared.store.ResolveCreateChannel(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveCreateChannel(evidence drift) = (%q,%v)", resolution, err)
	}
	if _, err := prepared.store.CommitCreateChannel(context.Background(), plan); !errors.Is(err,
		ErrChannelAuthorityPlanDiverged) {
		t.Fatalf("CommitCreateChannel(evidence drift) error = %v", err)
	}
}

func TestCreateChannelPlanResolveSeparatesSourceProgressFromStaleMesh(t *testing.T) {
	t.Parallel()
	prepared := prepareCreateChannelPlanFixture(t, "create-authority-plan-progress")
	if _, err := prepared.store.CommitCreateChannel(context.Background(), prepared.plan); err != nil {
		t.Fatal(err)
	}
	progressedAt := prepared.fixture.Channel().CreatedAt().Add(time.Second)
	if _, err := prepared.store.db.Exec(`UPDATE publication_epochs SET source_head_channel_seq=1,updated_at=?
		WHERE channel_id=?`, storeTime(progressedAt), prepared.fixture.Channel().ID().String()); err != nil {
		t.Fatal(err)
	}
	if resolution, err := prepared.store.ResolveCreateChannel(context.Background(), prepared.plan); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveCreateChannel(non-runtime progress) = (%q,%v)", resolution, err)
	}
	joiner := testkit.NewIdentity(t, "create-authority-plan-progress-joiner")
	requestID := stableEnrollmentRequest(t, prepared.fixture.Channel().ID(), prepared.grantID, joiner)
	acceptedAt := progressedAt.Add(time.Second)
	enrollment, err := prepared.store.PrepareChannelEnrollment(context.Background(), PrepareChannelEnrollmentSpec{
		ChannelID: prepared.fixture.Channel().ID(), GrantID: prepared.grantID, RequestID: requestID,
		AuthenticatedPeerID: joiner.PeerID(), JoinerOriginEpoch: joiner.OriginEpoch(),
		JoinerPublicKey: joiner.PublicKey(), At: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	transcript := enrollmentTestTranscript(t, prepared.fixture.Descriptor(), prepared.grantID, requestID, joiner,
		enrollment.RosterHead, 0x51, 0x52)
	if _, err := prepared.store.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: joiner.PeerID(), Transcript: transcript,
		AdvertisedMultiaddrs: joiner.Multiaddrs(), Proof: enrollmentTestProof(t, prepared.token, transcript),
		Signer: enrollmentTestSigner(t, prepared.fixture.Owner()), At: acceptedAt}); err != nil {
		t.Fatal(err)
	}
	if resolution, err := prepared.store.ResolveCreateChannel(context.Background(), prepared.plan); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveCreateChannel(stale mesh) = (%q,%v)", resolution, err)
	}
	if _, err := prepared.store.CommitCreateChannel(context.Background(), prepared.plan); !errors.Is(err,
		ErrChannelAuthorityPlanDiverged) {
		t.Fatalf("CommitCreateChannel(stale mesh) error = %v", err)
	}
	replay, err := prepared.store.PrepareCreateChannel(context.Background(), prepared.spec)
	if err != nil || replay.ChangesAuthority() {
		t.Fatalf("PrepareCreateChannel(progressed replay) = (%#v,%v)", replay.Result(), err)
	}
	if resolution, err := prepared.store.ResolveCreateChannel(context.Background(), replay); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveCreateChannel(current replay) = (%q,%v)", resolution, err)
	}
}

type createChannelPlanFixture struct {
	store   *Store
	fixture *testkit.SignedChannel
	grantID model.GrantID
	token   model.EnrollmentToken
	spec    CreateChannelSpec
	plan    CreateChannelPlan
}

func prepareCreateChannelPlanFixture(t *testing.T, seed string) createChannelPlanFixture {
	t.Helper()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, seed)
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, err := model.ParseGrantID("grant-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	token := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), grantID,
		seed, fixture.Channel().CreatedAt(), fixture.Channel().MemberLimit()-1)
	spec := CreateChannelSpec{Channel: fixture.Channel(), Genesis: fixture.OwnerMember().Member(),
		Token: token}
	plan, err := st.PrepareCreateChannel(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return createChannelPlanFixture{store: st, fixture: fixture, grantID: grantID,
		token: token, spec: spec, plan: plan}
}
