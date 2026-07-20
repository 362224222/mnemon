package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestChannelEnrollmentSigningPlanRollsBackAndFreezesMessagesAndInput(t *testing.T) {
	t.Parallel()
	fixture := newOwnerAcceptancePlanFixture(t, "accept-signing-freeze")
	spec := fixture.signingSpec
	addresses := append([]string(nil), spec.AdvertisedMultiaddrs...)
	spec.AdvertisedMultiaddrs = addresses
	signing, err := fixture.owner.ownerStore.PrepareChannelEnrollmentSigning(context.Background(), spec)
	if err != nil || !signing.RequiresSignatures() {
		t.Fatalf("PrepareChannelEnrollmentSigning() = (%t,%v)", signing.RequiresSignatures(), err)
	}
	memberMessage := signing.MemberSigningMessage()
	receiptMessage := signing.ReceiptSigningMessage()
	if len(memberMessage) == 0 || len(receiptMessage) == 0 {
		t.Fatal("fresh signing plan exposed empty messages")
	}
	memberMessage[0] ^= 0xff
	receiptMessage[0] ^= 0xff
	addresses[0] = "/ip4/127.0.0.1/tcp/49999"
	if bytes.Equal(memberMessage, signing.MemberSigningMessage()) ||
		bytes.Equal(receiptMessage, signing.ReceiptSigningMessage()) {
		t.Fatal("signing message getter leaked mutable plan storage")
	}
	assertEnrollmentTableCounts(t, fixture.owner.ownerStore, map[string]int{
		"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		"peer_bindings": 0})

	signatures := fixture.signatures(t, signing)
	plan, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(),
		signing, signatures)
	if err != nil || !plan.ChangesAuthority() {
		t.Fatalf("PrepareSignedChannelEnrollment() = (%t,%v)", plan.ChangesAuthority(), err)
	}
	for index := range signatures.MemberSignature {
		signatures.MemberSignature[index] ^= 0xff
	}
	for index := range signatures.ReceiptSignature {
		signatures.ReceiptSignature[index] ^= 0xff
	}
	assertEnrollmentTableCounts(t, fixture.owner.ownerStore, map[string]int{
		"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		"peer_bindings": 0})
	other := openTestStore(t)
	if _, err := other.PrepareSignedChannelEnrollment(context.Background(), signing,
		ChannelEnrollmentSignatures{}); !errors.Is(err, ErrChannelAuthorityPlan) {
		t.Fatalf("cross-Store signing plan error = %v", err)
	}
	if _, err := other.CommitChannelEnrollment(context.Background(), plan); !errors.Is(err,
		ErrChannelAuthorityPlan) {
		t.Fatalf("cross-Store authority plan error = %v", err)
	}
	if _, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan); err != nil {
		t.Fatalf("commit after caller signature mutation = %v", err)
	}
}

func TestChannelEnrollmentPlanResolvesFreshResponseLossExactly(t *testing.T) {
	t.Parallel()
	fixture := newOwnerAcceptancePlanFixture(t, "accept-response-loss-plan")
	plan := fixture.finalPlan(t)
	if resolution, err := fixture.owner.ownerStore.ResolveChannelEnrollment(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanUnchanged {
		t.Fatalf("ResolveChannelEnrollment(before) = (%q,%v)", resolution, err)
	}
	result, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan)
	if err != nil || result.Status != ChannelEnrollmentAccepted ||
		result.Roster.Head().Revision() != fixture.owner.head.Revision()+1 {
		t.Fatalf("CommitChannelEnrollment() = (%#v,%v)", result, err)
	}
	if resolution, err := fixture.owner.ownerStore.ResolveChannelEnrollment(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveChannelEnrollment(after) = (%q,%v)", resolution, err)
	}
	replayed, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan)
	if err != nil || replayed.Status != result.Status || replayed.Receipt.ReceiptID() != result.Receipt.ReceiptID() {
		t.Fatalf("CommitChannelEnrollment(response loss) = (%#v,%v)", replayed, err)
	}
}

func TestAcceptChannelEnrollmentBridgeSignsOutsideTransactionsAndSkipsReplay(t *testing.T) {
	t.Parallel()
	fixture := newOwnerAcceptancePlanFixture(t, "accept-bridge-signing")
	probe := &enrollmentTransactionProbeSigner{delegate: fixture.owner.signer,
		db: fixture.owner.ownerStore.db}
	spec := fixture.acceptSpec
	spec.Signer = probe
	result, err := fixture.owner.ownerStore.AcceptChannelEnrollment(context.Background(), spec)
	if err != nil || result.Status != ChannelEnrollmentAccepted || probe.calls != 2 {
		t.Fatalf("fresh bridge = (%#v,%v), signer calls=%d", result, err, probe.calls)
	}
	replayProbe := &enrollmentTransactionProbeSigner{delegate: fixture.owner.signer,
		db: fixture.owner.ownerStore.db}
	spec.Signer, spec.At = replayProbe, spec.At.Add(time.Second)
	replayed, err := fixture.owner.ownerStore.AcceptChannelEnrollment(context.Background(), spec)
	if err != nil || replayed.Status != ChannelEnrollmentReplayed || replayProbe.calls != 0 {
		t.Fatalf("replay bridge = (%#v,%v), signer calls=%d", replayed, err, replayProbe.calls)
	}
}

type ownerAcceptancePlanFixture struct {
	owner       channelEnrollmentFixture
	signingSpec PrepareChannelEnrollmentSigningSpec
	acceptSpec  AcceptChannelEnrollmentSpec
}

func newOwnerAcceptancePlanFixture(t *testing.T, seed string) ownerAcceptancePlanFixture {
	t.Helper()
	owner := newChannelEnrollmentFixture(t, seed)
	transcript := owner.transcript(t, 0x53, 0x54, owner.head)
	proof := enrollmentTestProof(t, owner.token, transcript)
	signing := PrepareChannelEnrollmentSigningSpec{AuthenticatedPeerID: owner.joiner.PeerID(),
		Transcript: transcript, AdvertisedMultiaddrs: owner.joiner.Multiaddrs(),
		Proof: proof, At: owner.acceptedAt}
	accepted := AcceptChannelEnrollmentSpec{AuthenticatedPeerID: owner.joiner.PeerID(),
		Transcript: transcript, AdvertisedMultiaddrs: owner.joiner.Multiaddrs(), Proof: proof,
		Signer: owner.signer, At: owner.acceptedAt}
	return ownerAcceptancePlanFixture{owner: owner, signingSpec: signing, acceptSpec: accepted}
}

func (fixture ownerAcceptancePlanFixture) signingPlan(t *testing.T) ChannelEnrollmentSigningPlan {
	t.Helper()
	plan, err := fixture.owner.ownerStore.PrepareChannelEnrollmentSigning(context.Background(),
		fixture.signingSpec)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func (fixture ownerAcceptancePlanFixture) signatures(t *testing.T,
	plan ChannelEnrollmentSigningPlan,
) ChannelEnrollmentSignatures {
	t.Helper()
	member, err := fixture.owner.signer.Sign(context.Background(), plan.MemberSigningMessage())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := fixture.owner.signer.Sign(context.Background(), plan.ReceiptSigningMessage())
	if err != nil {
		t.Fatal(err)
	}
	return ChannelEnrollmentSignatures{MemberSignature: member, ReceiptSignature: receipt}
}

func (fixture ownerAcceptancePlanFixture) finalPlan(t *testing.T) ChannelEnrollmentPlan {
	t.Helper()
	signing := fixture.signingPlan(t)
	plan, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(), signing,
		fixture.signatures(t, signing))
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

type enrollmentTransactionProbeSigner struct {
	delegate ChannelAuthoritySigner
	db       *sql.DB
	calls    int
}

func (signer *enrollmentTransactionProbeSigner) Sign(ctx context.Context,
	message []byte,
) ([]byte, error) {
	var one int
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := signer.db.QueryRowContext(probeCtx, `SELECT 1`).Scan(&one); err != nil {
		return nil, fmt.Errorf("signer invoked with Store transaction open: %w", err)
	}
	if one != 1 {
		return nil, errors.New("signer Store probe returned an unexpected result")
	}
	signer.calls++
	return signer.delegate.Sign(ctx, message)
}
