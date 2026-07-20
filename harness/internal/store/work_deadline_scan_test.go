package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestScanWorkDeadlinesIsBoundedDeterministicAndSeparatesExhaustion(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	works := make([]model.ReviewWork, 10)
	for index := range works {
		works[index] = commitDeadlineScanWork(t, fixture, fmt.Sprintf("bounded-%02d", index))
	}
	exhausted := works[5]
	mustExec(t, fixture.store, `UPDATE works SET version=? WHERE home_peer_id=? AND work_id=?`,
		model.MaxSQLiteInteger, exhausted.Ref().HomePeerID().String(), exhausted.Ref().WorkID().String())
	trustedNow := works[0].Deadline()

	first, err := fixture.store.ScanWorkDeadlines(context.Background(), trustedNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Due()) != WorkDeadlineScanLimit || !first.MoreDue() ||
		first.ExhaustedCount() != 1 || len(first.Exhausted()) != 1 ||
		first.Exhausted()[0] != exhausted.Ref() || first.NextDeadlineUnixNano() != 0 {
		t.Fatalf("deadline scan = due %d more=%t exhausted=%d/%v next=%d",
			len(first.Due()), first.MoreDue(), first.ExhaustedCount(), first.Exhausted(),
			first.NextDeadlineUnixNano())
	}
	want := []model.WorkRef{works[0].Ref(), works[1].Ref(), works[2].Ref(), works[3].Ref(),
		works[4].Ref(), works[6].Ref(), works[7].Ref(), works[8].Ref()}
	assertScannedDeadlineRefs(t, first.Due(), want)
	dueCopy, exhaustedCopy := first.Due(), first.Exhausted()
	dueCopy[0], exhaustedCopy[0] = WorkDeadlineCandidate{}, model.WorkRef{}
	if first.Due()[0].Work().Ref().IsZero() || first.Exhausted()[0].IsZero() {
		t.Fatal("scan exposed mutable result slices")
	}

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	second, err := restarted.ScanWorkDeadlines(context.Background(), trustedNow)
	if err != nil {
		t.Fatal(err)
	}
	assertRestartedDeadlineScan(t, second.Due(), first.Due(), want)
}

func assertScannedDeadlineRefs(t *testing.T, candidates []WorkDeadlineCandidate,
	want []model.WorkRef,
) {
	t.Helper()
	for index, candidate := range candidates {
		if candidate.Work().Ref() != want[index] || candidate.Work().Version() != 1 ||
			candidate.Cause().EventID() != candidate.Work().UpdatedBy() {
			t.Fatalf("due[%d] = %#v", index, candidate)
		}
	}
}

func assertRestartedDeadlineScan(t *testing.T, candidates, prior []WorkDeadlineCandidate,
	want []model.WorkRef,
) {
	t.Helper()
	for index, candidate := range candidates {
		if candidate.Work().Ref() != want[index] || candidate.Cause() != prior[index].Cause() {
			t.Fatalf("restart scan changed candidate %d", index)
		}
	}
}

func TestScanWorkDeadlinesSkipsFrozenChannelWithoutStarvingHealthyWork(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	frozenChannel := fixture.channel
	for index := 0; index < WorkDeadlineScanLimit; index++ {
		commitDeadlineScanWork(t, fixture, fmt.Sprintf("frozen-%02d", index))
	}
	mustExec(t, fixture.store, `UPDATE channels SET status='closed',topic_state='left'
		WHERE channel_id=?`, frozenChannel.String())
	installAdditionalDeadlineChannel(t, fixture, "healthy-channel")
	healthy := commitDeadlineScanWork(t, fixture, "healthy")

	scan, err := fixture.store.ScanWorkDeadlines(context.Background(), healthy.Deadline())
	if err != nil {
		t.Fatal(err)
	}
	if due := scan.Due(); len(due) != 1 || due[0].Work().Ref() != healthy.Ref() || scan.MoreDue() {
		t.Fatalf("active-channel due set = %#v, more=%t", due, scan.MoreDue())
	}
}

func commitDeadlineScanWork(t *testing.T, fixture *acceptanceFixture,
	suffix string,
) model.ReviewWork {
	t.Helper()
	_, authority := fixture.reserveOffer(t, "deadline-scan-"+suffix, nil)
	spec := fixture.offer(t, authority, "deadline-scan-"+suffix, fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now); err != nil {
		t.Fatal(err)
	}
	work, err := fixture.store.GetReviewWork(context.Background(), spec.Items[0].Work.Work.Ref())
	if err != nil {
		t.Fatal(err)
	}
	return work
}

func installAdditionalDeadlineChannel(t *testing.T, fixture *acceptanceFixture,
	seed string,
) {
	t.Helper()
	owner := testkit.NewIdentity(t, "owner:accept-"+t.Name())
	signed := testkit.NewSignedChannelForOwnerAt(t, seed, owner,
		fixture.now.Add(-time.Hour+2*time.Minute))
	reviewer := testkit.NewIdentity(t, "peer-accept-reviewer-a")
	if reviewer.PeerID() != fixture.reviewers[0] {
		t.Fatal("additional Channel reviewer identity drifted")
	}
	signed.AppendActiveIdentity(t, reviewer)
	fixture.channel = signed.Channel().ID()
	installAcceptanceChannel(t, fixture, signed)
}
