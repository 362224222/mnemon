package node

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestAssembleWorkExpiryBuildsExactSignedTransition(t *testing.T) {
	t.Parallel()
	authority, signer := deadlineBoundaryAuthority(t)
	item, err := assembleWorkExpiry(context.Background(), signer, authority)
	if err != nil {
		t.Fatal(err)
	}
	event := item.Publication.Event()
	if item.Work == nil || event.ID() != authority.eventID ||
		event.Type() != model.EventReviewExpired || event.Scope() != authority.scope ||
		!event.AcceptedAt().Equal(authority.acceptedAt) || !event.CreatedAt().Equal(authority.acceptedAt) ||
		len(event.CausedBy()) != 1 || event.CausedBy()[0] != authority.cause ||
		item.Work.ExpectedVersion != authority.work.Version() ||
		item.Work.ExpectedState != authority.work.State() ||
		item.Work.Work.State() != model.WorkExpired ||
		item.Work.Work.Version() != authority.work.Version()+1 ||
		item.Work.Work.UpdatedBy() != authority.eventID {
		t.Fatalf("assembled expiry = %#v / %#v", event, item.Work)
	}
	if err := model.VerifyPublication(signer.PublicKey(), item.Publication); err != nil {
		t.Fatalf("verify expiry publication: %v", err)
	}
	if got := event.Payload().String(); got != `{"deadline":"2026-07-19T07:00:00Z","iteration":1,"work_version":1}` {
		t.Fatalf("expiry payload = %s", got)
	}
}

func TestAssembleWorkExpiryRejectsEarlyOrIncompleteAuthority(t *testing.T) {
	t.Parallel()
	authority, signer := deadlineBoundaryAuthority(t)
	early := authority
	early.acceptedAt = authority.work.Deadline().Add(-time.Nanosecond)
	if _, err := assembleWorkExpiry(context.Background(), signer, early); !errors.Is(err, store.ErrWorkDeadlineStale) {
		t.Fatalf("early expiry error = %v", err)
	}
	incomplete := authority
	incomplete.cause = model.EventKey{}
	if _, err := assembleWorkExpiry(context.Background(), signer, incomplete); !errors.Is(err, store.ErrWorkDeadlineInvariant) {
		t.Fatalf("incomplete expiry error = %v", err)
	}
}

func TestMapWorkExpiryErrorPreservesStoreCategories(t *testing.T) {
	t.Parallel()
	if err := mapWorkExpiryError(eventpkg.ErrWorkExpiryStale); !errors.Is(err, store.ErrWorkDeadlineStale) {
		t.Fatalf("stale mapping = %v", err)
	}
	if err := mapWorkExpiryError(eventpkg.ErrWorkExpiryInvariant); !errors.Is(err, store.ErrWorkDeadlineInvariant) {
		t.Fatalf("invariant mapping = %v", err)
	}
	transport := errors.New("signing transport failed")
	if err := mapWorkExpiryError(transport); err != transport {
		t.Fatalf("transport mapping = %v", err)
	}
}

func deadlineBoundaryAuthority(t *testing.T) (workExpiryEventAuthority, *eventpkg.Ed25519Signer) {
	t.Helper()
	acceptedAt := deadlineWorkerTime(t, "2026-07-19T07:00:00Z")
	work := deadlineWorkerCandidate(t, "boundary", acceptedAt).work
	epoch, _ := model.ParseOriginEpoch("epoch-deadline-boundary")
	asset := model.Sum([]byte("deadline-boundary-assets")).String()
	node, err := model.NewNode(model.NodeSpec{PeerID: work.Ref().HomePeerID(), OriginEpoch: epoch,
		NextOriginSequence: 11, ActiveAssetRevision: asset,
		CreatedAt: acceptedAt.Add(-2 * time.Hour), UpdatedAt: acceptedAt.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-deadline-boundary", WorkspaceRoot: t.TempDir(), Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("deadline-credential")),
		ActiveAssetRevision: asset, HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: true,
		CreatedAt: acceptedAt.Add(-2 * time.Hour), UpdatedAt: acceptedAt.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	memberHead, err := model.NewRecordHead(1, model.Sum([]byte("deadline-member")))
	if err != nil {
		t.Fatal(err)
	}
	rosterHead, err := model.NewRecordHead(2, model.Sum([]byte("deadline-roster")))
	if err != nil {
		t.Fatal(err)
	}
	scope, err := model.NewEventScope(work.ChannelID(), work.Ref().HomePeerID(), epoch,
		11, 19, memberHead, rosterHead, work.Ref())
	if err != nil {
		t.Fatal(err)
	}
	causeID, _ := model.ParseEventID("event-deadline-boundary-cause")
	cause, err := model.NewEventKey(work.Ref().HomePeerID(), epoch, causeID)
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := model.ParseEventID("event-deadline-boundary-expired")
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := eventpkg.NewEd25519Signer(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return workExpiryEventAuthority{work: work, cause: cause, eventID: eventID,
		node: node, profile: profile, scope: scope, acceptedAt: acceptedAt}, signer
}
