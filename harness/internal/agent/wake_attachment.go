package agent

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrWakeAttachment = errors.New("prepare managed wake attachment")

type WakePreclaimStore interface {
	PreclaimAgentWake(context.Context, store.AgentWakePreclaimSpec) (store.AgentClaimResult, error)
	ListReapableAgentRunAttachments(context.Context,
		store.AgentAttachmentCleanupSpec) ([]store.ReapableAgentRunAttachment, error)
}

type WakeAttachmentOptions struct {
	NodeState     string
	AssetRevision string
	Clock         ServiceClock
	Random        io.Reader
}

// WakeAttachmentPreparer owns the crash-ordered filesystem/SQLite boundary
// immediately before a wake adapter starts a Runtime. It does not launch a
// process or treat Runtime completion as Handling completion.
type WakeAttachmentPreparer struct {
	store         WakePreclaimStore
	nodeState     string
	assetRevision string
	clock         ServiceClock
	random        io.Reader
	randomMu      sync.Mutex
}

type PreparedWake struct {
	status     store.AgentClaimStatus
	run        model.AgentRun
	attachment localapi.RunAttachment
	nodeState  string
}

func NewWakeAttachmentPreparer(st WakePreclaimStore,
	options WakeAttachmentOptions,
) (*WakeAttachmentPreparer, error) {
	if st == nil || options.NodeState == "" || options.AssetRevision == "" {
		return nil, fmt.Errorf("%w: Store, Node state and asset revision are required", ErrWakeAttachment)
	}
	if options.Clock == nil {
		options.Clock = wallServiceClock{}
	}
	if options.Random == nil {
		options.Random = cryptorandReader{}
	}
	return &WakeAttachmentPreparer{store: st, nodeState: options.NodeState,
		assetRevision: options.AssetRevision, clock: options.Clock, random: options.Random}, nil
}

// Prepare stages a capability before the Store transaction, publishes it only
// after a server-owned Run ID exists, and returns no launchable attachment for
// none/busy/waiting outcomes.
func (p *WakeAttachmentPreparer) Prepare(ctx context.Context,
	profile model.Profile,
) (PreparedWake, error) {
	if p == nil || ctx == nil || profile.ID() != model.TeamworkProfileID() || !profile.Enabled() ||
		profile.ActiveAssetRevision() != p.assetRevision {
		return PreparedWake{}, fmt.Errorf("%w: active Profile authority differs", ErrWakeAttachment)
	}
	at := p.clock.Now().Round(0).UTC()
	if at.IsZero() || at.UnixNano() <= 0 || !time.Unix(0, at.UnixNano()).UTC().Equal(at) {
		return PreparedWake{}, fmt.Errorf("%w: clock is invalid", ErrWakeAttachment)
	}
	budget, err := model.ParseHandlingBudget(profile.HandlingBudget())
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: Profile handling budget is invalid", ErrWakeAttachment)
	}
	leaseUntil := at.Add(time.Duration(budget.Spec().ClaimLeaseSeconds) * time.Second)
	if !leaseUntil.After(at) {
		return PreparedWake{}, fmt.Errorf("%w: claim lease cannot be represented", ErrWakeAttachment)
	}
	page, err := localapi.ListRunAttachmentCandidates(p.nodeState)
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: scan attachment candidates: %v", ErrWakeAttachment, err)
	}
	filesystemCandidates := page.Candidates()
	candidates := make([]store.AgentRunAttachmentCandidate, len(filesystemCandidates))
	for index, candidate := range filesystemCandidates {
		candidates[index] = store.AgentRunAttachmentCandidate{RunID: candidate.RunID(),
			TokenHash: candidate.TokenHash()}
	}
	reapable, err := p.store.ListReapableAgentRunAttachments(ctx, store.AgentAttachmentCleanupSpec{
		ProfileID: profile.ID(), ExpectedAssetRevision: p.assetRevision, At: at,
		Candidates: candidates,
	})
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: list expired capabilities: %v", ErrWakeAttachment, err)
	}
	for _, target := range reapable {
		if _, err := localapi.RemoveReapableRunAttachment(p.nodeState, target.RunID,
			target.TokenHash); err != nil {
			return PreparedWake{}, fmt.Errorf("%w: remove expired capability: %v", ErrWakeAttachment, err)
		}
	}
	if _, err := localapi.CleanupRunAttachmentStages(p.nodeState, at); err != nil {
		return PreparedWake{}, fmt.Errorf("%w: cleanup orphan stages: %v", ErrWakeAttachment, err)
	}
	p.randomMu.Lock()
	defer p.randomMu.Unlock()
	staged, err := localapi.StageRunAttachment(p.nodeState, p.random)
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: stage capability: %v", ErrWakeAttachment, err)
	}
	discard := true
	defer func() {
		if discard {
			_ = staged.Discard()
		}
	}()
	ownerBytes := make([]byte, managedSecretBytes)
	if _, err := io.ReadFull(p.random, ownerBytes); err != nil {
		clear(ownerBytes)
		return PreparedWake{}, fmt.Errorf("%w: generate claim owner: %v", ErrWakeAttachment, err)
	}
	claimOwner := "wake-" + base64.RawURLEncoding.EncodeToString(ownerBytes)
	clear(ownerBytes)
	claimed, err := p.store.PreclaimAgentWake(ctx, store.AgentWakePreclaimSpec{
		ProfileID: profile.ID(), ExpectedAssetRevision: p.assetRevision, ClaimOwner: claimOwner,
		AttachmentTokenHash: staged.TokenHash(), At: at, LeaseUntil: leaseUntil,
	})
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: Store preclaim: %v", ErrWakeAttachment, err)
	}
	if claimed.Status != store.AgentClaimActionable {
		return PreparedWake{status: claimed.Status}, nil
	}
	if claimed.Run.ID().IsZero() {
		return PreparedWake{}, fmt.Errorf("%w: Store returned an actionable preclaim without a Run",
			ErrWakeAttachment)
	}
	attachment, err := staged.Publish(claimed.Run.ID())
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: publish capability: %v", ErrWakeAttachment, err)
	}
	discard = false
	return PreparedWake{status: claimed.Status, run: claimed.Run, attachment: attachment,
		nodeState: p.nodeState}, nil
}

func (p PreparedWake) Status() store.AgentClaimStatus { return p.status }
func (p PreparedWake) Run() model.AgentRun            { return p.run }
func (p PreparedWake) AttachmentPath() string         { return p.attachment.Path() }

// Environment returns exactly one reference assignment for the Runtime
// adapter. It contains no capability bytes.
func (p PreparedWake) Environment() string {
	if p.status != store.AgentClaimActionable || p.attachment.Path() == "" {
		return ""
	}
	return localapi.RunAttachmentEnv + "=" + p.attachment.Path()
}

// RemoveAttachment is used when launch fails before the Runtime inherits the
// file. The durable claim remains fenced and will be requeued on lease expiry.
func (p PreparedWake) RemoveAttachment() error {
	if p.status != store.AgentClaimActionable || p.attachment.Path() == "" {
		return nil
	}
	return localapi.RemoveRunAttachment(p.nodeState, p.attachment)
}

// cryptorandReader avoids exposing a mutable package-global Reader through
// the preparer options while still defaulting to crypto/rand.
type cryptorandReader struct{}

func (cryptorandReader) Read(buffer []byte) (int, error) { return cryptorand.Read(buffer) }
