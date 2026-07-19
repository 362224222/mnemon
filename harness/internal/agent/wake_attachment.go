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

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const RunAttachmentEnvironment = "MNEMON_HARNESS_RUN_ATTACHMENT"

var (
	ErrWakeAttachment      = errors.New("prepare managed wake attachment")
	ErrWakeStoreNotInvoked = errors.New("managed wake Store operation was not invoked")
)

type WakePreclaimStore interface {
	PreclaimAgentWake(context.Context, store.AgentWakePreclaimSpec) (store.AgentClaimResult, error)
	ListReapableAgentRunAttachments(context.Context,
		store.AgentAttachmentCleanupSpec) ([]store.ReapableAgentRunAttachment, error)
}

// WakeAttachmentCandidate carries only the filesystem identity that the Store
// needs to authorize removal. Capability bytes remain owned by the filesystem
// adapter.
type WakeAttachmentCandidate struct {
	RunID     model.RunID
	TokenHash model.Digest
}

// WakeAttachmentFilesystem is the consumer-owned port for crash-ordered Run
// attachment operations. Implementations retain path, inode, ownership and
// capability fencing internally.
type WakeAttachmentFilesystem interface {
	ListCandidates() ([]WakeAttachmentCandidate, error)
	RemoveReapable(model.RunID, model.Digest) (bool, error)
	CleanupStages(time.Time) (int, error)
	Stage(io.Reader) (StagedRunAttachment, error)
}

type StagedRunAttachment interface {
	TokenHash() model.Digest
	Publish(model.RunID) (RunAttachment, error)
	Discard() error
}

type RunAttachment interface {
	Path() string
	Remove() error
}

type WakeAttachmentOptions struct {
	Attachments   WakeAttachmentFilesystem
	AssetRevision string
	Clock         ServiceClock
	Random        io.Reader
}

// WakeAttachmentPreparer owns the crash-ordered filesystem/SQLite boundary
// immediately before a wake adapter starts a Runtime. It does not launch a
// process or treat Runtime completion as Handling completion.
type WakeAttachmentPreparer struct {
	store         WakePreclaimStore
	attachments   WakeAttachmentFilesystem
	assetRevision string
	clock         ServiceClock
	random        io.Reader
	randomMu      sync.Mutex
}

type PreparedWake struct {
	status     store.AgentClaimStatus
	run        model.AgentRun
	attachment RunAttachment
}

func NewWakeAttachmentPreparer(st WakePreclaimStore,
	options WakeAttachmentOptions,
) (*WakeAttachmentPreparer, error) {
	if st == nil || options.Attachments == nil || options.AssetRevision == "" {
		return nil, fmt.Errorf("%w: Store, attachment filesystem and asset revision are required",
			ErrWakeAttachment)
	}
	if options.Clock == nil {
		options.Clock = wallServiceClock{}
	}
	if options.Random == nil {
		options.Random = cryptorandReader{}
	}
	return &WakeAttachmentPreparer{store: st, attachments: options.Attachments,
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
	filesystemCandidates, err := p.attachments.ListCandidates()
	if err != nil {
		return PreparedWake{}, fmt.Errorf("%w: scan attachment candidates: %v", ErrWakeAttachment, err)
	}
	candidates := make([]store.AgentRunAttachmentCandidate, len(filesystemCandidates))
	for index, candidate := range filesystemCandidates {
		candidates[index] = store.AgentRunAttachmentCandidate{RunID: candidate.RunID,
			TokenHash: candidate.TokenHash}
	}
	reapable, err := p.store.ListReapableAgentRunAttachments(ctx, store.AgentAttachmentCleanupSpec{
		ProfileID: profile.ID(), ExpectedAssetRevision: p.assetRevision, At: at,
		Candidates: candidates,
	})
	if err != nil {
		if errors.Is(err, ErrWakeStoreNotInvoked) {
			return PreparedWake{}, errors.Join(ErrWakeAttachment, ErrWakeStoreNotInvoked)
		}
		return PreparedWake{}, fmt.Errorf("%w: list expired capabilities: %v", ErrWakeAttachment, err)
	}
	for _, target := range reapable {
		if _, err := p.attachments.RemoveReapable(target.RunID, target.TokenHash); err != nil {
			return PreparedWake{}, fmt.Errorf("%w: remove expired capability: %v", ErrWakeAttachment, err)
		}
	}
	if _, err := p.attachments.CleanupStages(at); err != nil {
		return PreparedWake{}, fmt.Errorf("%w: cleanup orphan stages: %v", ErrWakeAttachment, err)
	}
	p.randomMu.Lock()
	defer p.randomMu.Unlock()
	staged, err := p.stageAttachment()
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
		if errors.Is(err, ErrWakeStoreNotInvoked) {
			return PreparedWake{}, errors.Join(ErrWakeAttachment, ErrWakeStoreNotInvoked)
		}
		return PreparedWake{}, fmt.Errorf("%w: Store preclaim: %v", ErrWakeAttachment, err)
	}
	if claimed.Status != store.AgentClaimActionable {
		return PreparedWake{status: claimed.Status}, nil
	}
	if claimed.Run.ID().IsZero() {
		return PreparedWake{}, fmt.Errorf("%w: Store returned an actionable preclaim without a Run",
			ErrWakeAttachment)
	}
	attachment, err := publishRunAttachment(staged, claimed.Run.ID())
	if err != nil {
		// The preclaim transaction already committed the Run. Preserve that
		// durable authority for the worker so it can record an immediate
		// launch failure; the empty attachment still makes this result
		// impossible to launch.
		return PreparedWake{status: claimed.Status, run: claimed.Run}, fmt.Errorf("%w: publish capability: %v",
			ErrWakeAttachment, err)
	}
	prepared := PreparedWake{status: claimed.Status, run: claimed.Run, attachment: attachment}
	discard = false
	return prepared, nil
}

func (p *WakeAttachmentPreparer) stageAttachment() (StagedRunAttachment, error) {
	staged, err := p.attachments.Stage(p.random)
	if err != nil {
		return nil, err
	}
	if staged == nil {
		return nil, errors.New("attachment filesystem returned no stage")
	}
	return staged, nil
}

func publishRunAttachment(staged StagedRunAttachment, runID model.RunID) (RunAttachment, error) {
	attachment, err := staged.Publish(runID)
	if err != nil {
		return nil, err
	}
	if attachment == nil || attachment.Path() == "" {
		return nil, errors.New("attachment filesystem returned no attachment")
	}
	return attachment, nil
}

func (p PreparedWake) Status() store.AgentClaimStatus { return p.status }
func (p PreparedWake) Run() model.AgentRun            { return p.run }
func (p PreparedWake) AttachmentPath() string {
	if p.attachment == nil {
		return ""
	}
	return p.attachment.Path()
}

// Environment returns exactly one reference assignment for the Runtime
// adapter. It contains no capability bytes.
func (p PreparedWake) Environment() string {
	if p.status != store.AgentClaimActionable || p.AttachmentPath() == "" {
		return ""
	}
	return RunAttachmentEnvironment + "=" + p.AttachmentPath()
}

// RemoveAttachment is used when launch fails before the Runtime inherits the
// file. The durable claim remains fenced and will be requeued on lease expiry.
func (p PreparedWake) RemoveAttachment() error {
	if p.status != store.AgentClaimActionable || p.AttachmentPath() == "" {
		return nil
	}
	return p.attachment.Remove()
}

// cryptorandReader avoids exposing a mutable package-global Reader through
// the preparer options while still defaulting to crypto/rand.
type cryptorandReader struct{}

func (cryptorandReader) Read(buffer []byte) (int, error) { return cryptorand.Read(buffer) }
