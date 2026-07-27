package peer

import (
	"context"
	"errors"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type artifactReceiverStageRegistration struct {
	owner artifactdomain.StageOwner
	state store.ArtifactStageState
}

type artifactReceiverPublishCheckpoint struct {
	closure store.VerifiedArtifactClosure
	state   store.ArtifactStageState
}

func (receiver *ArtifactReceiver) beginArtifactStage(ctx context.Context,
	claim *artifactReceiverClaim,
) (artifactReceiverStageRegistration, *artifactReceiverClaimFailure, error) {
	live, err := receiver.ensureLease(ctx, claim)
	if err != nil {
		return artifactReceiverStageRegistration{}, nil, err
	}
	if !live {
		if ctx.Err() != nil {
			return artifactReceiverStageRegistration{}, nil, ctx.Err()
		}
		return artifactReceiverStageRegistration{}, nil, errArtifactReceiverClaimStale
	}
	at, err := receiver.now()
	if err != nil {
		return artifactReceiverStageRegistration{}, nil, artifactReceiverFatal(
			ArtifactReceiverFatalWorkerInvariant, "read Artifact stage clock", err)
	}
	var registration artifactReceiverStageRegistration
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		registration, err = receiver.backend.beginStage(ctx, claim.fence, at)
		if err == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(err) {
			break
		}
	}
	if err != nil {
		failure, classified := receiver.classifyStoreClaimFailure(
			"begin Inbox Artifact stage", err)
		return artifactReceiverStageRegistration{}, failure, classified
	}
	if registration.owner.IsZero() ||
		registration.owner.Kind() != artifactdomain.StageOwnerInbox ||
		registration.owner.CanonicalID() != claim.fence.inboxID.String() ||
		(registration.state != store.ArtifactStageStaged &&
			registration.state != store.ArtifactStagePublishing) {
		return artifactReceiverStageRegistration{}, nil, artifactReceiverFatal(
			ArtifactReceiverFatalStoreInvariant,
			"begin Inbox Artifact stage returned an invalid owner", nil)
	}
	return registration, nil, nil
}

func (receiver *ArtifactReceiver) prepareAndPublishArtifact(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage, closure artifactdomain.Closure,
) (*artifactReceiverClaimFailure, error) {
	if err := stage.VerifyClosure(ctx, closure); err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return receiver.classifyArtifactStageFailure(
			"verify Inbox Artifact stage", claim.fence.attempt, err)
	}
	live, err := receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return nil, err
	}
	at, err := receiver.now()
	if err != nil {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read Artifact prepare clock", err)
	}
	checkpoint := storeArtifactReceiverClosure(closure)
	if err := receiver.probeArtifactPrepare(ctx, claim.fence, stage.Owner(),
		checkpoint, at); err != nil {
		return receiver.classifyStoreClaimFailure("prepare Inbox Artifact publish", err)
	}
	receiver.recordCheckpoint()
	if err := receiver.acceptArtifactPublish(ctx, claim, stage.Owner()); err != nil {
		return nil, err
	}
	return receiver.publishAcceptedArtifactStage(ctx, claim, stage, closure)
}

func (receiver *ArtifactReceiver) resumeArtifactPublish(ctx context.Context,
	claim *artifactReceiverClaim, registration artifactReceiverStageRegistration,
	stage *artifactdomain.Stage,
) (*artifactReceiverClaimFailure, error) {
	at, err := receiver.now()
	if err != nil {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read Artifact recovery clock", err)
	}
	checkpoint, err := receiver.readArtifactPublish(
		ctx, claim.fence, registration.owner, at)
	if err != nil {
		return receiver.classifyStoreClaimFailure("read Inbox Artifact publish", err)
	}
	if checkpoint.state != registration.state &&
		!(registration.state == store.ArtifactStagePublishing &&
			checkpoint.state == store.ArtifactStageReady) {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalStoreInvariant,
			"recovered Inbox Artifact stage changed state", nil)
	}
	closure, err := artifactReceiverClosureFromStore(ctx, checkpoint.closure, at)
	if err != nil {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalStoreInvariant,
			"recovered Inbox Artifact closure is invalid", err)
	}
	receiver.recordCheckpoint()
	if checkpoint.state == store.ArtifactStageReady {
		if err := stage.Publish(ctx, closure); err != nil {
			return receiver.classifyArtifactStageFailure(
				"verify ready Inbox Artifact publish", claim.fence.attempt, err)
		}
		receiver.recordReady()
		return nil, nil
	}
	if err := receiver.acceptArtifactPublish(ctx, claim, registration.owner); err != nil {
		return nil, err
	}
	return receiver.publishAcceptedArtifactStage(ctx, claim, stage, closure)
}

func (receiver *ArtifactReceiver) acceptArtifactPublish(ctx context.Context,
	claim *artifactReceiverClaim, owner artifactdomain.StageOwner,
) error {
	at, err := receiver.now()
	if err != nil {
		return artifactReceiverFatal(ArtifactReceiverFatalWorkerInvariant,
			"read Artifact acceptance clock", err)
	}
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		err = receiver.backend.acceptPublish(ctx, claim.fence, owner, at)
		if err == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(err) {
			break
		}
	}
	if err == nil || ctx.Err() != nil {
		return nil
	}
	if errors.Is(err, store.ErrPeerInboxArtifactStale) {
		receiver.recordStale()
		return errArtifactReceiverClaimStale
	}
	if errors.Is(err, store.ErrPeerInboxArtifactAuthority) {
		return errArtifactReceiverAuthorityChanged
	}
	return receiver.storeFatal("accept Inbox Artifact publish", err)
}

func (receiver *ArtifactReceiver) publishAcceptedArtifactStage(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage, closure artifactdomain.Closure,
) (*artifactReceiverClaimFailure, error) {
	if err := stage.Publish(ctx, closure); err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, receiver.casFatal("publish accepted Inbox Artifact stage", err)
	}
	failure, err := receiver.markReady(ctx, claim, stage.Owner())
	if failure != nil {
		return nil, artifactReceiverFatal(ArtifactReceiverFatalStoreInvariant,
			"accepted Inbox Artifact publish cannot be rescheduled", nil)
	}
	return nil, err
}

func (receiver *ArtifactReceiver) probeArtifactPrepare(ctx context.Context,
	fence artifactReceiverFence, owner artifactdomain.StageOwner,
	closure store.VerifiedArtifactClosure, at time.Time,
) error {
	var err error
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		err = receiver.backend.preparePublish(ctx, fence, owner, closure, at)
		if err == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(err) {
			break
		}
	}
	return err
}

func (receiver *ArtifactReceiver) readArtifactPublish(ctx context.Context,
	fence artifactReceiverFence, owner artifactdomain.StageOwner, at time.Time,
) (artifactReceiverPublishCheckpoint, error) {
	var checkpoint artifactReceiverPublishCheckpoint
	var err error
	for probe := 0; probe < artifactReceiverResponseLossProbe; probe++ {
		checkpoint, err = receiver.backend.readPublish(ctx, fence, owner, at)
		if err == nil || ctx.Err() != nil || artifactReceiverClosedStoreError(err) {
			break
		}
	}
	return checkpoint, err
}

func (receiver *ArtifactReceiver) classifyArtifactStageFailure(operation string,
	attempt uint32, cause error,
) (*artifactReceiverClaimFailure, error) {
	if artifactReceiverResourceFailure(cause) {
		return retryArtifactReceiverClaim(store.PeerInboxArtifactRetryResourceExhausted,
			artifactReceiverBackoff(attempt)), nil
	}
	return nil, receiver.casFatal(operation, cause)
}

func artifactReceiverClosureFromStore(ctx context.Context,
	durable store.VerifiedArtifactClosure, at time.Time,
) (artifactdomain.Closure, error) {
	return store.RebuildArtifactClosure(ctx, durable, at)
}

func (backend durableArtifactReceiverBackend) beginStage(ctx context.Context,
	fence artifactReceiverFence, at time.Time,
) (artifactReceiverStageRegistration, error) {
	if !fence.hasDurable {
		return artifactReceiverStageRegistration{}, store.ErrPeerInboxArtifactInput
	}
	result, err := backend.store.BeginPeerInboxArtifactStage(ctx,
		store.BeginPeerInboxArtifactStageSpec{Fence: fence.durable, At: at})
	if err != nil {
		return artifactReceiverStageRegistration{}, err
	}
	return artifactReceiverStageRegistration{
		owner: result.Owner(), state: result.State(),
	}, nil
}

func (backend durableArtifactReceiverBackend) preparePublish(ctx context.Context,
	fence artifactReceiverFence, owner artifactdomain.StageOwner,
	closure store.VerifiedArtifactClosure, at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	_, err := backend.store.PreparePeerInboxArtifactPublish(ctx,
		store.PreparePeerInboxArtifactPublishSpec{
			Fence: fence.durable, Owner: owner, Closure: closure, At: at,
		})
	return err
}

func (backend durableArtifactReceiverBackend) acceptPublish(ctx context.Context,
	fence artifactReceiverFence, owner artifactdomain.StageOwner, at time.Time,
) error {
	if !fence.hasDurable {
		return store.ErrPeerInboxArtifactInput
	}
	_, err := backend.store.AcceptPeerInboxArtifactPublish(ctx,
		store.AcceptPeerInboxArtifactPublishSpec{
			Fence: fence.durable, Owner: owner, At: at,
		})
	return err
}

func (backend durableArtifactReceiverBackend) readPublish(ctx context.Context,
	fence artifactReceiverFence, owner artifactdomain.StageOwner, at time.Time,
) (artifactReceiverPublishCheckpoint, error) {
	if !fence.hasDurable {
		return artifactReceiverPublishCheckpoint{}, store.ErrPeerInboxArtifactInput
	}
	result, err := backend.store.ReadPeerInboxArtifactPublish(ctx,
		store.ReadPeerInboxArtifactPublishSpec{Fence: fence.durable, Owner: owner, At: at})
	if err != nil {
		return artifactReceiverPublishCheckpoint{}, err
	}
	return artifactReceiverPublishCheckpoint{
		closure: result.Closure(), state: result.State(),
	}, nil
}
