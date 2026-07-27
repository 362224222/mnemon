package store

import (
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ArtifactStageState is the durable owner-scoped filesystem publication state.
// Publishing is never cleanup-eligible; it is a recovery obligation.
type ArtifactStageState string

const (
	ArtifactStageStaged     ArtifactStageState = "staged"
	ArtifactStagePublishing ArtifactStageState = "publishing"
	ArtifactStageReady      ArtifactStageState = "ready"
)

func (state ArtifactStageState) Valid() bool {
	return state == ArtifactStageStaged || state == ArtifactStagePublishing ||
		state == ArtifactStageReady
}

// OperationArtifactStageFence binds one physical stage generation to the
// exact operation lease that registered or recovered it.
type OperationArtifactStageFence struct {
	owner      artifactdomain.StageOwner
	leaseOwner string
	leaseUntil time.Time
}

func (fence OperationArtifactStageFence) Owner() artifactdomain.StageOwner {
	return fence.owner
}

func (fence OperationArtifactStageFence) LeaseOwner() string { return fence.leaseOwner }
func (fence OperationArtifactStageFence) LeaseUntil() time.Time {
	return fence.leaseUntil
}

type BeginOperationArtifactStageSpec struct {
	OperationID model.OperationID
	LeaseOwner  string
	LeaseUntil  time.Time
	At          time.Time
}

type OperationArtifactStageResult struct {
	fence    OperationArtifactStageFence
	state    ArtifactStageState
	replayed bool
}

func (result OperationArtifactStageResult) Fence() OperationArtifactStageFence {
	return result.fence
}

func (result OperationArtifactStageResult) State() ArtifactStageState { return result.state }
func (result OperationArtifactStageResult) Replayed() bool            { return result.replayed }

type PrepareOperationArtifactPublishSpec struct {
	Fence   OperationArtifactStageFence
	Capture model.JSON
	Closure VerifiedArtifactClosure
	At      time.Time
}

type MarkOperationArtifactReadySpec struct {
	Fence OperationArtifactStageFence
	At    time.Time
}

type BeginPeerInboxArtifactStageSpec struct {
	Fence PeerInboxArtifactFence
	At    time.Time
}

type PeerInboxArtifactStageRegistration struct {
	owner    artifactdomain.StageOwner
	state    ArtifactStageState
	replayed bool
}

func (result PeerInboxArtifactStageRegistration) Owner() artifactdomain.StageOwner {
	return result.owner
}

func (result PeerInboxArtifactStageRegistration) State() ArtifactStageState {
	return result.state
}

func (result PeerInboxArtifactStageRegistration) Replayed() bool { return result.replayed }
