package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readParentResumeCurrentAuthority(ctx context.Context, tx *sql.Tx, handling model.Handling,
	event model.Event, budget model.HandlingBudget, at time.Time,
) (freshCurrentAuthority, error) {
	authority, parent, err := readParentResumeDerivationAuthority(ctx, tx, handling, event)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	if at.Before(event.AcceptedAt()) || at.Before(parent.UpdatedAt()) {
		return freshCurrentAuthority{}, fmt.Errorf("%w: trusted time precedes parent-resume evidence",
			ErrCurrentReadInvariant)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return freshCurrentAuthority{}, fmt.Errorf("%w: parent-resume local Node: %v",
			ErrCurrentReadInvariant, err)
	}
	role, err := localCurrentRole(node.PeerID(), parent)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	if role != model.CurrentReviewer || node.PeerID() != authority.derivation.Child().HomePeerID() {
		return freshCurrentAuthority{}, fmt.Errorf("%w: parent-resume is not local reviewer authority",
			ErrCurrentReadInvariant)
	}
	brief, err := readCurrentWorkBrief(ctx, tx, parent)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	children, terminal, err := readParentResumeChildResults(ctx, tx, authority, budget)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	if terminal.id != event.ID() {
		return freshCurrentAuthority{}, fmt.Errorf("%w: parent-resume Handling is not bound to terminal child Event",
			ErrCurrentReadInvariant)
	}
	return freshCurrentAuthority{event: event, work: parent, role: role, brief: brief,
		childResults: children}, nil
}

func readParentResumeDerivationAuthority(ctx context.Context, tx *sql.Tx, handling model.Handling,
	event model.Event,
) (workDerivationAuthority, model.ReviewWork, error) {
	authority, found, err := readWorkDerivationAuthority(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return workDerivationAuthority{}, model.ReviewWork{}, fmt.Errorf("%w: parent-resume derivation: %v",
			ErrCurrentReadInvariant, err)
	}
	if !found {
		return workDerivationAuthority{}, model.ReviewWork{}, fmt.Errorf("%w: parent-resume child derivation is unavailable",
			ErrCurrentReadInvariant)
	}
	if err := requireParentResumeHandlingID(handling, authority); err != nil {
		return workDerivationAuthority{}, model.ReviewWork{}, err
	}
	parent, err := readReviewWork(ctx, tx, authority.derivation.Parent())
	if err != nil {
		return workDerivationAuthority{}, model.ReviewWork{}, fmt.Errorf("%w: parent-resume action Work: %v",
			ErrCurrentReadInvariant, err)
	}
	if err := requireExactParentResumeWork(parent, authority); err != nil {
		return workDerivationAuthority{}, model.ReviewWork{}, err
	}
	return authority, parent, nil
}

func requireParentResumeHandlingID(handling model.Handling, authority workDerivationAuthority) error {
	resumeID, err := deterministicDerivationHandlingID(authority.derivation.OperationID())
	if err != nil {
		return fmt.Errorf("%w: parent-resume Handling identity: %v",
			ErrCurrentReadInvariant, err)
	}
	if handling.ID() != resumeID {
		return fmt.Errorf("%w: Handling is not the derivation resume",
			ErrCurrentReadInvariant)
	}
	return nil
}

func requireExactParentResumeWork(parent model.ReviewWork, authority workDerivationAuthority) error {
	derivation := authority.derivation
	if parent.ChannelID() != derivation.ParentChannelID() || parent.Ref() != derivation.Parent() ||
		parent.Participants().ReviewerPeerID() != derivation.Child().HomePeerID() ||
		parent.Version() != derivation.ParentVersion() || parent.UpdatedBy() != derivation.ParentEventID() ||
		(parent.State() != model.WorkActive && parent.State() != model.WorkRework) {
		return fmt.Errorf("%w: parent-resume parent Work is no longer exact",
			ErrCurrentReadStale)
	}
	return nil
}

func readParentResumeChildResults(ctx context.Context, tx *sql.Tx,
	authority workDerivationAuthority,
	budget model.HandlingBudget,
) ([]model.CurrentChildResult, terminalChildEvent, error) {
	derivation := authority.derivation
	child, err := readReviewWork(ctx, tx, derivation.Child())
	if err != nil {
		return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume child: %v",
			ErrCurrentReadInvariant, err)
	}
	if child.ChannelID() != derivation.ChildChannelID() || child.Ref() != derivation.Child() ||
		!child.State().Terminal() {
		return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume child is not terminal",
			ErrCurrentReadInvariant)
	}
	terminal, roots, err := readTerminalChildEvent(ctx, tx, authority, child)
	if err != nil {
		return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume terminal child: %v",
			ErrCurrentReadInvariant, err)
	}
	if len(roots) > budget.Spec().MaxCurrentArtifactRefs {
		return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume child Artifact refs exceed Profile budget",
			ErrCurrentReadTooLarge)
	}
	refs := make([]model.CurrentArtifactRef, len(roots))
	for index, root := range roots {
		ref, err := model.NewCurrentArtifactRef(root)
		if err != nil {
			return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume child result root: %v",
				ErrCurrentReadInvariant, err)
		}
		refs[index] = ref
	}
	result, err := model.NewCurrentChildResult(model.CurrentChildResultSpec{
		Ordinal: 0, WorkRef: child.Ref(), State: child.State(),
		Version: child.Version(), Iteration: child.Iteration(), ArtifactRefs: refs,
	})
	if err != nil {
		return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume child result: %v",
			ErrCurrentReadInvariant, err)
	}
	return []model.CurrentChildResult{result}, terminal, nil
}

func validateStoredParentResumeCurrentWork(ctx context.Context, tx *sql.Tx,
	receipt model.CurrentReadReceipt, event model.Event, work model.ReviewWork,
	budget model.HandlingBudget,
) error {
	authority, err := readStoredParentResumeAuthority(ctx, tx, event, work)
	if err != nil {
		return err
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return fmt.Errorf("%w: replay parent-resume local Node: %v", ErrCurrentReadInvariant, err)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil {
		return err
	}
	if err := requireStoredParentResumeReviewer(node.PeerID(), role, authority); err != nil {
		return err
	}
	brief, err := readCurrentWorkBrief(ctx, tx, work)
	if err != nil {
		return err
	}
	if err := requireStoredParentResumeWorkProjection(receipt.Projection().ActionWork(),
		work, authority, role, brief); err != nil {
		return err
	}
	children, terminal, err := readParentResumeChildResults(ctx, tx, authority, budget)
	if err != nil {
		return err
	}
	if terminal.id != event.ID() || !sameCurrentChildResults(children, receipt.Projection().ChildResults()) {
		return fmt.Errorf("%w: replay parent-resume child results differ from durable authority",
			ErrCurrentReadInvariant)
	}
	return nil
}

func readStoredParentResumeAuthority(ctx context.Context, tx *sql.Tx,
	event model.Event, work model.ReviewWork,
) (workDerivationAuthority, error) {
	authority, found, err := readWorkDerivationAuthority(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return workDerivationAuthority{}, fmt.Errorf("%w: replay parent-resume derivation: %v",
			ErrCurrentReadInvariant, err)
	}
	if !found {
		return workDerivationAuthority{}, fmt.Errorf("%w: replay parent-resume child derivation is unavailable",
			ErrCurrentReadInvariant)
	}
	derivation := authority.derivation
	if work.ChannelID() != derivation.ParentChannelID() || work.Ref() != derivation.Parent() ||
		work.Participants().ReviewerPeerID() != derivation.Child().HomePeerID() {
		return workDerivationAuthority{}, fmt.Errorf("%w: replay parent-resume parent scope differs",
			ErrCurrentReadInvariant)
	}
	if work.Version() != derivation.ParentVersion() || work.UpdatedBy() != derivation.ParentEventID() ||
		(work.State() != model.WorkActive && work.State() != model.WorkRework) {
		return workDerivationAuthority{}, fmt.Errorf("%w: replay parent-resume parent Work is no longer exact",
			ErrCurrentReadInvariant)
	}
	return authority, nil
}

func requireStoredParentResumeReviewer(peer model.PeerID, role model.CurrentRole,
	authority workDerivationAuthority,
) error {
	if role != model.CurrentReviewer || peer != authority.derivation.Child().HomePeerID() {
		return fmt.Errorf("%w: replay parent-resume is not local reviewer authority",
			ErrCurrentReadInvariant)
	}
	return nil
}

func requireStoredParentResumeWorkProjection(projected model.CurrentWork,
	work model.ReviewWork, authority workDerivationAuthority, role model.CurrentRole,
	brief model.CurrentBrief,
) error {
	projectedBrief, ok := projected.Brief()
	if !ok || projected.Ref() != work.Ref() || projected.Version() != authority.derivation.ParentVersion() ||
		projected.DeadlineUnixNano() != work.DeadlineUnixNano() || projected.LocalRole() != role ||
		projectedBrief.Content() != brief.Content() ||
		projectedBrief.DeadlineUnixNano() != brief.DeadlineUnixNano() ||
		!sameCurrentArtifactRoots(projectedBrief.ArtifactRefs(), brief.ArtifactRefs()) {
		return fmt.Errorf("%w: replay parent-resume Work brief differs from durable authority",
			ErrCurrentReadInvariant)
	}
	return nil
}

func sameCurrentChildResults(left, right []model.CurrentChildResult) bool {
	if len(left) != 1 || len(right) != 1 {
		return false
	}
	return left[0].Ordinal() == right[0].Ordinal() &&
		left[0].WorkRef() == right[0].WorkRef() &&
		left[0].State() == right[0].State() &&
		left[0].Version() == right[0].Version() &&
		left[0].Iteration() == right[0].Iteration() &&
		sameCurrentArtifactRoots(left[0].ArtifactRefs(), right[0].ArtifactRefs())
}

func deriveParentResumeCurrentActions(role model.CurrentRole,
	work model.ReviewWork,
) []model.OperationKind {
	if role != model.CurrentReviewer ||
		(work.State() != model.WorkActive && work.State() != model.WorkRework) {
		return currentResolutionActions()
	}
	return []model.OperationKind{model.OperationTeamworkDeliver, model.OperationResolveRetry}
}
