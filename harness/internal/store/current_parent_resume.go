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
	group, parent, err := readParentResumeGroupAuthority(ctx, tx, handling, event)
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
	if role != model.CurrentReviewer || node.PeerID() != group.childHome {
		return freshCurrentAuthority{}, fmt.Errorf("%w: parent-resume is not local reviewer authority",
			ErrCurrentReadInvariant)
	}
	brief, err := readCurrentWorkBrief(ctx, tx, parent)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	children, last, err := readParentResumeChildResults(ctx, tx, group, budget)
	if err != nil {
		return freshCurrentAuthority{}, err
	}
	if last.id != event.ID() {
		return freshCurrentAuthority{}, fmt.Errorf("%w: parent-resume Handling is not bound to last terminal child Event",
			ErrCurrentReadInvariant)
	}
	return freshCurrentAuthority{event: event, work: parent, role: role, brief: brief,
		childResults: children}, nil
}

func readParentResumeGroupAuthority(ctx context.Context, tx *sql.Tx, handling model.Handling,
	event model.Event,
) (workDerivationGroup, model.ReviewWork, error) {
	group, found, err := readWorkDerivationGroup(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return workDerivationGroup{}, model.ReviewWork{}, fmt.Errorf("%w: parent-resume group: %v",
			ErrCurrentReadInvariant, err)
	}
	if !found {
		return workDerivationGroup{}, model.ReviewWork{}, fmt.Errorf("%w: parent-resume child derivation is unavailable",
			ErrCurrentReadInvariant)
	}
	if err := requireParentResumeHandlingID(handling, group); err != nil {
		return workDerivationGroup{}, model.ReviewWork{}, err
	}
	parent, err := readReviewWork(ctx, tx, group.parent)
	if err != nil {
		return workDerivationGroup{}, model.ReviewWork{}, fmt.Errorf("%w: parent-resume action Work: %v",
			ErrCurrentReadInvariant, err)
	}
	if err := requireExactParentResumeWork(parent, group); err != nil {
		return workDerivationGroup{}, model.ReviewWork{}, err
	}
	return group, parent, nil
}

func requireParentResumeHandlingID(handling model.Handling, group workDerivationGroup) error {
	resumeID, err := deterministicDerivationHandlingID(group.operation)
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

func requireExactParentResumeWork(parent model.ReviewWork, group workDerivationGroup) error {
	if parent.ChannelID() != group.parentChannel || parent.Ref() != group.parent ||
		parent.Participants().ReviewerPeerID() != group.childHome ||
		parent.Version() != group.parentVersion || parent.UpdatedBy() != group.parentEvent ||
		(parent.State() != model.WorkActive && parent.State() != model.WorkRework) {
		return fmt.Errorf("%w: parent-resume parent Work is no longer exact",
			ErrCurrentReadStale)
	}
	return nil
}

func readParentResumeChildResults(ctx context.Context, tx *sql.Tx, group workDerivationGroup,
	budget model.HandlingBudget,
) ([]model.CurrentChildResult, terminalChildEvent, error) {
	children, err := readParentResumeTerminalChildren(ctx, tx, group)
	if err != nil {
		return nil, terminalChildEvent{}, err
	}
	last, _, err := readTerminalChildEvents(ctx, tx, group, children)
	if err != nil {
		return nil, terminalChildEvent{}, fmt.Errorf("%w: parent-resume terminal child set: %v",
			ErrCurrentReadInvariant, err)
	}
	results, err := buildParentResumeChildResults(ctx, tx, group, children, budget)
	if err != nil {
		return nil, terminalChildEvent{}, err
	}
	return results, last, nil
}

func readParentResumeTerminalChildren(ctx context.Context, tx *sql.Tx,
	group workDerivationGroup,
) ([]model.ReviewWork, error) {
	children := make([]model.ReviewWork, len(group.rows))
	for index, row := range group.rows {
		work, err := readReviewWork(ctx, tx, row.Child())
		if err != nil {
			return nil, fmt.Errorf("%w: parent-resume child ordinal %d: %v",
				ErrCurrentReadInvariant, row.ChildOrdinal(), err)
		}
		if work.ChannelID() != row.ChildChannelID() || work.Ref() != row.Child() ||
			!work.State().Terminal() {
			return nil, fmt.Errorf("%w: parent-resume child ordinal %d is not terminal",
				ErrCurrentReadInvariant, row.ChildOrdinal())
		}
		children[index] = work
	}
	return children, nil
}

func buildParentResumeChildResults(ctx context.Context, tx *sql.Tx, group workDerivationGroup,
	children []model.ReviewWork, budget model.HandlingBudget,
) ([]model.CurrentChildResult, error) {
	results := make([]model.CurrentChildResult, len(children))
	totalArtifacts := 0
	for index, child := range children {
		refs, err := readParentResumeChildArtifactRefs(ctx, tx, group, child)
		if err != nil {
			return nil, err
		}
		totalArtifacts += len(refs)
		if totalArtifacts > budget.Spec().MaxCurrentArtifactRefs {
			return nil, fmt.Errorf("%w: parent-resume child Artifact refs exceed Profile budget",
				ErrCurrentReadTooLarge)
		}
		result, err := model.NewCurrentChildResult(model.CurrentChildResultSpec{
			Ordinal: uint8(index), WorkRef: child.Ref(), State: child.State(),
			Version: child.Version(), Iteration: child.Iteration(), ArtifactRefs: refs,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: parent-resume child result: %v",
				ErrCurrentReadInvariant, err)
		}
		results[index] = result
	}
	return results, nil
}

func readParentResumeChildArtifactRefs(ctx context.Context, tx *sql.Tx,
	group workDerivationGroup, child model.ReviewWork,
) ([]model.CurrentArtifactRef, error) {
	if child.State() != model.WorkClosed {
		return nil, nil
	}
	sequence, err := readEventOriginSequence(ctx, tx, child.UpdatedBy())
	if err != nil {
		return nil, err
	}
	roots, err := readClosedChildResultArtifacts(ctx, tx, group, child, child.UpdatedBy(), sequence)
	if err != nil {
		return nil, fmt.Errorf("%w: parent-resume child result Artifacts: %v",
			ErrCurrentReadInvariant, err)
	}
	refs := make([]model.CurrentArtifactRef, len(roots))
	for index, root := range roots {
		ref, err := model.NewCurrentArtifactRef(root)
		if err != nil {
			return nil, fmt.Errorf("%w: parent-resume child result root: %v",
				ErrCurrentReadInvariant, err)
		}
		refs[index] = ref
	}
	return refs, nil
}

func readEventOriginSequence(ctx context.Context, q rowQuerier, event model.EventID) (uint64, error) {
	var sequence uint64
	if err := q.QueryRowContext(ctx, "SELECT origin_seq FROM events WHERE event_id=?",
		event.String()).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("%w: parent-resume terminal Event sequence: %v",
			ErrCurrentReadInvariant, err)
	}
	return sequence, nil
}

func validateStoredParentResumeCurrentWork(ctx context.Context, tx *sql.Tx,
	receipt model.CurrentReadReceipt, event model.Event, work model.ReviewWork,
	budget model.HandlingBudget,
) error {
	group, err := readStoredParentResumeGroup(ctx, tx, event, work)
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
	if err := requireStoredParentResumeReviewer(node.PeerID(), role, group); err != nil {
		return err
	}
	brief, err := readCurrentWorkBrief(ctx, tx, work)
	if err != nil {
		return err
	}
	if err := requireStoredParentResumeWorkProjection(receipt.Projection().ActionWork(),
		work, group, role, brief); err != nil {
		return err
	}
	children, last, err := readParentResumeChildResults(ctx, tx, group, budget)
	if err != nil {
		return err
	}
	if last.id != event.ID() || !sameCurrentChildResults(children, receipt.Projection().ChildResults()) {
		return fmt.Errorf("%w: replay parent-resume child results differ from durable authority",
			ErrCurrentReadInvariant)
	}
	return nil
}

func readStoredParentResumeGroup(ctx context.Context, tx *sql.Tx,
	event model.Event, work model.ReviewWork,
) (workDerivationGroup, error) {
	group, found, err := readWorkDerivationGroup(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return workDerivationGroup{}, fmt.Errorf("%w: replay parent-resume group: %v",
			ErrCurrentReadInvariant, err)
	}
	if !found || work.ChannelID() != group.parentChannel || work.Ref() != group.parent ||
		work.Participants().ReviewerPeerID() != group.childHome {
		return workDerivationGroup{}, fmt.Errorf("%w: replay parent-resume parent scope differs",
			ErrCurrentReadInvariant)
	}
	if work.Version() != group.parentVersion ||
		(work.State() != model.WorkActive && work.State() != model.WorkRework) {
		return workDerivationGroup{}, fmt.Errorf("%w: replay parent-resume parent Work is no longer exact",
			ErrCurrentReadInvariant)
	}
	return group, nil
}

func requireStoredParentResumeReviewer(peer model.PeerID, role model.CurrentRole,
	group workDerivationGroup,
) error {
	if role != model.CurrentReviewer || peer != group.childHome {
		return fmt.Errorf("%w: replay parent-resume is not local reviewer authority",
			ErrCurrentReadInvariant)
	}
	return nil
}

func requireStoredParentResumeWorkProjection(projected model.CurrentWork,
	work model.ReviewWork, group workDerivationGroup, role model.CurrentRole,
	brief model.CurrentBrief,
) error {
	projectedBrief, ok := projected.Brief()
	if !ok || projected.Ref() != work.Ref() || projected.Version() != group.parentVersion ||
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
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Ordinal() != right[index].Ordinal() ||
			left[index].WorkRef() != right[index].WorkRef() ||
			left[index].State() != right[index].State() ||
			left[index].Version() != right[index].Version() ||
			left[index].Iteration() != right[index].Iteration() ||
			!sameCurrentArtifactRoots(left[index].ArtifactRefs(), right[index].ArtifactRefs()) {
			return false
		}
	}
	return true
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
