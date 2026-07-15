package teamwork

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestNewDerivationGroupFreezesCanonicalScope(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	parent := newTestWorkInChannel(t, "channel-alpha", "work-parent", "peer-a", model.WorkActive, 2, 1, deadline)
	children := []DerivationChildScope{
		newChildScope(t, 2, "channel-beta", "work-child-c", parent.Participants().ReviewerPeerID()),
		newChildScope(t, 0, "channel-beta", "work-child-a", parent.Participants().ReviewerPeerID()),
		newChildScope(t, 1, "channel-beta", "work-child-b", parent.Participants().ReviewerPeerID()),
	}
	operation := parseOperation(t, "operation-derived")

	group, err := NewDerivationGroup(DerivationGroupSpec{operation, parent, parent.UpdatedBy(), children})
	if err != nil {
		t.Fatalf("NewDerivationGroup() error = %v", err)
	}
	if group.OperationID() != operation || group.ParentChannelID() != parent.ChannelID() || group.ParentWorkRef() != parent.Ref() || group.ParentVersion() != 2 || group.ParentState() != model.WorkActive || group.ParentSourceEvent() != parent.UpdatedBy() {
		t.Fatalf("group lost frozen parent scope")
	}
	for index, child := range group.Children() {
		if child.Ordinal() != uint8(index) || child.WorkRef().WorkID().String() != fmt.Sprintf("work-child-%c", 'a'+index) {
			t.Errorf("child[%d] not in frozen ordinal order: %d/%s", index, child.Ordinal(), child.WorkRef().WorkID().String())
		}
	}

	childrenCopy := group.Children()
	childrenCopy[0] = DerivationChildScope{}
	if group.Children()[0].WorkRef().IsZero() {
		t.Fatalf("Children() exposed mutable group storage")
	}
}

func TestNewDerivationGroupRejectsInvalidSizeOrdinalAndScope(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	parent := newTestWorkInChannel(t, "channel-alpha", "work-parent-invalid", "peer-a", model.WorkRework, 4, 2, deadline)
	home := parent.Participants().ReviewerPeerID()
	valid := []DerivationChildScope{
		newChildScope(t, 0, "channel-beta", "work-child-0", home),
		newChildScope(t, 1, "channel-beta", "work-child-1", home),
	}
	operation := parseOperation(t, "operation-invalid")
	otherEvent, _ := model.ParseEventID("event-other")

	eight := make([]DerivationChildScope, model.MaxChildWorks+1)
	for index := 0; index < model.MaxChildWorks; index++ {
		eight[index] = newChildScope(t, uint8(index), "channel-beta", fmt.Sprintf("work-eight-%d", index), home)
	}
	tests := []struct {
		name     string
		parent   model.ReviewWork
		source   model.EventID
		children []DerivationChildScope
		wantErr  error
	}{
		{"no children", parent, parent.UpdatedBy(), nil, ErrInvalidDerivation},
		{"eight children", parent, parent.UpdatedBy(), eight, ErrInvalidDerivation},
		{"ordinal gap", parent, parent.UpdatedBy(), []DerivationChildScope{valid[0], newChildScope(t, 2, "channel-beta", "work-gap", home)}, ErrInvalidDerivation},
		{"duplicate ordinal", parent, parent.UpdatedBy(), []DerivationChildScope{valid[0], newChildScope(t, 0, "channel-beta", "work-dupe-ordinal", home)}, ErrInvalidDerivation},
		{"duplicate Work", parent, parent.UpdatedBy(), []DerivationChildScope{valid[0], {ordinal: 1, channelID: valid[0].channelID, workRef: valid[0].workRef}}, ErrInvalidDerivation},
		{"mixed child Channel", parent, parent.UpdatedBy(), []DerivationChildScope{valid[0], newChildScope(t, 1, "channel-gamma", "work-mixed", home)}, ErrDerivationScope},
		{"child not homed by parent reviewer", parent, parent.UpdatedBy(), []DerivationChildScope{valid[0], newChildScope(t, 1, "channel-beta", "work-wrong-home", parsePeer(t, "peer-other"))}, ErrDerivationScope},
		{"wrong parent source Event", parent, otherEvent, valid, ErrDerivationScope},
		{"parent not reviewer-active", newTestWorkInChannel(t, "channel-alpha", "work-parent-delivered", "peer-a", model.WorkDelivered, 5, 2, deadline), parent.UpdatedBy(), valid, ErrInvalidDerivation},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewDerivationGroup(DerivationGroupSpec{operation, test.parent, test.source, test.children})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewDerivationGroup() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestPlanDerivationDispositionIsMutuallyExclusive(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	parent := newTestWorkInChannel(t, "channel-alpha", "work-parent-disposition", "peer-a", model.WorkActive, 2, 1, deadline)
	childHome := parent.Participants().ReviewerPeerID()
	children := []model.ReviewWork{
		newTestWorkInChannel(t, "channel-beta", "work-result-a", childHome.String(), model.WorkClosed, 4, 1, deadline),
		newTestWorkInChannel(t, "channel-beta", "work-result-b", childHome.String(), model.WorkDeclined, 2, 1, deadline),
		newTestWorkInChannel(t, "channel-beta", "work-result-c", childHome.String(), model.WorkCancelled, 3, 1, deadline),
		newTestWorkInChannel(t, "channel-beta", "work-result-d", childHome.String(), model.WorkExpired, 3, 1, deadline),
	}
	scopes := make([]DerivationChildScope, len(children))
	for index, child := range children {
		scopes[index], _ = NewDerivationChildScope(uint8(index), child.ChannelID(), child.Ref())
	}
	group, err := NewDerivationGroup(DerivationGroupSpec{parseOperation(t, "operation-disposition"), parent, parent.UpdatedBy(), scopes})
	if err != nil {
		t.Fatalf("NewDerivationGroup() error = %v", err)
	}

	shuffled := []model.ReviewWork{children[3], children[1], children[0], children[2]}
	resume, err := PlanDerivationDisposition(group, parent, shuffled)
	if err != nil {
		t.Fatalf("PlanDerivationDisposition(resume) error = %v", err)
	}
	assertDisposition(t, resume, DerivationResume, true, true)
	for index, result := range resume.ChildResults() {
		if result.Ordinal() != uint8(index) || result.WorkRef() != children[index].Ref() || !result.State().Terminal() {
			t.Errorf("result[%d] is not canonical terminal child", index)
		}
	}

	pendingChildren := append([]model.ReviewWork(nil), children...)
	pendingChildren[1] = newTestWorkInChannel(t, "channel-beta", "work-result-b", childHome.String(), model.WorkDelivered, 2, 1, deadline)
	changedParent := newTestWorkInChannel(t, "channel-alpha", "work-parent-disposition", "peer-a", model.WorkActive, 3, 1, deadline)
	pending, err := PlanDerivationDisposition(group, changedParent, pendingChildren)
	if err != nil {
		t.Fatalf("PlanDerivationDisposition(pending) error = %v", err)
	}
	assertDisposition(t, pending, DerivationPending, false, false)

	staleVersion, err := PlanDerivationDisposition(group, changedParent, children)
	if err != nil {
		t.Fatalf("PlanDerivationDisposition(stale version) error = %v", err)
	}
	assertDisposition(t, staleVersion, DerivationParentStale, false, true)
	if staleVersion.FrozenParentVersion() != 2 || staleVersion.LatestParentVersion() != 3 {
		t.Fatalf("stale plan lost frozen/latest versions")
	}

	terminalParent := newTestWorkInChannel(t, "channel-alpha", "work-parent-disposition", "peer-a", model.WorkCancelled, 2, 1, deadline)
	staleState, err := PlanDerivationDisposition(group, terminalParent, children)
	if err != nil {
		t.Fatalf("PlanDerivationDisposition(stale state) error = %v", err)
	}
	assertDisposition(t, staleState, DerivationParentStale, false, true)

	results := resume.ChildResults()
	results[0] = DerivationChildResult{}
	if resume.ChildResults()[0].WorkRef().IsZero() {
		t.Fatalf("ChildResults() exposed mutable plan storage")
	}
}

func TestPlanDerivationDispositionRejectsScopeDrift(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	parent := newTestWorkInChannel(t, "channel-alpha", "work-parent-scope", "peer-a", model.WorkActive, 2, 1, deadline)
	child := newTestWorkInChannel(t, "channel-beta", "work-child-scope", parent.Participants().ReviewerPeerID().String(), model.WorkClosed, 3, 1, deadline)
	scope, _ := NewDerivationChildScope(0, child.ChannelID(), child.Ref())
	group, err := NewDerivationGroup(DerivationGroupSpec{parseOperation(t, "operation-scope"), parent, parent.UpdatedBy(), []DerivationChildScope{scope}})
	if err != nil {
		t.Fatalf("NewDerivationGroup() error = %v", err)
	}

	wrongParent := newTestWorkInChannel(t, "channel-other", "work-parent-scope", "peer-a", model.WorkActive, 2, 1, deadline)
	if _, err := PlanDerivationDisposition(group, wrongParent, []model.ReviewWork{child}); !errors.Is(err, ErrDerivationScope) {
		t.Fatalf("parent scope drift error = %v", err)
	}
	wrongChild := newTestWorkInChannel(t, "channel-other", "work-child-scope", parent.Participants().ReviewerPeerID().String(), model.WorkClosed, 3, 1, deadline)
	if _, err := PlanDerivationDisposition(group, parent, []model.ReviewWork{wrongChild}); !errors.Is(err, ErrDerivationScope) {
		t.Fatalf("child scope drift error = %v", err)
	}
	if _, err := PlanDerivationDisposition(group, parent, nil); !errors.Is(err, ErrDerivationScope) {
		t.Fatalf("missing child error = %v", err)
	}
}

func newChildScope(t *testing.T, ordinal uint8, channelName, workName string, home model.PeerID) DerivationChildScope {
	t.Helper()
	workID, err := model.ParseWorkID(workName)
	if err != nil {
		t.Fatalf("ParseWorkID(%q): %v", workName, err)
	}
	ref, err := model.NewWorkRef(home, workID)
	if err != nil {
		t.Fatalf("NewWorkRef(): %v", err)
	}
	scope, err := NewDerivationChildScope(ordinal, parseChannel(t, channelName), ref)
	if err != nil {
		t.Fatalf("NewDerivationChildScope(): %v", err)
	}
	return scope
}

func parseOperation(t *testing.T, value string) model.OperationID {
	t.Helper()
	operation, err := model.ParseOperationID(value)
	if err != nil {
		t.Fatalf("ParseOperationID(%q): %v", value, err)
	}
	return operation
}

func assertDisposition(t *testing.T, plan DerivationDispositionPlan, want DerivationDisposition, wake, complete bool) {
	t.Helper()
	if plan.Disposition() != want || plan.ShouldWake() != wake || plan.Complete() != complete {
		t.Fatalf("disposition = %s wake=%t complete=%t, want %s/%t/%t", plan.Disposition(), plan.ShouldWake(), plan.Complete(), want, wake, complete)
	}
}
