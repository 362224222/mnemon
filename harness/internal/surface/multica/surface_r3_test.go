package multica

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestClassifySurfaceActionSplitsNoDisplayAndActivation(t *testing.T) {
	none := ClassifySurfaceAction(SurfaceClassificationInput{
		EventRef:    "event-1",
		ResourceRef: "assignment/asg-1",
	})
	if none.Action != SurfaceActionNone {
		t.Fatalf("no surface action = %s, want %s", none.Action, SurfaceActionNone)
	}

	display := ClassifySurfaceAction(SurfaceClassificationInput{
		EventRef:         "event-2",
		ResourceRef:      "progress_digest/prog-1",
		VisibleSummary:   "支付回调已恢复，等待复核。",
		DisplayRequested: true,
	})
	if display.Action != SurfaceActionDisplayOnly || display.Role != SurfaceRoleDisplay {
		t.Fatalf("display classification = %+v", display)
	}

	activation := ClassifySurfaceAction(SurfaceClassificationInput{
		EventRef:            "event-3",
		ResourceRef:         "assignment/asg-2",
		ActivationRequested: true,
	})
	if activation.Action != SurfaceActionActivationNeeded || activation.Role != SurfaceRoleActivate {
		t.Fatalf("activation classification = %+v", activation)
	}

	invalidActivation := ClassifySurfaceAction(SurfaceClassificationInput{ActivationRequested: true})
	if invalidActivation.Action != SurfaceActionNone {
		t.Fatalf("activation without event_ref must not create surface action: %+v", invalidActivation)
	}
}

func TestBuildDisplayWritebackPlanIsDisplaySafe(t *testing.T) {
	plan, err := BuildDisplayWritebackPlan(DisplayWritebackRequest{
		IssueID: "issue-1",
		Refs: SurfaceRefs{
			EventRef:          "event-progress-1",
			ResourceRef:       "progress_digest/prog-1",
			ProjectionRef:     "projection-1",
			SourceArtifactRef: "multica.comment:comment-1",
		},
		Title:              "进展",
		StatusLabel:        "result",
		Summary:            "已确认退款风控误杀来自昨日规则变更。",
		EvidenceRefs:       []string{"ctx:风险登记", "artifact:规则 diff"},
		DesiredStatus:      StatusInProgress,
		AssignedToProvider: true,
		AssigneeAgentID:    "agent-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != "" {
		t.Fatalf("display lane must not move provider-assigned issue to active status: %+v", plan)
	}
	if plan.SkippedStatusReason == "" {
		t.Fatalf("unsafe status skip reason missing: %+v", plan)
	}
	if plan.Metadata[MulticaMetadataSurfaceRole] != string(SurfaceRoleDisplay) ||
		plan.Metadata[MulticaMetadataNoAutoDispatch] != "true" ||
		plan.Metadata[MulticaMetadataEventRef] != "event-progress-1" {
		t.Fatalf("display metadata mismatch: %+v", plan.Metadata)
	}
	if !strings.Contains(plan.CommentBody, "Mnemon 更新: 进展") ||
		!strings.Contains(plan.CommentBody, "已确认退款风控误杀") ||
		!strings.Contains(plan.CommentBody, "ctx:风险登记") {
		t.Fatalf("comment body is not a Chinese structured OA update:\n%s", plan.CommentBody)
	}
}

func TestBuildDisplayWritebackPlanAllowsSafeDoneStatus(t *testing.T) {
	plan, err := BuildDisplayWritebackPlan(DisplayWritebackRequest{
		IssueID:       "issue-1",
		Refs:          SurfaceRefs{EventRef: "event-result-1", ResourceRef: "progress_digest/prog-2"},
		Title:         "结果",
		Summary:       "支付回调失败已完成复核。",
		DesiredStatus: StatusDone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != StatusDone {
		t.Fatalf("safe done status = %q, want %q", plan.Status, StatusDone)
	}
}

func TestBuildActivationCarrierRequiresEventRef(t *testing.T) {
	if _, err := BuildActivationCarrier(ActivationCarrierRequest{
		IssueID:     "issue-1",
		ResourceRef: "assignment/asg-1",
	}); err == nil {
		t.Fatal("activation carrier without event_ref must fail")
	}

	carrier, err := BuildActivationCarrier(ActivationCarrierRequest{
		IssueID:      "issue-1",
		EventRef:     "event-assignment-1",
		ResourceRef:  "assignment/asg-1",
		CarrierTitle: "继续处理支付回调排查",
	})
	if err != nil {
		t.Fatal(err)
	}
	if carrier.Metadata[MulticaMetadataSurfaceRole] != string(SurfaceRoleActivate) ||
		carrier.Metadata[MulticaMetadataEventRef] != "event-assignment-1" {
		t.Fatalf("activation metadata mismatch: %+v", carrier.Metadata)
	}
}

func TestClassifyHumanInteractionKeepsCanonicalBoundary(t *testing.T) {
	comment := ClassifyHumanInteraction(HumanInteraction{
		Kind:      HumanInteractionComment,
		IssueID:   "issue-1",
		CommentID: "comment-1",
		Body:      "客服侧已经承诺今晚 20:00 前给出补偿名单。",
	})
	if comment.SurfaceRole != SurfaceRoleInput ||
		comment.ObservationType != "multica.comment.observed" ||
		comment.ExternalID != "multica-comment-comment-1" {
		t.Fatalf("comment decision mismatch: %+v", comment)
	}

	status := ClassifyHumanInteraction(HumanInteraction{
		Kind:    HumanInteractionStatusMove,
		IssueID: "issue-1",
		Status:  "审核中",
	})
	if status.RequestActivation || status.Drift || status.ObservationType != "multica.status_request.observed" {
		t.Fatalf("status move must be a request observation: %+v", status)
	}

	edit := ClassifyHumanInteraction(HumanInteraction{Kind: HumanInteractionManagedEdit})
	if !edit.Drift || edit.SurfaceRole != SurfaceRoleDrift {
		t.Fatalf("managed edit must become drift: %+v", edit)
	}

	tag := ClassifyHumanInteraction(HumanInteraction{Kind: HumanInteractionTag, EventRef: "event-1"})
	if !tag.RequestActivation || tag.SurfaceRole != SurfaceRoleActivate {
		t.Fatalf("tag with event_ref must request activation: %+v", tag)
	}

	externalTag := ClassifyHumanInteraction(HumanInteraction{Kind: HumanInteractionTag})
	if externalTag.RequestActivation || externalTag.ObservationType != "multica.tag_input.observed" {
		t.Fatalf("tag without event_ref must remain external input: %+v", externalTag)
	}
}

func TestProjectionLedgerDedupesByEventRoleAndTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projection-ledger.jsonl")
	ledger := NewFileProjectionLedger(path)
	record := ProjectionLedgerRecord{
		EventRef:      "event-1",
		ResourceRef:   "progress_digest/prog-1",
		ProjectionRef: "projection-1",
		SurfaceRole:   SurfaceRoleDisplay,
		TargetKind:    "comment",
		TargetID:      "comment-1",
		Status:        "reserved",
	}
	if _, ok, err := ledger.Reserve(record); err != nil || !ok {
		t.Fatalf("reserve failed ok=%v err=%v", ok, err)
	}
	if _, ok, err := NewFileProjectionLedger(path).Reserve(record); err != nil || ok {
		t.Fatalf("duplicate reserve ok=%v err=%v", ok, err)
	}
	record.Status = "written"
	if err := NewFileProjectionLedger(path).Record(record); err != nil {
		t.Fatal(err)
	}
	found, ok, err := NewFileProjectionLedger(path).Find("event-1", SurfaceRoleDisplay, ProjectionLedgerTarget{Kind: "comment", ID: "comment-1"})
	if err != nil || !ok {
		t.Fatalf("find failed ok=%v err=%v", ok, err)
	}
	if found.Status != "written" || found.ProjectionRef != "projection-1" {
		t.Fatalf("ledger record mismatch: %+v", found)
	}
}
