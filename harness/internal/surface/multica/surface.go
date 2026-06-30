package multica

import (
	"fmt"
	"strings"
)

type SurfaceRole string

const (
	SurfaceRoleDisplay  SurfaceRole = "display"
	SurfaceRoleActivate SurfaceRole = "activate"
	SurfaceRoleInput    SurfaceRole = "input"
	SurfaceRoleDrift    SurfaceRole = "drift"
)

type SurfaceAction string

const (
	SurfaceActionNone             SurfaceAction = "no_action"
	SurfaceActionDisplayOnly      SurfaceAction = "display_only"
	SurfaceActionActivationNeeded SurfaceAction = "activation_needed"
)

const (
	MulticaMetadataEventRef          = "mnemon.event_ref"
	MulticaMetadataResourceRef       = "mnemon.resource_ref"
	MulticaMetadataProjectionRef     = "mnemon.projection_ref"
	MulticaMetadataSourceArtifactRef = "mnemon.source_artifact_ref"
	MulticaMetadataSurfaceRole       = "mnemon.surface_role"
	MulticaMetadataNoAutoDispatch    = "mnemon.no_auto_dispatch"
	MulticaMetadataCaseID            = "mnemon.case_id"
	MulticaMetadataRound             = "mnemon.round"
	MulticaMetadataPoCID             = "mnemon.poc_id"
)

type SurfaceRefs struct {
	EventRef          string
	ResourceRef       string
	ProjectionRef     string
	SourceArtifactRef string
}

func (r SurfaceRefs) Metadata(role SurfaceRole) map[string]string {
	meta := map[string]string{}
	addSurfaceMetadata(meta, MulticaMetadataEventRef, r.EventRef)
	addSurfaceMetadata(meta, MulticaMetadataResourceRef, r.ResourceRef)
	addSurfaceMetadata(meta, MulticaMetadataProjectionRef, r.ProjectionRef)
	addSurfaceMetadata(meta, MulticaMetadataSourceArtifactRef, r.SourceArtifactRef)
	if role != "" {
		meta[MulticaMetadataSurfaceRole] = string(role)
	}
	return meta
}

func ParseSurfaceRefs(meta map[string]any) SurfaceRefs {
	if meta == nil {
		return SurfaceRefs{}
	}
	return SurfaceRefs{
		EventRef:          surfaceAnyString(meta[MulticaMetadataEventRef]),
		ResourceRef:       surfaceAnyString(meta[MulticaMetadataResourceRef]),
		ProjectionRef:     surfaceAnyString(meta[MulticaMetadataProjectionRef]),
		SourceArtifactRef: surfaceAnyString(meta[MulticaMetadataSourceArtifactRef]),
	}
}

func ParseListedSurfaceRefs(meta map[string]string) SurfaceRefs {
	if meta == nil {
		return SurfaceRefs{}
	}
	return SurfaceRefs{
		EventRef:          strings.TrimSpace(meta[MulticaMetadataEventRef]),
		ResourceRef:       strings.TrimSpace(meta[MulticaMetadataResourceRef]),
		ProjectionRef:     strings.TrimSpace(meta[MulticaMetadataProjectionRef]),
		SourceArtifactRef: strings.TrimSpace(meta[MulticaMetadataSourceArtifactRef]),
	}
}

type SurfaceClassificationInput struct {
	EventRef             string
	ResourceRef          string
	VisibleSummary       string
	VisibleStatus        string
	DisplayRequested     bool
	ActivationRequested  bool
	LocalActionRequired  bool
	HasMulticaControlRef bool
}

type SurfaceClassification struct {
	Action SurfaceAction
	Role   SurfaceRole
	Reason string
}

func ClassifySurfaceAction(in SurfaceClassificationInput) SurfaceClassification {
	if in.ActivationRequested || in.LocalActionRequired {
		if strings.TrimSpace(in.EventRef) == "" {
			return SurfaceClassification{Action: SurfaceActionNone, Reason: "activation requires event_ref"}
		}
		return SurfaceClassification{Action: SurfaceActionActivationNeeded, Role: SurfaceRoleActivate, Reason: "accepted event requires explicit activation"}
	}
	if in.DisplayRequested ||
		strings.TrimSpace(in.VisibleSummary) != "" ||
		strings.TrimSpace(in.VisibleStatus) != "" ||
		in.HasMulticaControlRef {
		return SurfaceClassification{Action: SurfaceActionDisplayOnly, Role: SurfaceRoleDisplay, Reason: "accepted event has display surface value"}
	}
	return SurfaceClassification{Action: SurfaceActionNone, Reason: "no Multica surface action required"}
}

type DisplayWritebackRequest struct {
	IssueID            string
	Refs               SurfaceRefs
	Title              string
	StatusLabel        string
	Summary            string
	Result             string
	EvidenceRefs       []string
	ArtifactRefs       []string
	DesiredStatus      string
	AssigneeAgentID    string
	AssignedToProvider bool
}

type DisplayWritebackPlan struct {
	IssueID              string
	CommentBody          string
	Metadata             map[string]string
	Status               string
	SkippedStatusReason  string
	NoAutoDispatch       bool
	SourceArtifactLinked bool
}

func BuildDisplayWritebackPlan(req DisplayWritebackRequest) (DisplayWritebackPlan, error) {
	issueID := strings.TrimSpace(req.IssueID)
	if issueID == "" {
		return DisplayWritebackPlan{}, fmt.Errorf("Multica issue id is required")
	}
	if strings.TrimSpace(req.Refs.EventRef) == "" {
		return DisplayWritebackPlan{}, fmt.Errorf("event_ref is required for display writeback")
	}
	meta := req.Refs.Metadata(SurfaceRoleDisplay)
	meta[MulticaMetadataNoAutoDispatch] = "true"
	plan := DisplayWritebackPlan{
		IssueID:              issueID,
		CommentBody:          FormatDisplayWritebackComment(req),
		Metadata:             meta,
		NoAutoDispatch:       true,
		SourceArtifactLinked: strings.TrimSpace(req.Refs.SourceArtifactRef) != "",
	}
	status := CanonicalIssueStatus(req.DesiredStatus)
	if status == "" {
		return plan, nil
	}
	if !DisplayStatusWriteAllowed(DisplayStatusWriteInput{
		Status:             status,
		AssigneeAgentID:    req.AssigneeAgentID,
		AssignedToProvider: req.AssignedToProvider,
	}) {
		plan.SkippedStatusReason = "display status would risk provider dispatch"
		return plan, nil
	}
	plan.Status = status
	return plan, nil
}

type DisplayStatusWriteInput struct {
	Status             string
	AssigneeAgentID    string
	AssignedToProvider bool
}

func DisplayStatusWriteAllowed(in DisplayStatusWriteInput) bool {
	status := CanonicalIssueStatus(in.Status)
	if status == "" {
		return false
	}
	if !in.AssignedToProvider && strings.TrimSpace(in.AssigneeAgentID) == "" {
		return true
	}
	switch status {
	case StatusBacklog, StatusDone, StatusCancelled:
		return true
	default:
		return false
	}
}

func FormatDisplayWritebackComment(req DisplayWritebackRequest) string {
	var b strings.Builder
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = "进展"
	}
	b.WriteString("Mnemon 更新: ")
	b.WriteString(title)
	b.WriteString("\n\n")
	writeDisplaySection(&b, "状态", req.StatusLabel)
	writeDisplaySection(&b, "摘要", req.Summary)
	writeDisplaySection(&b, "结果", req.Result)
	writeDisplayList(&b, "证据", req.EvidenceRefs)
	writeDisplayList(&b, "产物", req.ArtifactRefs)
	writeDisplaySection(&b, "事件引用", req.Refs.EventRef)
	return strings.TrimSpace(b.String())
}

type ActivationCarrierRequest struct {
	IssueID       string
	EventRef      string
	ResourceRef   string
	CarrierTitle  string
	TargetAgentID string
}

type ActivationCarrier struct {
	IssueID  string
	Title    string
	Metadata map[string]string
}

func BuildActivationCarrier(req ActivationCarrierRequest) (ActivationCarrier, error) {
	issueID := strings.TrimSpace(req.IssueID)
	eventRef := strings.TrimSpace(req.EventRef)
	resourceRef := strings.TrimSpace(req.ResourceRef)
	if issueID == "" {
		return ActivationCarrier{}, fmt.Errorf("Multica issue id is required")
	}
	if eventRef == "" {
		return ActivationCarrier{}, fmt.Errorf("event_ref is required for activation carrier")
	}
	if resourceRef == "" {
		return ActivationCarrier{}, fmt.Errorf("resource_ref is required for activation carrier")
	}
	title := strings.TrimSpace(req.CarrierTitle)
	if title == "" {
		title = "Mnemon activation"
	}
	meta := SurfaceRefs{EventRef: eventRef, ResourceRef: resourceRef}.Metadata(SurfaceRoleActivate)
	addSurfaceMetadata(meta, "mnemon.target_agent_id", req.TargetAgentID)
	return ActivationCarrier{IssueID: issueID, Title: title, Metadata: meta}, nil
}

type HumanInteractionKind string

const (
	HumanInteractionComment     HumanInteractionKind = "comment"
	HumanInteractionStatusMove  HumanInteractionKind = "status_move"
	HumanInteractionManagedEdit HumanInteractionKind = "managed_edit"
	HumanInteractionTag         HumanInteractionKind = "tag"
)

type HumanInteraction struct {
	Kind      HumanInteractionKind
	IssueID   string
	CommentID string
	EventRef  string
	Body      string
	Status    string
}

type InteractionDecision struct {
	SurfaceRole       SurfaceRole
	ObservationType   string
	ExternalID        string
	RequestActivation bool
	Drift             bool
	Reason            string
}

func ClassifyHumanInteraction(in HumanInteraction) InteractionDecision {
	switch in.Kind {
	case HumanInteractionComment:
		return InteractionDecision{
			SurfaceRole:     SurfaceRoleInput,
			ObservationType: "multica.comment.observed",
			ExternalID:      "multica-comment-" + strings.TrimSpace(in.CommentID),
			Reason:          "human comment imports as observation",
		}
	case HumanInteractionStatusMove:
		return InteractionDecision{
			SurfaceRole:     SurfaceRoleInput,
			ObservationType: "multica.status_request.observed",
			ExternalID:      "multica-status-" + strings.TrimSpace(in.IssueID) + "-" + CanonicalIssueStatus(in.Status),
			Reason:          "human status move is a request, not canonical state",
		}
	case HumanInteractionManagedEdit:
		return InteractionDecision{SurfaceRole: SurfaceRoleDrift, Drift: true, Reason: "managed block was edited outside Mnemon writeback"}
	case HumanInteractionTag:
		if strings.TrimSpace(in.EventRef) != "" {
			return InteractionDecision{SurfaceRole: SurfaceRoleActivate, RequestActivation: true, Reason: "tag carries event_ref"}
		}
		return InteractionDecision{SurfaceRole: SurfaceRoleInput, ObservationType: "multica.tag_input.observed", Reason: "tag without event_ref is external input"}
	default:
		return InteractionDecision{SurfaceRole: SurfaceRoleInput, ObservationType: "multica.input.observed", Reason: "unknown human interaction imports as observation"}
	}
}

func writeDisplaySection(b *strings.Builder, heading, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	b.WriteString("## ")
	b.WriteString(heading)
	b.WriteString("\n\n")
	b.WriteString(value)
	b.WriteString("\n\n")
}

func writeDisplayList(b *strings.Builder, heading string, values []string) {
	cleaned := cleanSurfaceStrings(values)
	if len(cleaned) == 0 {
		return
	}
	b.WriteString("## ")
	b.WriteString(heading)
	b.WriteString("\n\n")
	for _, value := range cleaned {
		b.WriteString("- ")
		b.WriteString(value)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
}

func cleanSurfaceStrings(values []string) []string {
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func addSurfaceMetadata(meta map[string]string, key, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key != "" && value != "" {
		meta[key] = value
	}
}

func surfaceAnyString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return ""
	}
}
