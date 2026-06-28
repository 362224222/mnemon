package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	pview "github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

func (s *runtimeRPCState) writeMulticaHubArtifacts(ctx context.Context, cli driver.MulticaCLI, client *access.Client, rootIssue driver.MulticaIssue, result *runtimeImportResult) {
	if result == nil {
		return
	}
	if !runtimeMulticaHubWriteEnabled(s.Env) {
		result.HubWriteStatus = "skipped"
		return
	}
	if !strings.EqualFold(result.HubBackend, driver.MulticaHubBackend) || result.HubKind != driver.MulticaHubKindSession {
		result.HubWriteStatus = "skipped"
		return
	}
	if client == nil {
		result.HubWriteStatus = "failed"
		result.HubWriteErr = fmt.Errorf("mnemond client is required")
		return
	}
	proj, err := client.PullPresentationView(contract.ActorID(result.Principal), contract.Subscription{
		Actor: contract.ActorID(result.Principal),
		Refs: []contract.ResourceRef{
			{Kind: "assignment", ID: "project"},
			{Kind: "progress_digest", ID: "project"},
		},
	})
	if err != nil {
		result.HubWriteStatus = "failed"
		result.HubWriteErr = fmt.Errorf("pull teamwork view for Multica hub write: %w", err)
		return
	}
	reg, ok, err := runtimeMulticaRegistry(s.Env, s.CWD)
	if err != nil {
		result.HubWriteStatus = "failed"
		result.HubWriteErr = err
		return
	}
	if !ok {
		result.HubWriteStatus = "failed"
		result.HubWriteErr = fmt.Errorf("Multica registry is required for assignment routing")
		return
	}
	ledger := driver.NewFileMulticaHubLedger(driver.MulticaHubLedgerPath(s.CWD, envValue(s.Env, "MNEMON_MULTICA_HUB_LEDGER")))
	if err := s.writeAssignmentMailboxes(ctx, cli, ledger, reg, proj, rootIssue, result); err != nil {
		result.HubWriteStatus = "failed"
		result.HubWriteErr = err
		return
	}
	if err := s.writeProgressComments(ctx, cli, ledger, proj, result); err != nil {
		result.HubWriteStatus = "failed"
		result.HubWriteErr = err
		return
	}
	switch {
	case result.HubChildIssues > 0 && result.HubFeedbackComments > 0:
		result.HubWriteStatus = "updated"
	case result.HubChildIssues > 0:
		result.HubWriteStatus = "created"
	case result.HubFeedbackComments > 0:
		result.HubWriteStatus = "commented"
	default:
		result.HubWriteStatus = "noop"
	}
}

func (s *runtimeRPCState) writeAssignmentMailboxes(ctx context.Context, cli driver.MulticaCLI, ledger *driver.FileMulticaHubLedger, reg driver.MulticaRegistry, proj pview.View, rootIssue driver.MulticaIssue, result *runtimeImportResult) error {
	for _, assignment := range hubViewItems(proj, "assignment") {
		item := runtimeAssignmentItem(assignment)
		if item.ID == "" || item.Assignee == "" {
			continue
		}
		participant, ok := multicaParticipantForPrincipal(reg, item.Assignee)
		if !ok || strings.TrimSpace(participant.AgentID) == "" {
			return fmt.Errorf("no Multica agent mapping for assignment assignee %q", item.Assignee)
		}
		fingerprint := driver.MulticaAssignmentFingerprint(driver.MulticaAssignmentFingerprintInput{
			AssignmentID:     item.ID,
			Assignee:         item.Assignee,
			Scope:            item.Scope,
			ExpectedWork:     item.ExpectedWork,
			ExpectedFeedback: item.ExpectedFeedback,
			SignalRef:        item.SignalRef,
			ContextRefs:      item.ContextRefs,
			EvidenceRefs:     item.EvidenceRefs,
			CorrelationID:    result.CorrelationID,
		})
		source := driver.MulticaHubLedgerSource{
			SessionID:             result.SessionID,
			CorrelationID:         result.CorrelationID,
			EventID:               item.EventID,
			AssignmentID:          item.ID,
			AssignmentFingerprint: fingerprint,
			Principal:             item.Assignee,
			ProjectionKind:        "assignment",
		}
		if _, ok, err := ledger.Find(driver.MulticaHubKindAssignmentMailbox, source); err != nil {
			return err
		} else if ok {
			continue
		}
		if existing, ok, err := findExistingMulticaAssignmentIssue(ctx, cli, result.RootIssueID, source); err != nil {
			return err
		} else if ok {
			if err := ledger.Record(driver.MulticaHubLedgerRecord{
				Kind:   driver.MulticaHubKindAssignmentMailbox,
				Source: source,
				Target: driver.MulticaHubLedgerTarget{
					RootIssueID:  result.RootIssueID,
					ChildIssueID: existing.ID,
					Status:       "existing",
				},
			}); err != nil {
				return err
			}
			continue
		}
		child, err := cli.CreateIssue(ctx, driver.MulticaCreateIssueRequest{
			Title:       assignmentMailboxTitle(item),
			Description: assignmentMailboxDescription(item, result),
			ParentID:    result.RootIssueID,
			Status:      "todo",
			Priority:    "medium",
		})
		if err != nil {
			return err
		}
		meta := driver.MulticaHubMetadata{
			SchemaVersion:         "1",
			HubBackend:            driver.MulticaHubBackend,
			Kind:                  driver.MulticaHubKindAssignmentMailbox,
			SessionID:             result.SessionID,
			CorrelationID:         result.CorrelationID,
			EventID:               item.EventID,
			EventType:             "assignment.accepted",
			EventPhase:            string(eventmodel.PhaseAccepted),
			AssignmentID:          item.ID,
			AssignmentFingerprint: fingerprint,
			Principal:             item.Assignee,
			SourceIssueID:         rootIssue.ID,
			RootIssueID:           result.RootIssueID,
			ProjectionOwner:       result.Principal,
			MulticaAgentID:        participant.AgentID,
			ProjectedAt:           s.now().UTC().Format(time.RFC3339),
		}
		if err := cli.SetIssueMetadataMap(ctx, child.ID, meta.Map()); err != nil {
			return err
		}
		if _, err := cli.AssignIssue(ctx, child.ID, participant.AgentID); err != nil {
			return err
		}
		markIssueInProgress(ctx, cli, child.ID)
		if err := ledger.Record(driver.MulticaHubLedgerRecord{
			Kind:   driver.MulticaHubKindAssignmentMailbox,
			Source: source,
			Target: driver.MulticaHubLedgerTarget{
				RootIssueID:  result.RootIssueID,
				ChildIssueID: child.ID,
				Status:       "created",
			},
		}); err != nil {
			return err
		}
		result.HubChildIssues++
	}
	return nil
}

func (s *runtimeRPCState) writeProgressComments(ctx context.Context, cli driver.MulticaCLI, ledger *driver.FileMulticaHubLedger, proj pview.View, result *runtimeImportResult) error {
	for _, progress := range hubViewItems(proj, "progress_digest") {
		item := runtimeProgressItem(progress)
		if item.ID == "" || item.AssignmentRef == "" {
			continue
		}
		source := driver.MulticaHubLedgerSource{
			SessionID:      result.SessionID,
			CorrelationID:  result.CorrelationID,
			EventID:        item.EventID,
			AssignmentID:   item.AssignmentRef,
			Principal:      item.Actor,
			ProjectionKind: "progress",
		}
		if _, ok, err := ledger.Find(driver.MulticaHubKindFeedbackCarrier, source); err != nil {
			return err
		} else if ok {
			continue
		}
		child, ok, err := findAssignmentTargetFromLedger(ledger, result.SessionID, item.AssignmentRef)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		commentBody := driver.FormatMulticaProjectionComment("assignment feedback", progressCommentBody(item), []string{item.EventID})
		comment, err := cli.AddIssueComment(ctx, child, commentBody)
		if err != nil {
			return err
		}
		if status := multicaStatusForProgress(item); status != "" {
			_, _ = cli.SetIssueStatus(ctx, child, status)
		}
		_, _ = cli.SetIssueStatus(ctx, result.RootIssueID, "in_review")
		if err := ledger.Record(driver.MulticaHubLedgerRecord{
			Kind:   driver.MulticaHubKindFeedbackCarrier,
			Source: source,
			Target: driver.MulticaHubLedgerTarget{
				RootIssueID:  result.RootIssueID,
				ChildIssueID: child,
				CommentID:    comment.ID,
				Status:       "commented",
			},
		}); err != nil {
			return err
		}
		result.HubFeedbackComments++
	}
	return nil
}

func multicaStatusForProgress(item runtimeProgress) string {
	switch strings.ToLower(strings.TrimSpace(item.FeedbackKind)) {
	case "blocker":
		return "blocked"
	case "result":
		return "in_review"
	case "progress":
		return "in_progress"
	}
	if strings.TrimSpace(item.Blocker) != "" {
		return "blocked"
	}
	if strings.TrimSpace(item.Result) != "" {
		return "in_review"
	}
	return ""
}

type runtimeAssignment struct {
	ID               string
	EventID          string
	Actor            string
	Assignee         string
	Scope            string
	TTL              string
	SignalRef        string
	ExpectedWork     string
	ExpectedFeedback string
	Rationale        string
	ContextRefs      []string
	EvidenceRefs     []string
}

type runtimeProgress struct {
	ID            string
	EventID       string
	Actor         string
	AssignmentRef string
	Scope         string
	FeedbackKind  string
	Summary       string
	Result        string
	Blocker       string
	ArtifactRefs  []string
	EvidenceRefs  []string
}

func runtimeAssignmentItem(item map[string]any) runtimeAssignment {
	id := hubItemFirstString(item, "assignment_id", "id", "declaration_id")
	if id == "" {
		id = hubItemString(item, "event_id")
	}
	return runtimeAssignment{
		ID:               id,
		EventID:          hubItemFirstString(item, "event_id", "id", "declaration_id", "assignment_id"),
		Actor:            hubItemString(item, "actor"),
		Assignee:         hubItemString(item, "assignee"),
		Scope:            hubItemString(item, "scope"),
		TTL:              hubItemString(item, "ttl"),
		SignalRef:        hubItemString(item, "signal_ref"),
		ExpectedWork:     hubItemString(item, "expected_work"),
		ExpectedFeedback: hubItemString(item, "expected_feedback"),
		Rationale:        hubItemString(item, "rationale"),
		ContextRefs:      hubItemStringList(item, "context_refs"),
		EvidenceRefs:     hubItemStringList(item, "evidence_refs"),
	}
}

func runtimeProgressItem(item map[string]any) runtimeProgress {
	id := hubItemFirstString(item, "id", "declaration_id", "event_id")
	return runtimeProgress{
		ID:            id,
		EventID:       hubItemFirstString(item, "event_id", "id", "declaration_id"),
		Actor:         hubItemString(item, "actor"),
		AssignmentRef: hubItemString(item, "assignment_ref"),
		Scope:         hubItemString(item, "scope"),
		FeedbackKind:  hubItemString(item, "feedback_kind"),
		Summary:       hubItemString(item, "summary"),
		Result:        hubItemString(item, "result"),
		Blocker:       hubItemString(item, "blocker"),
		ArtifactRefs:  hubItemStringList(item, "artifact_refs"),
		EvidenceRefs:  hubItemStringList(item, "evidence_refs"),
	}
}

func runtimeMulticaHubWriteEnabled(env []string) bool {
	value := strings.TrimSpace(envValue(env, "MNEMON_MULTICA_HUB_WRITE"))
	if value == "" {
		return true
	}
	switch strings.ToLower(value) {
	case "0", "false", "off", "disabled", "no":
		return false
	default:
		return true
	}
}

func runtimeMulticaRegistry(env []string, cwd string) (driver.MulticaRegistry, bool, error) {
	paths := []string{}
	if explicit := envValue(env, "MNEMON_MULTICA_REGISTRY"); explicit != "" {
		paths = append(paths, explicit)
	}
	if strings.TrimSpace(cwd) != "" {
		paths = append(paths, driver.MulticaRegistryPath(cwd, ""))
	}
	for _, path := range paths {
		reg, ok, err := driver.LoadMulticaRegistry(path)
		if err != nil || ok {
			return reg, ok, err
		}
	}
	return driver.MulticaRegistry{}, false, nil
}

func multicaParticipantForPrincipal(reg driver.MulticaRegistry, principal string) (driver.MulticaParticipantRecord, bool) {
	for _, participant := range reg.Participants {
		if strings.TrimSpace(participant.Principal) == strings.TrimSpace(principal) {
			return participant, true
		}
	}
	return driver.MulticaParticipantRecord{}, false
}

func hubViewItems(proj pview.View, kind string) []map[string]any {
	var out []map[string]any
	for _, content := range proj.Content {
		if string(content.Ref.Kind) != kind {
			continue
		}
		for _, field := range []string{"items", "entries", "declarations"} {
			if raw, ok := content.Fields[field]; ok {
				out = append(out, hubAnyItems(raw)...)
				break
			}
		}
	}
	return out
}

func hubAnyItems(raw any) []map[string]any {
	var out []map[string]any
	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
	case []map[string]any:
		out = append(out, v...)
	}
	return out
}

func hubItemFirstString(item map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := hubItemString(item, key); value != "" {
			return value
		}
	}
	return ""
}

func hubItemString(item map[string]any, key string) string {
	if value, ok := item[key].(string); ok {
		return strings.TrimSpace(value)
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if value, ok := m[key].(string); ok {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func hubItemStringList(item map[string]any, key string) []string {
	if out := hubStringList(item[key]); len(out) > 0 {
		return out
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if out := hubStringList(m[key]); len(out) > 0 {
				return out
			}
		}
	}
	return nil
}

func hubStringList(raw any) []string {
	seen := map[string]bool{}
	var out []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		out = append(out, value)
	}
	switch v := raw.(type) {
	case []string:
		for _, item := range v {
			add(item)
		}
	case []any:
		for _, item := range v {
			if value, ok := item.(string); ok {
				add(value)
			}
		}
	case string:
		add(v)
	}
	return out
}

func assignmentMailboxTitle(item runtimeAssignment) string {
	scope := strings.TrimSpace(item.Scope)
	if scope == "" {
		scope = strings.TrimSpace(item.ID)
	}
	if scope == "" {
		scope = "assignment"
	}
	return "Mnemon assignment " + item.ID + ": " + scope
}

func assignmentMailboxDescription(item runtimeAssignment, result *runtimeImportResult) string {
	var b strings.Builder
	b.WriteString("Mnemon assignment mailbox\n\n")
	writeLine := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		b.WriteString(label)
		b.WriteString(": ")
		b.WriteString(value)
		b.WriteString("\n")
	}
	writeLine("Assignment", item.ID)
	writeLine("Session", result.SessionID)
	writeLine("Scope", item.Scope)
	writeLine("Assignee", item.Assignee)
	writeLine("Expected work", item.ExpectedWork)
	writeLine("Expected feedback", item.ExpectedFeedback)
	writeLine("Rationale", item.Rationale)
	return strings.TrimSpace(b.String())
}

func progressCommentBody(item runtimeProgress) string {
	var b strings.Builder
	writeLine := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		b.WriteString(label)
		b.WriteString(": ")
		b.WriteString(value)
		b.WriteString("\n")
	}
	writeLine("Assignment", item.AssignmentRef)
	writeLine("Feedback", item.FeedbackKind)
	writeLine("Summary", item.Summary)
	writeLine("Result", item.Result)
	writeLine("Blocker", item.Blocker)
	if len(item.ArtifactRefs) > 0 {
		writeLine("Artifacts", strings.Join(item.ArtifactRefs, ", "))
	}
	if len(item.EvidenceRefs) > 0 {
		writeLine("Evidence", strings.Join(item.EvidenceRefs, ", "))
	}
	return strings.TrimSpace(b.String())
}

func findExistingMulticaAssignmentIssue(ctx context.Context, cli driver.MulticaCLI, rootIssueID string, source driver.MulticaHubLedgerSource) (driver.MulticaIssue, bool, error) {
	children, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return driver.MulticaIssue{}, false, err
	}
	for _, child := range children {
		meta := driver.MulticaIssueHubMetadata(child)
		if !meta.IsAssignmentMailbox() {
			listed, err := cli.ListIssueMetadata(ctx, child.ID)
			if err != nil {
				return driver.MulticaIssue{}, false, err
			}
			meta = driver.ParseMulticaHubMetadata(stringMapToAny(listed))
		}
		if !meta.IsAssignmentMailbox() {
			continue
		}
		if meta.SessionID == source.SessionID &&
			meta.AssignmentID == source.AssignmentID &&
			meta.AssignmentFingerprint == source.AssignmentFingerprint &&
			meta.Principal == source.Principal {
			return child, true, nil
		}
	}
	return driver.MulticaIssue{}, false, nil
}

func findAssignmentTargetFromLedger(ledger *driver.FileMulticaHubLedger, sessionID, assignmentID string) (string, bool, error) {
	records, err := ledger.Records()
	if err != nil {
		return "", false, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Kind != driver.MulticaHubKindAssignmentMailbox {
			continue
		}
		if record.Source.SessionID == sessionID && record.Source.AssignmentID == assignmentID && strings.TrimSpace(record.Target.ChildIssueID) != "" {
			return record.Target.ChildIssueID, true, nil
		}
	}
	return "", false, nil
}

func stringMapToAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
