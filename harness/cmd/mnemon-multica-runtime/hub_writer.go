package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	pview "github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

func (s *runtimeRPCState) writeMulticaHubArtifacts(ctx context.Context, cli driver.MulticaCLI, client *access.Client, rootIssue driver.MulticaIssue, result *runtimeImportResult) {
	if result == nil {
		return
	}
	if !multicasurface.RuntimeHubWriteEnabled(s.Env) {
		result.HubWriteStatus = "skipped"
		return
	}
	if !strings.EqualFold(result.HubBackend, driver.MulticaHubBackend) ||
		(result.HubKind != driver.MulticaHubKindSession && result.HubKind != driver.MulticaHubKindAssignmentMailbox) {
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
	ledger := driver.NewFileMulticaHubLedger(runtimeMulticaHubLedgerPath(s.Env, s.CWD))
	if result.HubKind == driver.MulticaHubKindSession {
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
		if err := s.writeAssignmentMailboxes(ctx, cli, ledger, reg, proj, rootIssue, result); err != nil {
			result.HubWriteStatus = "failed"
			result.HubWriteErr = err
			return
		}
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
	existingChildren, err := cli.ListIssueChildren(ctx, result.RootIssueID)
	if err != nil {
		return err
	}
	var projections []runtimeAssignmentProjection
	for _, assignment := range hubViewItems(proj, "assignment") {
		item := runtimeAssignmentItem(assignment)
		if item.ID == "" || item.Assignee == "" {
			continue
		}
		if !hubItemAfterRootIngest(item.IngestSeq, result) {
			continue
		}
		if !runtimeAssignmentMatchesCurrentMulticaScope(item, result) {
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
		if existing, ok, err := findExistingMulticaAssignmentIssueInChildren(ctx, cli, existingChildren, source); err != nil {
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
		projections = append(projections, runtimeAssignmentProjection{
			Item:        item,
			Participant: participant,
			Source:      source,
			Metadata:    meta,
			RootIssue:   rootIssue,
			Result:      result,
		})
	}
	created, err := s.projectAssignmentMailboxes(ctx, cli, ledger, projections)
	result.HubChildIssues += created
	if err != nil {
		return err
	}
	return nil
}

type runtimeAssignmentProjection struct {
	Item        runtimeAssignment
	Participant driver.MulticaParticipantRecord
	Source      driver.MulticaHubLedgerSource
	Metadata    driver.MulticaHubMetadata
	RootIssue   driver.MulticaIssue
	Result      *runtimeImportResult
}

func (s *runtimeRPCState) projectAssignmentMailboxes(ctx context.Context, cli driver.MulticaCLI, ledger *driver.FileMulticaHubLedger, projections []runtimeAssignmentProjection) (int, error) {
	if len(projections) == 0 {
		return 0, nil
	}
	created := 0
	var errs []error
	for _, projection := range projections {
		material := assignmentMailboxMaterial(projection.Item, projection.Result, projection.RootIssue, projection.Participant)
		child, err := retryMulticaHubValue(ctx, func() (driver.MulticaIssue, error) {
			return cli.CreateIssue(ctx, driver.MulticaCreateIssueRequest{
				Title:          multicasurface.AssignmentMailboxTitle(material),
				Description:    multicasurface.AssignmentMailboxDescription(material),
				ParentID:       projection.Result.RootIssueID,
				Status:         "in_progress",
				Priority:       "medium",
				AllowDuplicate: true,
			})
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("create assignment mailbox %s: %w", projection.Item.ID, err))
			continue
		}
		fullMeta := projection.Metadata.Map()
		dispatchMeta := multicasurface.AssignmentMailboxDispatchMetadata(fullMeta)
		if err := setMulticaHubMetadataMap(ctx, cli, child.ID, dispatchMeta); err != nil {
			errs = append(errs, fmt.Errorf("tag assignment mailbox %s (%s): %w", projection.Item.ID, child.ID, err))
			continue
		}
		if _, err := retryMulticaHubValue(ctx, func() (driver.MulticaIssue, error) {
			return cli.AssignIssue(ctx, child.ID, projection.Participant.AgentID)
		}); err != nil {
			errs = append(errs, fmt.Errorf("assign assignment mailbox %s (%s): %w", projection.Item.ID, child.ID, err))
			continue
		}
		if err := ledger.Record(driver.MulticaHubLedgerRecord{
			Kind:   driver.MulticaHubKindAssignmentMailbox,
			Source: projection.Source,
			Target: driver.MulticaHubLedgerTarget{
				RootIssueID:  projection.Result.RootIssueID,
				ChildIssueID: child.ID,
				Status:       "created",
			},
		}); err != nil {
			return created, err
		}
		created++
		if err := setMulticaHubMetadataMap(ctx, cli, child.ID, multicasurface.AssignmentMailboxSupplementalMetadata(fullMeta, dispatchMeta)); err != nil {
			errs = append(errs, fmt.Errorf("tag supplemental assignment mailbox %s (%s): %w", projection.Item.ID, child.ID, err))
			continue
		}
	}
	return created, errors.Join(errs...)
}

func setMulticaHubMetadataMap(ctx context.Context, cli driver.MulticaCLI, issueID string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var errs []error
	for _, key := range keys {
		value := values[key]
		if err := retryMulticaHubOperation(ctx, func() error {
			return cli.SetIssueMetadata(ctx, issueID, key, value, "string")
		}); err != nil {
			errs = append(errs, fmt.Errorf("set %s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

func retryMulticaHubOperation(ctx context.Context, op func() error) error {
	_, err := retryMulticaHubValue(ctx, func() (struct{}, error) {
		return struct{}{}, op()
	})
	return err
}

func retryMulticaHubValue[T any](ctx context.Context, op func() (T, error)) (T, error) {
	var zero T
	const attempts = 3
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(time.Duration(attempt) * 250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return zero, ctx.Err()
			case <-timer.C:
			}
		}
		var value T
		value, err = op()
		if err == nil {
			return value, nil
		}
	}
	return zero, err
}

func (s *runtimeRPCState) writeProgressComments(ctx context.Context, cli driver.MulticaCLI, ledger *driver.FileMulticaHubLedger, proj pview.View, result *runtimeImportResult) error {
	for _, progress := range hubViewItems(proj, "progress_digest") {
		item := runtimeProgressItem(progress)
		if item.ID == "" || item.AssignmentRef == "" {
			continue
		}
		if !hubItemAfterRootIngest(item.IngestSeq, result) {
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
		material := progressFeedbackMaterial(item)
		if rec, ok, err := ledger.Find(driver.MulticaHubKindFeedbackCarrier, source); err != nil {
			return err
		} else if ok {
			child := strings.TrimSpace(rec.Target.ChildIssueID)
			if child == "" {
				var found bool
				child, found, err = findAssignmentTargetFromLedger(ledger, result.SessionID, item.AssignmentRef, item.Actor)
				if err != nil {
					return err
				}
				if !found {
					child, found, err = findAssignmentTargetFromMulticaHub(ctx, cli, result.RootIssueID, result.SessionID, item.AssignmentRef, item.Actor)
					if err != nil {
						return err
					}
				}
				if !found {
					continue
				}
			}
			if !runtimeProgressMatchesCurrentMulticaScope(item, result, child) {
				continue
			}
			if err := s.ensureProgressIssueStatuses(ctx, cli, result, child, material); err != nil {
				return err
			}
			continue
		}
		child, ok, err := findAssignmentTargetFromLedger(ledger, result.SessionID, item.AssignmentRef, item.Actor)
		if err != nil {
			return err
		}
		if !ok {
			child, ok, err = findAssignmentTargetFromMulticaHub(ctx, cli, result.RootIssueID, result.SessionID, item.AssignmentRef, item.Actor)
			if err != nil {
				return err
			}
		}
		if !ok {
			continue
		}
		if !runtimeProgressMatchesCurrentMulticaScope(item, result, child) {
			continue
		}
		commentBody := projection.FormatComment(projection.CommentMaterial{
			Title:        "assignment feedback",
			Body:         multicasurface.ProgressCommentBody(material),
			EventIDs:     []string{item.EventID},
			EventType:    "progress_digest.accepted",
			SessionID:    result.SessionID,
			AssignmentID: item.AssignmentRef,
		})
		comment, err := cli.AddIssueComment(ctx, child, commentBody)
		if err != nil {
			return err
		}
		if err := s.ensureProgressIssueStatuses(ctx, cli, result, child, material); err != nil {
			return err
		}
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

func (s *runtimeRPCState) ensureProgressIssueStatuses(ctx context.Context, cli driver.MulticaCLI, result *runtimeImportResult, child string, material multicasurface.ProgressFeedbackMaterial) error {
	if status := multicasurface.ProgressIssueStatus(material); status != "" {
		if err := retryMulticaHubOperation(ctx, func() error {
			_, err := cli.SetIssueStatus(ctx, child, status)
			return err
		}); err != nil {
			return fmt.Errorf("set assignment feedback issue %s status %s: %w", child, status, err)
		}
	}
	allDone := false
	if multicasurface.ProgressCompletesAssignment(material) {
		var err error
		allDone, err = allMulticaAssignmentChildrenDone(ctx, cli, result.RootIssueID, result.SessionID, child)
		if err != nil {
			return err
		}
	}
	if rootStatus := multicasurface.ProgressRootIssueStatus(material, allDone); rootStatus != "" {
		if err := retryMulticaHubOperation(ctx, func() error {
			_, err := cli.SetIssueStatus(ctx, result.RootIssueID, rootStatus)
			return err
		}); err != nil {
			return fmt.Errorf("set root issue %s status %s: %w", result.RootIssueID, rootStatus, err)
		}
	}
	return nil
}

type runtimeAssignment struct {
	ID               string
	EventID          string
	IngestSeq        int64
	SessionID        string
	RootIssueID      string
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
	IngestSeq     int64
	SessionID     string
	RootIssueID   string
	Actor         string
	AssignmentRef string
	Scope         string
	FeedbackKind  string
	Summary       string
	Result        string
	Blocker       string
	ContextRefs   []string
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
		IngestSeq:        hubItemInt64(item, "ingest_seq"),
		SessionID:        hubItemString(item, "session_id"),
		RootIssueID:      hubItemString(item, "root_issue_id"),
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
		IngestSeq:     hubItemInt64(item, "ingest_seq"),
		SessionID:     hubItemString(item, "session_id"),
		RootIssueID:   hubItemString(item, "root_issue_id"),
		Actor:         hubItemString(item, "actor"),
		AssignmentRef: hubItemString(item, "assignment_ref"),
		Scope:         hubItemString(item, "scope"),
		FeedbackKind:  hubItemString(item, "feedback_kind"),
		Summary:       hubItemString(item, "summary"),
		Result:        hubItemString(item, "result"),
		Blocker:       hubItemString(item, "blocker"),
		ContextRefs:   hubItemStringList(item, "context_refs"),
		ArtifactRefs:  hubItemStringList(item, "artifact_refs"),
		EvidenceRefs:  hubItemStringList(item, "evidence_refs"),
	}
}

func runtimeAssignmentMatchesCurrentMulticaScope(item runtimeAssignment, result *runtimeImportResult) bool {
	if !runtimeExplicitMulticaScopeMatches(item.SessionID, item.RootIssueID, result) {
		return false
	}
	refs := append([]string{}, item.ContextRefs...)
	refs = append(refs, item.EvidenceRefs...)
	return runtimeRefsMatchCurrentMulticaScope(refs, result)
}

func runtimeProgressMatchesCurrentMulticaScope(item runtimeProgress, result *runtimeImportResult, childIssueID string) bool {
	if !runtimeExplicitMulticaScopeMatches(item.SessionID, item.RootIssueID, result) {
		return false
	}
	refs := append([]string{}, item.ContextRefs...)
	refs = append(refs, item.EvidenceRefs...)
	refs = append(refs, item.ArtifactRefs...)
	return runtimeRefsMatchCurrentMulticaScope(refs, result, childIssueID)
}

func runtimeExplicitMulticaScopeMatches(sessionID, rootIssueID string, result *runtimeImportResult) bool {
	if result == nil {
		return true
	}
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" && strings.TrimSpace(result.SessionID) != "" && sessionID != strings.TrimSpace(result.SessionID) {
		return false
	}
	if rootIssueID = strings.TrimSpace(rootIssueID); rootIssueID != "" && strings.TrimSpace(result.RootIssueID) != "" && rootIssueID != strings.TrimSpace(result.RootIssueID) {
		return false
	}
	return true
}

func runtimeRefsMatchCurrentMulticaScope(refs []string, result *runtimeImportResult, extraIssueIDs ...string) bool {
	scoped := false
	for _, ref := range refs {
		isScoped, matches := runtimeMulticaScopeRefMatches(ref, result, extraIssueIDs...)
		if !isScoped {
			continue
		}
		scoped = true
		if matches {
			return true
		}
	}
	return !scoped
}

func runtimeMulticaScopeRefMatches(ref string, result *runtimeImportResult, extraIssueIDs ...string) (bool, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false, false
	}
	lower := strings.ToLower(ref)
	prefixes := []string{"multica:issue:", "multica:issue/", "mention://issue/", "multica:session:", "multica:session/", "multica:task:", "multica:task/"}
	scoped := false
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			scoped = true
			break
		}
	}
	if !scoped {
		return false, false
	}
	candidates := []string{}
	if result != nil {
		root := strings.TrimSpace(result.RootIssueID)
		if root != "" {
			candidates = append(candidates,
				"multica:issue:"+root,
				"multica:issue/"+root,
				"mention://issue/"+root,
				"multica:session:"+root,
				"multica:session/"+root,
			)
		}
		if session := strings.TrimSpace(result.SessionID); session != "" {
			candidates = append(candidates, session)
		}
		if correlation := strings.TrimSpace(result.CorrelationID); correlation != "" {
			candidates = append(candidates, correlation)
		}
		if task := strings.TrimSpace(result.TaskID); task != "" {
			candidates = append(candidates,
				"multica:task:"+task,
				"multica:task/"+task,
			)
		}
	}
	for _, issueID := range extraIssueIDs {
		issueID = strings.TrimSpace(issueID)
		if issueID == "" {
			continue
		}
		candidates = append(candidates,
			"multica:issue:"+issueID,
			"multica:issue/"+issueID,
			"mention://issue/"+issueID,
		)
	}
	for _, candidate := range candidates {
		if ref == candidate {
			return true, true
		}
	}
	return true, false
}

func runtimeMulticaRegistry(env []string, cwd string) (driver.MulticaRegistry, bool, error) {
	paths := []string{}
	if explicit := multicasurface.RuntimeEnvValue(env, "MNEMON_MULTICA_REGISTRY"); explicit != "" {
		paths = append(paths, explicit)
	}
	if workspace := multicasurface.RuntimeEnvValue(env, "MNEMON_MANAGED_WORKSPACE"); workspace != "" {
		paths = append(paths, driver.MulticaRegistryPath(workspace, ""))
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

func runtimeMulticaHubLedgerPath(env []string, cwd string) string {
	if explicit := multicasurface.RuntimeEnvValue(env, "MNEMON_MULTICA_HUB_LEDGER"); explicit != "" {
		return driver.MulticaHubLedgerPath("", explicit)
	}
	if workspace := multicasurface.RuntimeEnvValue(env, "MNEMON_MANAGED_WORKSPACE"); workspace != "" {
		return driver.MulticaHubLedgerPath(workspace, "")
	}
	return driver.MulticaHubLedgerPath(cwd, "")
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

func hubItemInt64(item map[string]any, key string) int64 {
	if value, ok := hubInt64(item[key]); ok {
		return value
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if value, ok := hubInt64(m[key]); ok {
				return value
			}
		}
	}
	return 0
}

func hubInt64(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func hubItemAfterRootIngest(ingestSeq int64, result *runtimeImportResult) bool {
	if result == nil || result.Receipt == nil || result.Receipt.Seq <= 0 || ingestSeq <= 0 {
		return true
	}
	return ingestSeq > result.Receipt.Seq
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

func assignmentMailboxMaterial(item runtimeAssignment, result *runtimeImportResult, rootIssue driver.MulticaIssue, participant driver.MulticaParticipantRecord) multicasurface.AssignmentMailboxMaterial {
	material := multicasurface.AssignmentMailboxMaterial{
		ID:               item.ID,
		Scope:            item.Scope,
		Assignee:         item.Assignee,
		AssigneeDisplay:  firstNonEmpty(participant.AgentName, participant.AgentID),
		RootIssueID:      rootIssue.ID,
		RootIssueLabel:   firstNonEmpty(rootIssue.Identifier, rootIssue.ID),
		RootIssueTitle:   rootIssue.Title,
		ExpectedWork:     item.ExpectedWork,
		ExpectedFeedback: item.ExpectedFeedback,
		Rationale:        item.Rationale,
	}
	if result != nil {
		material.SessionID = result.SessionID
		material.RootIssueID = firstNonEmpty(material.RootIssueID, result.RootIssueID)
	}
	return material
}

func progressFeedbackMaterial(item runtimeProgress) multicasurface.ProgressFeedbackMaterial {
	return multicasurface.ProgressFeedbackMaterial{
		AssignmentRef: item.AssignmentRef,
		FeedbackKind:  item.FeedbackKind,
		Summary:       item.Summary,
		Result:        item.Result,
		Blocker:       item.Blocker,
		ArtifactRefs:  item.ArtifactRefs,
		EvidenceRefs:  item.EvidenceRefs,
	}
}

func findExistingMulticaAssignmentIssue(ctx context.Context, cli driver.MulticaCLI, rootIssueID string, source driver.MulticaHubLedgerSource) (driver.MulticaIssue, bool, error) {
	children, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return driver.MulticaIssue{}, false, err
	}
	return findExistingMulticaAssignmentIssueInChildren(ctx, cli, children, source)
}

func findExistingMulticaAssignmentIssueInChildren(ctx context.Context, cli driver.MulticaCLI, children []driver.MulticaIssue, source driver.MulticaHubLedgerSource) (driver.MulticaIssue, bool, error) {
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

func findAssignmentTargetFromLedger(ledger *driver.FileMulticaHubLedger, sessionID, assignmentID, principal string) (string, bool, error) {
	records, err := ledger.Records()
	if err != nil {
		return "", false, err
	}
	var fallback string
	matches := 0
	principal = strings.TrimSpace(principal)
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Kind != driver.MulticaHubKindAssignmentMailbox {
			continue
		}
		if record.Source.SessionID != sessionID || record.Source.AssignmentID != assignmentID || strings.TrimSpace(record.Target.ChildIssueID) == "" {
			continue
		}
		matches++
		if principal != "" && record.Source.Principal == principal {
			return record.Target.ChildIssueID, true, nil
		}
		if fallback == "" {
			fallback = record.Target.ChildIssueID
		}
	}
	if principal == "" || matches == 1 {
		return fallback, fallback != "", nil
	}
	return "", false, nil
}

func findAssignmentTargetFromMulticaHub(ctx context.Context, cli driver.MulticaCLI, rootIssueID, sessionID, assignmentID, principal string) (string, bool, error) {
	children, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return "", false, err
	}
	var fallback string
	matches := 0
	principal = strings.TrimSpace(principal)
	for _, child := range children {
		meta := driver.MulticaIssueHubMetadata(child)
		if !meta.IsAssignmentMailbox() {
			listed, err := cli.ListIssueMetadata(ctx, child.ID)
			if err != nil {
				return "", false, err
			}
			meta = driver.ParseMulticaHubMetadata(stringMapToAny(listed))
		}
		if !meta.IsAssignmentMailbox() {
			continue
		}
		if meta.SessionID != sessionID || meta.AssignmentID != assignmentID || strings.TrimSpace(child.ID) == "" {
			continue
		}
		matches++
		if principal != "" && meta.Principal == principal {
			return child.ID, true, nil
		}
		if fallback == "" {
			fallback = child.ID
		}
	}
	if principal == "" || matches == 1 {
		return fallback, fallback != "", nil
	}
	return "", false, nil
}

func allMulticaAssignmentChildrenDone(ctx context.Context, cli driver.MulticaCLI, rootIssueID, sessionID, justCompletedChildID string) (bool, error) {
	children, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return false, err
	}
	seen := false
	for _, child := range children {
		meta := driver.MulticaIssueHubMetadata(child)
		if !meta.IsAssignmentMailbox() {
			listed, err := cli.ListIssueMetadata(ctx, child.ID)
			if err != nil {
				return false, err
			}
			meta = driver.ParseMulticaHubMetadata(stringMapToAny(listed))
		}
		if !meta.IsAssignmentMailbox() || meta.SessionID != sessionID {
			continue
		}
		seen = true
		if child.ID == justCompletedChildID {
			continue
		}
		if multicasurface.IssueStatusDone(child.Status) {
			continue
		}
		return false, nil
	}
	return seen, nil
}

func stringMapToAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
