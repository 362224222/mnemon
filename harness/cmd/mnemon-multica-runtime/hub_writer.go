package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	pview "github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
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
	ledger := driver.NewFileMulticaHubLedger(multicasurface.RuntimeMulticaHubLedgerPath(s.Env, s.CWD))
	if result.HubKind == driver.MulticaHubKindSession {
		reg, ok, err := multicasurface.RuntimeMulticaRegistry(s.Env, s.CWD)
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
	for _, assignment := range multicasurface.RuntimeViewItems(proj, "assignment") {
		item := multicasurface.RuntimeAssignmentViewItem(assignment)
		if item.ID == "" || item.Assignee == "" {
			continue
		}
		if !runtimeItemAfterRootIngest(item.IngestSeq, result) {
			continue
		}
		if !runtimeAssignmentMatchesCurrentMulticaScope(item, result) {
			continue
		}
		participant, ok := multicasurface.MulticaParticipantForPrincipal(reg, item.Assignee)
		if !ok || strings.TrimSpace(participant.AgentID) == "" {
			return fmt.Errorf("no Multica agent mapping for assignment assignee %q", item.Assignee)
		}
		projection := multicasurface.AssignmentMailboxProjectionForRuntimeItem(multicasurface.AssignmentMailboxProjectionMaterial{
			Item:            item,
			SessionID:       result.SessionID,
			CorrelationID:   result.CorrelationID,
			RootIssueID:     result.RootIssueID,
			SourceIssueID:   rootIssue.ID,
			ProjectionOwner: result.Principal,
			MulticaAgentID:  participant.AgentID,
			ProjectedAt:     s.now(),
		})
		source := projection.Source
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
		projections = append(projections, runtimeAssignmentProjection{
			Item:        item,
			Participant: participant,
			Source:      source,
			Metadata:    projection.Metadata,
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
	Item        multicasurface.RuntimeAssignmentItem
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
		reservation := driver.MulticaHubLedgerRecord{
			Kind:   driver.MulticaHubKindAssignmentMailbox,
			Source: projection.Source,
			Target: driver.MulticaHubLedgerTarget{
				RootIssueID: projection.Result.RootIssueID,
				Status:      "reserved",
			},
		}
		if _, reserved, err := ledger.Reserve(reservation); err != nil {
			return created, err
		} else if !reserved {
			continue
		}
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
	for _, progress := range multicasurface.RuntimeViewItems(proj, "progress_digest") {
		item := multicasurface.RuntimeProgressViewItem(progress)
		if item.ID == "" || item.AssignmentRef == "" {
			continue
		}
		progressProjection := multicasurface.ProgressFeedbackProjectionForRuntimeItem(multicasurface.ProgressFeedbackProjectionMaterial{
			Item:          item,
			SessionID:     result.SessionID,
			CorrelationID: result.CorrelationID,
		})
		rec, recorded, err := ledger.Find(driver.MulticaHubKindFeedbackCarrier, progressProjection.Source)
		if err != nil {
			return err
		}
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
		ok, err := runtimeProgressAfterAssignmentMailbox(ledger, result.SessionID, item.AssignmentRef, item.Actor, child, item.IngestSeq)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if recorded {
			if err := s.ensureProgressIssueStatuses(ctx, cli, result, child, progressProjection.Feedback); err != nil {
				return err
			}
			continue
		}
		reservation := driver.MulticaHubLedgerRecord{
			Kind:   driver.MulticaHubKindFeedbackCarrier,
			Source: progressProjection.Source,
			Target: driver.MulticaHubLedgerTarget{
				RootIssueID:  result.RootIssueID,
				ChildIssueID: child,
				Status:       "reserved",
			},
		}
		if _, reserved, err := ledger.Reserve(reservation); err != nil {
			return err
		} else if !reserved {
			if err := s.ensureProgressIssueStatuses(ctx, cli, result, child, progressProjection.Feedback); err != nil {
				return err
			}
			continue
		}
		comment, err := cli.AddIssueComment(ctx, child, progressProjection.CommentBody)
		if err != nil {
			return err
		}
		if err := s.ensureProgressIssueStatuses(ctx, cli, result, child, progressProjection.Feedback); err != nil {
			return err
		}
		if err := ledger.Record(driver.MulticaHubLedgerRecord{
			Kind:   driver.MulticaHubKindFeedbackCarrier,
			Source: progressProjection.Source,
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

func runtimeProgressAfterAssignmentMailbox(ledger *driver.FileMulticaHubLedger, sessionID, assignmentID, principal, childIssueID string, progressSeq int64) (bool, error) {
	if progressSeq <= 0 {
		return true, nil
	}
	records, err := ledger.Records()
	if err != nil {
		return false, err
	}
	sessionID = strings.TrimSpace(sessionID)
	assignmentID = strings.TrimSpace(assignmentID)
	principal = strings.TrimSpace(principal)
	childIssueID = strings.TrimSpace(childIssueID)
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Kind != driver.MulticaHubKindAssignmentMailbox ||
			strings.TrimSpace(record.Source.SessionID) != sessionID ||
			strings.TrimSpace(record.Source.AssignmentID) != assignmentID ||
			strings.TrimSpace(record.Source.Principal) != principal ||
			strings.TrimSpace(record.Target.ChildIssueID) != childIssueID {
			continue
		}
		assignmentSeq := record.Source.IngestSeq
		if assignmentSeq <= 0 {
			assignmentSeq = multicaLocalEventSeq(record.Source.EventID)
		}
		if assignmentSeq <= 0 {
			return true, nil
		}
		return progressSeq > assignmentSeq, nil
	}
	return true, nil
}

func multicaLocalEventSeq(eventID string) int64 {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return 0
	}
	parts := strings.Split(eventID, "/")
	if len(parts) == 0 {
		return 0
	}
	seq, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return seq
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

func runtimeAssignmentMatchesCurrentMulticaScope(item multicasurface.RuntimeAssignmentItem, result *runtimeImportResult) bool {
	return multicasurface.RuntimeAssignmentMatchesScope(item, runtimeMulticaScopeMaterial(result))
}

func runtimeProgressMatchesCurrentMulticaScope(item multicasurface.RuntimeProgressItem, result *runtimeImportResult, childIssueID string) bool {
	return multicasurface.RuntimeProgressMatchesScope(item, runtimeMulticaScopeMaterial(result), childIssueID)
}

func runtimeMulticaScopeMaterial(result *runtimeImportResult) multicasurface.RuntimeScopeMaterial {
	if result == nil {
		return multicasurface.RuntimeScopeMaterial{}
	}
	return multicasurface.RuntimeScopeMaterial{
		SessionID:     result.SessionID,
		RootIssueID:   result.RootIssueID,
		CorrelationID: result.CorrelationID,
		TaskID:        result.TaskID,
	}
}

func runtimeItemAfterRootIngest(ingestSeq int64, result *runtimeImportResult) bool {
	if result == nil || result.Receipt == nil || result.Receipt.Seq <= 0 || ingestSeq <= 0 {
		return true
	}
	return multicasurface.RuntimeItemAfterRootIngest(ingestSeq, result.Receipt.Seq)
}

func assignmentMailboxMaterial(item multicasurface.RuntimeAssignmentItem, result *runtimeImportResult, rootIssue driver.MulticaIssue, participant driver.MulticaParticipantRecord) multicasurface.AssignmentMailboxMaterial {
	material := multicasurface.AssignmentMailboxRuntimeMaterial{
		Item:                item,
		RootIssueID:         rootIssue.ID,
		RootIssueIdentifier: rootIssue.Identifier,
		RootIssueTitle:      rootIssue.Title,
		AssigneeAgentName:   participant.AgentName,
		AssigneeAgentID:     participant.AgentID,
	}
	if result != nil {
		material.SessionID = result.SessionID
		material.FallbackRootIssueID = result.RootIssueID
	}
	return multicasurface.AssignmentMailboxMaterialForRuntimeItem(material)
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
		meta, err := cli.ResolveIssueHubMetadata(ctx, child)
		if err != nil {
			return driver.MulticaIssue{}, false, err
		}
		if !meta.IsAssignmentMailbox() {
			continue
		}
		if multicasurface.AssignmentMailboxMatchesSource(meta, source) {
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
	target, ok := multicasurface.SelectAssignmentTarget(
		multicasurface.AssignmentTargetCandidatesFromLedgerRecords(records),
		sessionID,
		assignmentID,
		principal,
	)
	return target, ok, nil
}

func findAssignmentTargetFromMulticaHub(ctx context.Context, cli driver.MulticaCLI, rootIssueID, sessionID, assignmentID, principal string) (string, bool, error) {
	children, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return "", false, err
	}
	candidates := []multicasurface.AssignmentTargetCandidate{}
	for _, child := range children {
		meta, err := cli.ResolveIssueHubMetadata(ctx, child)
		if err != nil {
			return "", false, err
		}
		if !meta.IsAssignmentMailbox() {
			continue
		}
		if candidate, ok := multicasurface.AssignmentTargetCandidateFromMailboxMetadata(child.ID, meta); ok {
			candidates = append(candidates, candidate)
		}
	}
	target, ok := multicasurface.SelectAssignmentTarget(candidates, sessionID, assignmentID, principal)
	return target, ok, nil
}

func allMulticaAssignmentChildrenDone(ctx context.Context, cli driver.MulticaCLI, rootIssueID, sessionID, justCompletedChildID string) (bool, error) {
	children, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return false, err
	}
	seen := false
	for _, child := range children {
		meta, err := cli.ResolveIssueHubMetadata(ctx, child)
		if err != nil {
			return false, err
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
