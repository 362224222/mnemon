package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
		participant, ok := multicaParticipantForPrincipal(reg, item.Assignee)
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
	for _, progress := range multicasurface.RuntimeViewItems(proj, "progress_digest") {
		item := multicasurface.RuntimeProgressViewItem(progress)
		if item.ID == "" || item.AssignmentRef == "" {
			continue
		}
		if !runtimeItemAfterRootIngest(item.IngestSeq, result) {
			continue
		}
		progressProjection := multicasurface.ProgressFeedbackProjectionForRuntimeItem(multicasurface.ProgressFeedbackProjectionMaterial{
			Item:          item,
			SessionID:     result.SessionID,
			CorrelationID: result.CorrelationID,
		})
		if rec, ok, err := ledger.Find(driver.MulticaHubKindFeedbackCarrier, progressProjection.Source); err != nil {
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
			if err := s.ensureProgressIssueStatuses(ctx, cli, result, child, progressProjection.Feedback); err != nil {
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

func runtimeItemAfterRootIngest(ingestSeq int64, result *runtimeImportResult) bool {
	if result == nil || result.Receipt == nil || result.Receipt.Seq <= 0 || ingestSeq <= 0 {
		return true
	}
	return multicasurface.RuntimeItemAfterRootIngest(ingestSeq, result.Receipt.Seq)
}

func assignmentMailboxMaterial(item multicasurface.RuntimeAssignmentItem, result *runtimeImportResult, rootIssue driver.MulticaIssue, participant driver.MulticaParticipantRecord) multicasurface.AssignmentMailboxMaterial {
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
