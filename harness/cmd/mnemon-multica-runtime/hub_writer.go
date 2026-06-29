package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	if !runtimeMulticaHubWriteEnabled(s.Env) {
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
	const maxAssignmentWorkers = 3
	workers := maxAssignmentWorkers
	if len(projections) < workers {
		workers = len(projections)
	}
	jobs := make(chan runtimeAssignmentProjection)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ledgerMu sync.Mutex
	var firstErr error
	created := 0
	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	hasErr := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for projection := range jobs {
				if hasErr() {
					continue
				}
				material := assignmentMailboxMaterial(projection.Item, projection.Result, projection.RootIssue, projection.Participant)
				child, err := cli.CreateIssue(ctx, driver.MulticaCreateIssueRequest{
					Title:       multicasurface.AssignmentMailboxTitle(material),
					Description: multicasurface.AssignmentMailboxDescription(material),
					ParentID:    projection.Result.RootIssueID,
					Status:      "in_progress",
					Priority:    "medium",
				})
				if err != nil {
					recordErr(err)
					continue
				}
				fullMeta := projection.Metadata.Map()
				dispatchMeta := assignmentMailboxDispatchMetadata(fullMeta)
				if err := cli.SetIssueMetadataMap(ctx, child.ID, dispatchMeta); err != nil {
					recordErr(err)
					continue
				}
				if _, err := cli.AssignIssue(ctx, child.ID, projection.Participant.AgentID); err != nil {
					recordErr(err)
					continue
				}
				ledgerMu.Lock()
				err = ledger.Record(driver.MulticaHubLedgerRecord{
					Kind:   driver.MulticaHubKindAssignmentMailbox,
					Source: projection.Source,
					Target: driver.MulticaHubLedgerTarget{
						RootIssueID:  projection.Result.RootIssueID,
						ChildIssueID: child.ID,
						Status:       "created",
					},
				})
				ledgerMu.Unlock()
				if err != nil {
					recordErr(err)
					continue
				}
				mu.Lock()
				created++
				mu.Unlock()
				if err := cli.SetIssueMetadataMap(ctx, child.ID, assignmentMailboxSupplementalMetadata(fullMeta, dispatchMeta)); err != nil {
					recordErr(err)
				}
			}
		}()
	}
	for _, projection := range projections {
		if hasErr() {
			break
		}
		jobs <- projection
	}
	close(jobs)
	wg.Wait()
	return created, firstErr
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
		material := progressFeedbackMaterial(item)
		commentBody := projection.FormatComment(projection.CommentMaterial{
			Title:    "assignment feedback",
			Body:     multicasurface.ProgressCommentBody(material),
			EventIDs: []string{item.EventID},
		})
		comment, err := cli.AddIssueComment(ctx, child, commentBody)
		if err != nil {
			return err
		}
		if status := multicasurface.ProgressIssueStatus(material); status != "" {
			_, _ = cli.SetIssueStatus(ctx, child, status)
		}
		allDone := false
		if multicasurface.ProgressCompletesAssignment(material) {
			allDone, _ = allMulticaAssignmentChildrenDone(ctx, cli, result.RootIssueID, result.SessionID, child)
		}
		if rootStatus := multicasurface.ProgressRootIssueStatus(material, allDone); rootStatus != "" {
			_, _ = cli.SetIssueStatus(ctx, result.RootIssueID, rootStatus)
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

type runtimeAssignment struct {
	ID               string
	EventID          string
	IngestSeq        int64
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
		IngestSeq:        hubItemInt64(item, "ingest_seq"),
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
	if workspace := envValue(env, "MNEMON_MANAGED_WORKSPACE"); workspace != "" {
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
	if explicit := envValue(env, "MNEMON_MULTICA_HUB_LEDGER"); explicit != "" {
		return driver.MulticaHubLedgerPath("", explicit)
	}
	if workspace := envValue(env, "MNEMON_MANAGED_WORKSPACE"); workspace != "" {
		return driver.MulticaHubLedgerPath(workspace, "")
	}
	return driver.MulticaHubLedgerPath(cwd, "")
}

func assignmentMailboxDispatchMetadata(full map[string]string) map[string]string {
	keys := []string{
		driver.MulticaMetadataSchemaVersion,
		driver.MulticaMetadataHubBackend,
		driver.MulticaMetadataKind,
		driver.MulticaMetadataSessionID,
		driver.MulticaMetadataCorrelationID,
		driver.MulticaMetadataEventID,
		driver.MulticaMetadataAssignmentID,
		driver.MulticaMetadataAssignmentFingerprint,
		driver.MulticaMetadataPrincipal,
		driver.MulticaMetadataSourceIssueID,
		driver.MulticaMetadataRootIssueID,
	}
	out := map[string]string{}
	for _, key := range keys {
		if value := strings.TrimSpace(full[key]); value != "" {
			out[key] = value
		}
	}
	return out
}

func assignmentMailboxSupplementalMetadata(full, dispatch map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range full {
		if _, ok := dispatch[key]; ok {
			continue
		}
		if value = strings.TrimSpace(value); value != "" {
			out[key] = value
		}
	}
	return out
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
