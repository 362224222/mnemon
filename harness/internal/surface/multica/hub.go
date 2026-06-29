package multica

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	MulticaHubBackend = "multica"

	MulticaDefaultHubLedgerRelPath = ".mnemon/harness/multica/hub-ledger.jsonl"

	MulticaMetadataSchemaVersion          = "mnemon.schema_version"
	MulticaMetadataHubBackend             = "mnemon.hub_backend"
	MulticaMetadataKind                   = "mnemon.kind"
	MulticaMetadataSessionID              = "mnemon.session_id"
	MulticaMetadataCorrelationID          = "mnemon.correlation_id"
	MulticaMetadataEventID                = "mnemon.event_id"
	MulticaMetadataEventType              = "mnemon.event_type"
	MulticaMetadataEventPhase             = "mnemon.event_phase"
	MulticaMetadataAssignmentID           = "mnemon.assignment_id"
	MulticaMetadataAssignmentFingerprint  = "mnemon.assignment_fingerprint"
	MulticaMetadataPrincipal              = "mnemon.principal"
	MulticaMetadataSourceIssueID          = "mnemon.source_issue_id"
	MulticaMetadataRootIssueID            = "mnemon.root_issue_id"
	MulticaMetadataProjectionOwner        = "mnemon.projection_owner"
	MulticaMetadataMulticaAgentID         = "mnemon.multica_agent_id"
	MulticaMetadataProjectedAt            = "mnemon.projected_at"
	MulticaMetadataEnvelopeDigest         = "mnemon.envelope_digest"
	MulticaHubKindSession                 = "session_mailbox"
	MulticaHubKindAssignmentMailbox       = "assignment_mailbox"
	MulticaHubKindFeedbackCarrier         = "feedback_carrier"
	MulticaHubKindAssignmentProjectionOld = "assignment_projection"
)

type MulticaHubMetadata struct {
	SchemaVersion         string
	HubBackend            string
	Kind                  string
	SessionID             string
	CorrelationID         string
	EventID               string
	EventType             string
	EventPhase            string
	AssignmentID          string
	AssignmentFingerprint string
	Principal             string
	SourceIssueID         string
	RootIssueID           string
	ProjectionOwner       string
	MulticaAgentID        string
	ProjectedAt           string
	EnvelopeDigest        string
}

type RootSessionMetadataMaterial struct {
	HubMetadata     MulticaHubMetadata
	EventID         string
	EventType       string
	EventPhase      string
	Principal       string
	SourceIssueID   string
	ProjectionOwner string
	ProjectedAt     time.Time
}

type MulticaAssignmentFingerprintInput struct {
	AssignmentID     string   `json:"assignment_id,omitempty"`
	Assignee         string   `json:"assignee,omitempty"`
	Scope            string   `json:"scope,omitempty"`
	ExpectedWork     string   `json:"expected_work,omitempty"`
	ExpectedFeedback string   `json:"expected_feedback,omitempty"`
	SignalRef        string   `json:"signal_ref,omitempty"`
	ContextRefs      []string `json:"context_refs,omitempty"`
	EvidenceRefs     []string `json:"evidence_refs,omitempty"`
	CorrelationID    string   `json:"correlation_id,omitempty"`
}

type MulticaHubLedgerRecord struct {
	SchemaVersion int                    `json:"schema_version"`
	Kind          string                 `json:"kind"`
	Source        MulticaHubLedgerSource `json:"source"`
	Target        MulticaHubLedgerTarget `json:"target"`
	CreatedAt     string                 `json:"created_at"`
	UpdatedAt     string                 `json:"updated_at,omitempty"`
}

type MulticaHubLedgerSource struct {
	SessionID             string `json:"session_id,omitempty"`
	CorrelationID         string `json:"correlation_id,omitempty"`
	EventID               string `json:"event_id,omitempty"`
	AssignmentID          string `json:"assignment_id,omitempty"`
	AssignmentFingerprint string `json:"assignment_fingerprint,omitempty"`
	Principal             string `json:"principal,omitempty"`
	ProjectionKind        string `json:"projection_kind,omitempty"`
}

type MulticaHubLedgerTarget struct {
	RootIssueID  string `json:"root_issue_id,omitempty"`
	ChildIssueID string `json:"child_issue_id,omitempty"`
	CommentID    string `json:"comment_id,omitempty"`
	Status       string `json:"status,omitempty"`
}

type FileMulticaHubLedger struct {
	path    string
	loaded  bool
	loadErr error
	records []MulticaHubLedgerRecord
}

func MulticaHubLedgerPath(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimSpace(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, MulticaDefaultHubLedgerRelPath)
}

func NewFileMulticaHubLedger(path string) *FileMulticaHubLedger {
	return &FileMulticaHubLedger{path: strings.TrimSpace(path)}
}

func (l *FileMulticaHubLedger) Records() ([]MulticaHubLedgerRecord, error) {
	if err := l.load(); err != nil {
		return nil, err
	}
	return append([]MulticaHubLedgerRecord(nil), l.records...), nil
}

func (l *FileMulticaHubLedger) Find(kind string, source MulticaHubLedgerSource) (MulticaHubLedgerRecord, bool, error) {
	if err := l.load(); err != nil {
		return MulticaHubLedgerRecord{}, false, err
	}
	want := multicaHubLedgerKey(kind, source)
	for i := len(l.records) - 1; i >= 0; i-- {
		rec := l.records[i]
		if multicaHubLedgerKey(rec.Kind, rec.Source) == want {
			return rec, true, nil
		}
	}
	return MulticaHubLedgerRecord{}, false, nil
}

func (l *FileMulticaHubLedger) Record(record MulticaHubLedgerRecord) error {
	if err := l.load(); err != nil {
		return err
	}
	if strings.TrimSpace(l.path) == "" {
		return fmt.Errorf("multica hub ledger path is required")
	}
	key := multicaHubLedgerKey(record.Kind, record.Source)
	for _, existing := range l.records {
		if multicaHubLedgerKey(existing.Kind, existing.Source) == key && existing.Target == record.Target {
			return nil
		}
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = 1
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(record.CreatedAt) == "" {
		record.CreatedAt = now
	}
	for i := len(l.records) - 1; i >= 0; i-- {
		if multicaHubLedgerKey(l.records[i].Kind, l.records[i].Source) == key {
			record.CreatedAt = l.records[i].CreatedAt
			record.UpdatedAt = now
			break
		}
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(record); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	l.upsertLoaded(record)
	return nil
}

func (l *FileMulticaHubLedger) load() error {
	if l.loaded {
		return l.loadErr
	}
	l.loaded = true
	if strings.TrimSpace(l.path) == "" {
		l.loadErr = fmt.Errorf("multica hub ledger path is required")
		return l.loadErr
	}
	f, err := os.Open(l.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		l.loadErr = err
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record MulticaHubLedgerRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			l.loadErr = err
			return err
		}
		l.upsertLoaded(record)
	}
	if err := scanner.Err(); err != nil {
		l.loadErr = err
		return err
	}
	return nil
}

func (l *FileMulticaHubLedger) upsertLoaded(record MulticaHubLedgerRecord) {
	key := multicaHubLedgerKey(record.Kind, record.Source)
	for i := range l.records {
		if multicaHubLedgerKey(l.records[i].Kind, l.records[i].Source) == key {
			l.records[i] = record
			return
		}
	}
	l.records = append(l.records, record)
}

func multicaHubLedgerKey(kind string, source MulticaHubLedgerSource) string {
	return strings.Join([]string{
		strings.TrimSpace(kind),
		strings.TrimSpace(source.SessionID),
		strings.TrimSpace(source.CorrelationID),
		strings.TrimSpace(source.EventID),
		strings.TrimSpace(source.AssignmentID),
		strings.TrimSpace(source.AssignmentFingerprint),
		strings.TrimSpace(source.Principal),
		strings.TrimSpace(source.ProjectionKind),
	}, "\x00")
}

func ParseMulticaHubMetadata(raw map[string]any) MulticaHubMetadata {
	meta := NormalizeMulticaMetadata(raw)
	return MulticaHubMetadata{
		SchemaVersion:         meta[MulticaMetadataSchemaVersion],
		HubBackend:            meta[MulticaMetadataHubBackend],
		Kind:                  meta[MulticaMetadataKind],
		SessionID:             meta[MulticaMetadataSessionID],
		CorrelationID:         meta[MulticaMetadataCorrelationID],
		EventID:               meta[MulticaMetadataEventID],
		EventType:             meta[MulticaMetadataEventType],
		EventPhase:            meta[MulticaMetadataEventPhase],
		AssignmentID:          meta[MulticaMetadataAssignmentID],
		AssignmentFingerprint: meta[MulticaMetadataAssignmentFingerprint],
		Principal:             meta[MulticaMetadataPrincipal],
		SourceIssueID:         meta[MulticaMetadataSourceIssueID],
		RootIssueID:           meta[MulticaMetadataRootIssueID],
		ProjectionOwner:       meta[MulticaMetadataProjectionOwner],
		MulticaAgentID:        meta[MulticaMetadataMulticaAgentID],
		ProjectedAt:           meta[MulticaMetadataProjectedAt],
		EnvelopeDigest:        meta[MulticaMetadataEnvelopeDigest],
	}
}

func (m MulticaHubMetadata) Map() map[string]string {
	out := map[string]string{}
	add := func(key, value string) {
		value = strings.TrimSpace(value)
		if value != "" {
			out[key] = value
		}
	}
	add(MulticaMetadataSchemaVersion, firstNonEmptyString(m.SchemaVersion, "1"))
	add(MulticaMetadataHubBackend, firstNonEmptyString(m.HubBackend, MulticaHubBackend))
	add(MulticaMetadataKind, m.Kind)
	add(MulticaMetadataSessionID, m.SessionID)
	add(MulticaMetadataCorrelationID, m.CorrelationID)
	add(MulticaMetadataEventID, m.EventID)
	add(MulticaMetadataEventType, m.EventType)
	add(MulticaMetadataEventPhase, m.EventPhase)
	add(MulticaMetadataAssignmentID, m.AssignmentID)
	add(MulticaMetadataAssignmentFingerprint, m.AssignmentFingerprint)
	add(MulticaMetadataPrincipal, m.Principal)
	add(MulticaMetadataSourceIssueID, m.SourceIssueID)
	add(MulticaMetadataRootIssueID, m.RootIssueID)
	add(MulticaMetadataProjectionOwner, m.ProjectionOwner)
	add(MulticaMetadataMulticaAgentID, m.MulticaAgentID)
	add(MulticaMetadataProjectedAt, m.ProjectedAt)
	add(MulticaMetadataEnvelopeDigest, m.EnvelopeDigest)
	return out
}

func (m MulticaHubMetadata) IsAssignmentMailbox() bool {
	if strings.TrimSpace(m.HubBackend) != "" && m.HubBackend != MulticaHubBackend {
		return false
	}
	switch strings.TrimSpace(m.Kind) {
	case MulticaHubKindAssignmentMailbox, MulticaHubKindAssignmentProjectionOld:
		return strings.TrimSpace(m.AssignmentID) != "" || strings.TrimSpace(m.AssignmentFingerprint) != ""
	default:
		return false
	}
}

func RootSessionHubMetadata(meta MulticaHubMetadata, issueID string) MulticaHubMetadata {
	issueID = strings.TrimSpace(issueID)
	meta.HubBackend = MulticaHubBackend
	meta.Kind = firstNonEmptyString(meta.Kind, MulticaHubKindSession)
	meta.RootIssueID = firstNonEmptyString(meta.RootIssueID, issueID)
	meta.SessionID = firstNonEmptyString(meta.SessionID, MulticaSessionID(meta.RootIssueID))
	if issueID != "" {
		meta.CorrelationID = firstNonEmptyString(meta.CorrelationID, "multica:issue:"+issueID)
	}
	return meta
}

func AssignmentMailboxHubMetadata(meta MulticaHubMetadata, issueID string) MulticaHubMetadata {
	issueID = strings.TrimSpace(issueID)
	meta.HubBackend = firstNonEmptyString(meta.HubBackend, MulticaHubBackend)
	meta.Kind = firstNonEmptyString(meta.Kind, MulticaHubKindAssignmentMailbox)
	meta.RootIssueID = firstNonEmptyString(meta.RootIssueID, meta.SourceIssueID, issueID)
	meta.SessionID = firstNonEmptyString(meta.SessionID, MulticaSessionID(meta.RootIssueID))
	if issueID != "" {
		meta.CorrelationID = firstNonEmptyString(meta.CorrelationID, "multica:issue:"+issueID)
	}
	return meta
}

func AssignmentMailboxMarker(meta MulticaHubMetadata, issueID string) string {
	if strings.TrimSpace(meta.EventID) != "" {
		return strings.TrimSpace(meta.EventID)
	}
	if strings.TrimSpace(meta.AssignmentID) != "" {
		return "multica-assignment-" + strings.TrimSpace(meta.AssignmentID)
	}
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return ""
	}
	return "multica-issue-" + issueID
}

func RootSessionMetadataMap(material RootSessionMetadataMaterial) map[string]string {
	meta := RootSessionHubMetadata(material.HubMetadata, material.SourceIssueID)
	meta.EventID = firstNonEmptyString(material.EventID, meta.EventID)
	meta.EventType = firstNonEmptyString(material.EventType, meta.EventType)
	meta.EventPhase = firstNonEmptyString(material.EventPhase, meta.EventPhase)
	meta.Principal = firstNonEmptyString(material.Principal, meta.Principal)
	meta.SourceIssueID = firstNonEmptyString(material.SourceIssueID, meta.SourceIssueID)
	meta.ProjectionOwner = firstNonEmptyString(material.ProjectionOwner, meta.ProjectionOwner)
	if !material.ProjectedAt.IsZero() {
		meta.ProjectedAt = firstNonEmptyString(meta.ProjectedAt, material.ProjectedAt.UTC().Format(time.RFC3339))
	}
	return meta.Map()
}

func AssignmentMailboxDispatchMetadata(full map[string]string) map[string]string {
	keys := []string{
		MulticaMetadataSchemaVersion,
		MulticaMetadataHubBackend,
		MulticaMetadataKind,
		MulticaMetadataSessionID,
		MulticaMetadataCorrelationID,
		MulticaMetadataEventID,
		MulticaMetadataAssignmentID,
		MulticaMetadataAssignmentFingerprint,
		MulticaMetadataPrincipal,
		MulticaMetadataSourceIssueID,
		MulticaMetadataRootIssueID,
	}
	out := map[string]string{}
	for _, key := range keys {
		if value := strings.TrimSpace(full[key]); value != "" {
			out[key] = value
		}
	}
	return out
}

func AssignmentMailboxSupplementalMetadata(full, dispatch map[string]string) map[string]string {
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

func NormalizeMulticaMetadata(raw any) map[string]string {
	out := map[string]string{}
	merge := func(key string, value any) {
		key = strings.TrimSpace(key)
		if key == "" {
			return
		}
		if key == "metadata" {
			for k, v := range NormalizeMulticaMetadata(value) {
				out[k] = v
			}
			return
		}
		out[key] = multicaMetadataString(value)
	}
	switch v := raw.(type) {
	case nil:
	case map[string]string:
		for key, value := range v {
			merge(key, value)
		}
	case map[string]any:
		if key := firstNonEmptyString(anyString(v["key"]), anyString(v["name"])); key != "" {
			merge(key, firstPresent(v, "value", "val", "metadata_value"))
			return out
		}
		for key, value := range v {
			merge(key, value)
		}
	case []any:
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			key := firstNonEmptyString(anyString(m["key"]), anyString(m["name"]))
			if key == "" {
				for k, value := range NormalizeMulticaMetadata(m) {
					out[k] = value
				}
				continue
			}
			merge(key, firstPresent(m, "value", "val", "metadata_value"))
		}
	}
	return out
}

func MulticaAssignmentFingerprint(input MulticaAssignmentFingerprintInput) string {
	input.AssignmentID = strings.TrimSpace(input.AssignmentID)
	input.Assignee = strings.TrimSpace(input.Assignee)
	input.Scope = strings.TrimSpace(input.Scope)
	input.ExpectedWork = strings.TrimSpace(input.ExpectedWork)
	input.ExpectedFeedback = strings.TrimSpace(input.ExpectedFeedback)
	input.SignalRef = strings.TrimSpace(input.SignalRef)
	input.CorrelationID = strings.TrimSpace(input.CorrelationID)
	input.ContextRefs = cleanMulticaRefs(input.ContextRefs)
	input.EvidenceRefs = cleanMulticaRefs(input.EvidenceRefs)
	sort.Strings(input.ContextRefs)
	sort.Strings(input.EvidenceRefs)
	data, _ := json.Marshal(input)
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func MulticaSessionID(rootIssueID string) string {
	rootIssueID = strings.TrimSpace(rootIssueID)
	if rootIssueID == "" {
		return ""
	}
	return "multica:session:" + rootIssueID
}

func cleanMulticaRefs(values []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func multicaMetadataString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return strings.TrimSpace(v.String())
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", v), "0"), ".")
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return strings.TrimSpace(fmt.Sprint(v))
		}
		return strings.TrimSpace(string(data))
	}
}

func anyString(value any) string {
	if s, ok := value.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func firstPresent(m map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			return value
		}
	}
	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
