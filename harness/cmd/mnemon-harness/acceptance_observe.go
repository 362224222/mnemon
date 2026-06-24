package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

var (
	acceptanceObserveJSON     bool
	acceptanceObserveWatch    bool
	acceptanceObserveOnce     bool
	acceptanceObserveInterval time.Duration
	acceptanceObserveLatestN  int
)

var acceptanceObserveCmd = &cobra.Command{
	Use:   "observe",
	Short: "Observe an acceptance run's mnemond and mnemonhub event state",
	RunE: func(cmd *cobra.Command, args []string) error {
		if !acceptanceObserveWatch {
			report, err := observeAcceptanceRun(acceptanceRunRoot, acceptanceObserveLatestN)
			if err != nil {
				return err
			}
			if acceptanceObserveJSON {
				return writeJSON(cmd.OutOrStdout(), report)
			}
			writeAcceptanceObserveText(cmd.OutOrStdout(), report)
			return nil
		}
		if acceptanceObserveInterval <= 0 {
			acceptanceObserveInterval = 2 * time.Second
		}
		for {
			report, err := observeAcceptanceRun(acceptanceRunRoot, acceptanceObserveLatestN)
			if err != nil {
				return err
			}
			if acceptanceObserveJSON {
				if err := writeJSON(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			} else {
				writeAcceptanceObserveWatchText(cmd.OutOrStdout(), report)
			}
			if acceptanceObserveOnce {
				return nil
			}
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-time.After(acceptanceObserveInterval):
			}
		}
	},
}

func init() {
	acceptanceObserveCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceObserveCmd.Flags().BoolVar(&acceptanceObserveJSON, "json", false, "emit JSON instead of text")
	acceptanceObserveCmd.Flags().IntVar(&acceptanceObserveLatestN, "latest", 5, "number of latest events to show per store")
	acceptanceObserveCmd.Flags().BoolVar(&acceptanceObserveWatch, "watch", false, "continue refreshing observation snapshots")
	acceptanceObserveCmd.Flags().DurationVar(&acceptanceObserveInterval, "interval", 2*time.Second, "watch refresh interval")
	acceptanceObserveCmd.Flags().BoolVar(&acceptanceObserveOnce, "once", false, "render one watch snapshot and exit")
	acceptanceCmd.AddCommand(acceptanceObserveCmd)
}

type acceptanceObserveReport struct {
	SchemaVersion int                         `json:"schema_version"`
	GeneratedAt   string                      `json:"generated_at"`
	RunRoot       string                      `json:"run_root"`
	Topology      acceptanceObserveTopology   `json:"topology"`
	Stores        []acceptanceStoreInspect    `json:"stores"`
	HubAudits     []acceptanceAuditInspect    `json:"hub_audits,omitempty"`
	RenderAudits  []acceptanceRenderAuditInfo `json:"render_audits,omitempty"`
	CrossEvents   []acceptanceCrossEvent      `json:"cross_events,omitempty"`
	Warnings      []string                    `json:"warnings,omitempty"`
}

type acceptanceObserveTopology struct {
	MnemondStores      int      `json:"mnemond_stores"`
	MnemonhubStores    int      `json:"mnemonhub_stores"`
	SharedMnemond      bool     `json:"shared_mnemond"`
	PerHostagent       bool     `json:"per_hostagent_mnemond"`
	Mode               string   `json:"mode"`
	DistinctStorePaths []string `json:"distinct_store_paths,omitempty"`
}

type acceptanceStoreInspect struct {
	Name                    string                     `json:"name"`
	Role                    string                     `json:"role"`
	Path                    string                     `json:"path"`
	Counts                  map[string]int             `json:"counts"`
	EnvelopeByPhase         map[string]int             `json:"envelope_by_phase,omitempty"`
	EnvelopeByType          map[string]int             `json:"envelope_by_type,omitempty"`
	SyncEventsByStatus      map[string]int             `json:"sync_events_by_status,omitempty"`
	RemoteEventsByStatus    map[string]int             `json:"remote_events_by_status,omitempty"`
	GovernedRowsByKind      map[string]int             `json:"governed_rows_by_kind,omitempty"`
	ImportedAcceptedByRef   map[string]int             `json:"imported_accepted_by_ref,omitempty"`
	ImportedRemoteDecisions map[string]int             `json:"imported_remote_decisions,omitempty"`
	LatestEnvelopes         []acceptanceEventSummary   `json:"latest_envelopes,omitempty"`
	LatestObserved          []acceptanceEventSummary   `json:"latest_observed,omitempty"`
	RenderAudit             *acceptanceRenderAuditInfo `json:"render_audit,omitempty"`
	Warnings                []string                   `json:"warnings,omitempty"`
	rawRemoteEventSummaries []acceptanceRemoteEventSummary
}

type acceptanceEventSummary struct {
	Seq           int64  `json:"seq"`
	Phase         string `json:"phase,omitempty"`
	Type          string `json:"type,omitempty"`
	Subject       string `json:"subject,omitempty"`
	Actor         string `json:"actor,omitempty"`
	DecisionID    string `json:"decision_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
}

type acceptanceRemoteEventSummary struct {
	RemoteSeq       int64  `json:"remote_seq"`
	RemotePeerID    string `json:"remote_peer_id"`
	OriginReplicaID string `json:"origin_replica_id"`
	LocalDecisionID string `json:"local_decision_id"`
	Actor           string `json:"actor"`
	ResourceKind    string `json:"resource_kind"`
	ResourceID      string `json:"resource_id"`
	ResourceVersion int64  `json:"resource_version"`
	Status          string `json:"status"`
	DecidedAt       string `json:"decided_at"`
}

type acceptanceRenderAuditInfo struct {
	Path               string             `json:"path"`
	Entries            int                `json:"entries"`
	Status             map[string]int     `json:"status,omitempty"`
	PresentationCounts map[string]int     `json:"presentation_counts,omitempty"`
	EventCounts        map[string]int     `json:"event_counts,omitempty"`
	Latest             []renderAuditEntry `json:"latest,omitempty"`
	Warnings           []string           `json:"warnings,omitempty"`
}

type renderAuditEntry struct {
	CreatedAt          string         `json:"created_at,omitempty"`
	AuditID            string         `json:"audit_id,omitempty"`
	Principal          string         `json:"principal,omitempty"`
	RenderIntent       string         `json:"render_intent,omitempty"`
	Status             string         `json:"status,omitempty"`
	PresentationCounts map[string]int `json:"presentation_counts,omitempty"`
	EventCounts        map[string]int `json:"event_counts,omitempty"`
}

type acceptanceAuditInspect struct {
	Path    string         `json:"path"`
	Lines   int            `json:"lines"`
	Verbs   map[string]int `json:"verbs,omitempty"`
	Results map[string]int `json:"results,omitempty"`
	Latest  []string       `json:"latest,omitempty"`
}

type acceptanceCrossEvent struct {
	HubStore        string   `json:"hub_store"`
	RemoteSeq       int64    `json:"remote_seq"`
	OriginReplicaID string   `json:"origin_replica_id"`
	LocalDecisionID string   `json:"local_decision_id"`
	Actor           string   `json:"actor"`
	EventSubject    string   `json:"event_subject"`
	Status          string   `json:"status"`
	ImportedBy      []string `json:"imported_by,omitempty"`
}

func observeAcceptanceRun(runRoot string, latest int) (acceptanceObserveReport, error) {
	if strings.TrimSpace(runRoot) == "" {
		return acceptanceObserveReport{}, fmt.Errorf("--run-root is required")
	}
	abs, err := filepath.Abs(runRoot)
	if err != nil {
		return acceptanceObserveReport{}, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return acceptanceObserveReport{}, err
	}
	if !info.IsDir() {
		return acceptanceObserveReport{}, fmt.Errorf("run root is not a directory: %s", abs)
	}
	if latest <= 0 {
		latest = 5
	}
	report := acceptanceObserveReport{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Truncate(time.Second).Format(time.RFC3339),
		RunRoot:       abs,
	}
	dbPaths, renderAuditPaths, hubAuditPaths, err := findAcceptanceArtifacts(abs)
	if err != nil {
		return acceptanceObserveReport{}, err
	}
	for _, path := range dbPaths {
		storeReport, err := inspectAcceptanceStore(abs, path, latest)
		if err != nil {
			storeReport = acceptanceStoreInspect{
				Name:     inferAcceptanceStoreName(abs, path),
				Role:     inferAcceptanceStoreRole(path),
				Path:     path,
				Counts:   map[string]int{},
				Warnings: []string{err.Error()},
			}
			report.Warnings = append(report.Warnings, fmt.Sprintf("inspect %s: %v", path, err))
		}
		if storeReport.Role == "mnemond" {
			if auditPath := colocatedRenderAudit(path, renderAuditPaths); auditPath != "" {
				audit, err := inspectRenderAudit(auditPath, latest)
				if err != nil {
					audit = acceptanceRenderAuditInfo{Path: auditPath, Warnings: []string{err.Error()}}
					report.Warnings = append(report.Warnings, fmt.Sprintf("render audit %s: %v", auditPath, err))
				}
				storeReport.RenderAudit = &audit
			}
		}
		report.Stores = append(report.Stores, storeReport)
	}
	for _, path := range renderAuditPaths {
		attached := false
		for _, storePath := range dbPaths {
			if colocatedRenderAudit(storePath, []string{path}) == path {
				attached = true
				break
			}
		}
		if attached {
			continue
		}
		audit, err := inspectRenderAudit(path, latest)
		if err != nil {
			audit = acceptanceRenderAuditInfo{Path: path, Warnings: []string{err.Error()}}
			report.Warnings = append(report.Warnings, fmt.Sprintf("render audit %s: %v", path, err))
		}
		report.RenderAudits = append(report.RenderAudits, audit)
	}
	for _, path := range hubAuditPaths {
		audit, err := inspectHubAudit(path, latest)
		if err != nil {
			audit = acceptanceAuditInspect{Path: path}
			report.Warnings = append(report.Warnings, fmt.Sprintf("hub audit %s: %v", path, err))
		}
		report.HubAudits = append(report.HubAudits, audit)
	}
	report.Topology = buildAcceptanceTopology(report.Stores)
	report.CrossEvents = buildAcceptanceCrossEvents(report.Stores)
	if len(dbPaths) == 0 {
		report.Warnings = append(report.Warnings, "no governed.db, mnemond.db, or hub.db files found")
	}
	if len(renderAuditPaths) == 0 {
		report.Warnings = append(report.Warnings, "no render-audit.jsonl files found")
	}
	if len(hubAuditPaths) == 0 {
		report.Warnings = append(report.Warnings, "no sync-audit.jsonl files found")
	}
	return report, nil
}

func findAcceptanceArtifacts(root string) (dbPaths, renderAuditPaths, hubAuditPaths []string, err error) {
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		switch d.Name() {
		case "governed.db", "mnemond.db", "hub.db":
			dbPaths = append(dbPaths, path)
		case "render-audit.jsonl":
			renderAuditPaths = append(renderAuditPaths, path)
		case "sync-audit.jsonl":
			hubAuditPaths = append(hubAuditPaths, path)
		}
		return nil
	})
	sort.Strings(dbPaths)
	sort.Strings(renderAuditPaths)
	sort.Strings(hubAuditPaths)
	return dbPaths, renderAuditPaths, hubAuditPaths, err
}

func inspectAcceptanceStore(root, path string, latest int) (acceptanceStoreInspect, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return acceptanceStoreInspect{}, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report := acceptanceStoreInspect{
		Name:   inferAcceptanceStoreName(root, path),
		Role:   inferAcceptanceStoreRole(path),
		Path:   path,
		Counts: map[string]int{},
	}
	for _, table := range []string{"events", "event_envelopes", "decisions", "resources", "sync_events", "sync_remote_events"} {
		if exists, err := sqliteTableExists(ctx, db, table); err != nil {
			return report, err
		} else if exists {
			count, err := sqliteCount(ctx, db, table)
			if err != nil {
				return report, err
			}
			countKey := table
			if table == "resources" {
				countKey = "governed_rows"
			}
			report.Counts[countKey] = count
		}
	}
	if report.Counts["event_envelopes"] > 0 {
		report.EnvelopeByPhase, err = sqliteGroupCount(ctx, db, "event_envelopes", "phase")
		if err != nil {
			return report, err
		}
		report.EnvelopeByType, err = sqliteGroupCount(ctx, db, "event_envelopes", "event_type")
		if err != nil {
			return report, err
		}
		report.LatestEnvelopes, err = sqliteLatestEventEnvelopes(ctx, db, latest)
		if err != nil {
			return report, err
		}
		report.ImportedAcceptedByRef, err = sqliteImportedAcceptedByRef(ctx, db)
		if err != nil {
			return report, err
		}
		report.Counts["imported_accepted"] = sumCountMap(report.ImportedAcceptedByRef)
	}
	if report.Counts["events"] > 0 {
		report.LatestObserved, err = sqliteLatestObservedEvents(ctx, db, latest)
		if err != nil {
			return report, err
		}
		report.Counts["remote_synced_observed"] = countRemoteSyncedObserved(report.LatestObserved)
		report.ImportedRemoteDecisions, err = sqliteImportedRemoteDecisions(ctx, db)
		if err != nil {
			return report, err
		}
		report.Counts["remote_synced_observed"] = sumCountMap(report.ImportedRemoteDecisions)
	}
	if report.Counts["sync_events"] > 0 {
		report.SyncEventsByStatus, err = sqliteGroupCount(ctx, db, "sync_events", "status")
		if err != nil {
			return report, err
		}
	}
	if report.Counts["sync_remote_events"] > 0 {
		report.RemoteEventsByStatus, err = sqliteGroupCount(ctx, db, "sync_remote_events", "status")
		if err != nil {
			return report, err
		}
		report.rawRemoteEventSummaries, err = sqliteRemoteEventSummaries(ctx, db, latest)
		if err != nil {
			return report, err
		}
	}
	if report.Counts["governed_rows"] > 0 {
		report.GovernedRowsByKind, err = sqliteGroupCount(ctx, db, "resources", "kind")
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

func sqliteTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return name == table, nil
}

func sqliteCount(ctx context.Context, db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count)
	return count, err
}

func sqliteGroupCount(ctx context.Context, db *sql.DB, table, column string) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+column+`, COUNT(*) FROM `+table+` GROUP BY `+column+` ORDER BY `+column)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var key string
		var count int
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		out[key] = count
	}
	return out, rows.Err()
}

func sqliteLatestEventEnvelopes(ctx context.Context, db *sql.DB, limit int) ([]acceptanceEventSummary, error) {
	rows, err := db.QueryContext(ctx, `
SELECT seq, phase, event_type, subject, actor, decision_id, correlation_id, created_at
FROM event_envelopes
ORDER BY seq DESC
LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []acceptanceEventSummary
	for rows.Next() {
		var rec acceptanceEventSummary
		if err := rows.Scan(&rec.Seq, &rec.Phase, &rec.Type, &rec.Subject, &rec.Actor, &rec.DecisionID, &rec.CorrelationID, &rec.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func sqliteLatestObservedEvents(ctx context.Context, db *sql.DB, limit int) ([]acceptanceEventSummary, error) {
	rows, err := db.QueryContext(ctx, `SELECT ingest_seq, payload FROM events ORDER BY ingest_seq DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []acceptanceEventSummary
	for rows.Next() {
		var seq int64
		var payload string
		if err := rows.Scan(&seq, &payload); err != nil {
			return nil, err
		}
		rec := acceptanceEventSummary{Seq: seq, Phase: "observed"}
		var raw map[string]any
		if err := json.Unmarshal([]byte(payload), &raw); err == nil {
			rec.Type, _ = raw["type"].(string)
			rec.CorrelationID, _ = raw["correlation_id"].(string)
			if actor, ok := raw["actor"].(string); ok {
				rec.Actor = actor
			}
			if ts, ok := raw["ts"].(string); ok {
				rec.CreatedAt = ts
			}
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func sqliteImportedRemoteDecisions(ctx context.Context, db *sql.DB) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT payload FROM events WHERE payload LIKE '%remote_synced_event.observed%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var raw struct {
			Payload struct {
				Material struct {
					OriginReplicaID string `json:"OriginReplicaID"`
					LocalDecisionID string `json:"LocalDecisionID"`
				} `json:"material"`
			} `json:"payload"`
		}
		if err := json.Unmarshal([]byte(payload), &raw); err != nil {
			continue
		}
		if raw.Payload.Material.OriginReplicaID == "" || raw.Payload.Material.LocalDecisionID == "" {
			continue
		}
		out[remoteDecisionKey(raw.Payload.Material.OriginReplicaID, raw.Payload.Material.LocalDecisionID)]++
	}
	return out, rows.Err()
}

func sqliteRemoteEventSummaries(ctx context.Context, db *sql.DB, limit int) ([]acceptanceRemoteEventSummary, error) {
	rows, err := db.QueryContext(ctx, `
SELECT remote_seq, remote_peer_id, origin_replica_id, local_decision_id, actor,
       resource_kind, resource_id, resource_version, status, decided_at
FROM sync_remote_events
ORDER BY remote_seq DESC
LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []acceptanceRemoteEventSummary
	for rows.Next() {
		var rec acceptanceRemoteEventSummary
		if err := rows.Scan(&rec.RemoteSeq, &rec.RemotePeerID, &rec.OriginReplicaID, &rec.LocalDecisionID, &rec.Actor, &rec.ResourceKind, &rec.ResourceID, &rec.ResourceVersion, &rec.Status, &rec.DecidedAt); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func sqliteImportedAcceptedByRef(ctx context.Context, db *sql.DB) (map[string]int, error) {
	rows, err := db.QueryContext(ctx, `
SELECT event_type, subject, COUNT(*)
FROM event_envelopes
WHERE actor='sync@local'
GROUP BY event_type, subject
ORDER BY event_type, subject`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var typ, subject string
		var count int
		if err := rows.Scan(&typ, &subject, &count); err != nil {
			return nil, err
		}
		out[typ+"|"+subject] = count
	}
	return out, rows.Err()
}

func inspectRenderAudit(path string, latest int) (acceptanceRenderAuditInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return acceptanceRenderAuditInfo{}, err
	}
	defer f.Close()
	info := acceptanceRenderAuditInfo{
		Path:               path,
		Status:             map[string]int{},
		PresentationCounts: map[string]int{},
		EventCounts:        map[string]int{},
	}
	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			info.Warnings = append(info.Warnings, "bad render audit json line: "+err.Error())
			continue
		}
		info.Entries++
		entry := renderAuditEntry{
			CreatedAt:    stringFromAny(raw["CreatedAt"], raw["created_at"]),
			AuditID:      stringFromAny(raw["AuditID"], raw["audit_id"]),
			Principal:    stringFromAny(raw["Principal"], raw["principal"]),
			RenderIntent: stringFromAny(raw["RenderIntent"], raw["render_intent"]),
			Status:       stringFromAny(raw["Status"], raw["status"]),
		}
		if entry.Status != "" {
			info.Status[entry.Status]++
		}
		entry.PresentationCounts = intMapFromAny(raw["PresentationCounts"], raw["presentation_counts"])
		entry.EventCounts = intMapFromAny(raw["EventCounts"], raw["event_counts"])
		for k, v := range entry.PresentationCounts {
			info.PresentationCounts[k] += v
		}
		for k, v := range entry.EventCounts {
			info.EventCounts[k] += v
		}
		info.Latest = append([]renderAuditEntry{entry}, info.Latest...)
		if len(info.Latest) > latest {
			info.Latest = info.Latest[:latest]
		}
	}
	if err := sc.Err(); err != nil {
		return info, err
	}
	return info, nil
}

func inspectHubAudit(path string, latest int) (acceptanceAuditInspect, error) {
	f, err := os.Open(path)
	if err != nil {
		return acceptanceAuditInspect{}, err
	}
	defer f.Close()
	info := acceptanceAuditInspect{
		Path:    path,
		Verbs:   map[string]int{},
		Results: map[string]int{},
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		info.Lines++
		if verb := tokenValue(line, "verb"); verb != "" {
			info.Verbs[verb]++
		}
		if result := tokenValue(line, "result"); result != "" {
			info.Results[result]++
		}
		info.Latest = append([]string{line}, info.Latest...)
		if len(info.Latest) > latest {
			info.Latest = info.Latest[:latest]
		}
	}
	if err := sc.Err(); err != nil {
		return info, err
	}
	return info, nil
}

func buildAcceptanceTopology(stores []acceptanceStoreInspect) acceptanceObserveTopology {
	top := acceptanceObserveTopology{}
	paths := map[string]bool{}
	hasLocalShared := false
	for _, st := range stores {
		switch st.Role {
		case "mnemond":
			top.MnemondStores++
			if st.Name == "local-shared" {
				hasLocalShared = true
			}
		case "mnemonhub":
			top.MnemonhubStores++
		}
		paths[st.Path] = true
	}
	for path := range paths {
		top.DistinctStorePaths = append(top.DistinctStorePaths, path)
	}
	sort.Strings(top.DistinctStorePaths)
	top.SharedMnemond = hasLocalShared || top.MnemondStores == 1
	top.PerHostagent = top.MnemondStores > 1
	switch {
	case top.SharedMnemond && top.PerHostagent:
		top.Mode = "mixed"
	case top.PerHostagent:
		top.Mode = "per-hostagent-mnemond"
	case top.SharedMnemond:
		top.Mode = "shared-mnemond"
	default:
		top.Mode = "unknown"
	}
	return top
}

func buildAcceptanceCrossEvents(stores []acceptanceStoreInspect) []acceptanceCrossEvent {
	var out []acceptanceCrossEvent
	for _, st := range stores {
		if st.Role != "mnemonhub" {
			continue
		}
		for _, remote := range st.rawRemoteEventSummaries {
			event := acceptanceCrossEvent{
				HubStore:        st.Name,
				RemoteSeq:       remote.RemoteSeq,
				OriginReplicaID: remote.OriginReplicaID,
				LocalDecisionID: remote.LocalDecisionID,
				Actor:           remote.Actor,
				EventSubject:    remote.ResourceKind + "/" + remote.ResourceID + "@" + strconv.FormatInt(remote.ResourceVersion, 10),
				Status:          remote.Status,
			}
			for _, target := range stores {
				if target.Role != "mnemond" {
					continue
				}
				if hasImportedRemoteEvent(target, remote) {
					event.ImportedBy = append(event.ImportedBy, target.Name)
				}
			}
			sort.Strings(event.ImportedBy)
			out = append(out, event)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].HubStore != out[j].HubStore {
			return out[i].HubStore < out[j].HubStore
		}
		return out[i].RemoteSeq < out[j].RemoteSeq
	})
	return out
}

func hasImportedRemoteEvent(store acceptanceStoreInspect, remote acceptanceRemoteEventSummary) bool {
	return store.ImportedRemoteDecisions[remoteDecisionKey(remote.OriginReplicaID, remote.LocalDecisionID)] > 0
}

func remoteDecisionKey(originReplicaID, localDecisionID string) string {
	return strings.TrimSpace(originReplicaID) + "|" + strings.TrimSpace(localDecisionID)
}

func colocatedRenderAudit(dbPath string, audits []string) string {
	sameDir := filepath.Join(filepath.Dir(dbPath), "render-audit.jsonl")
	for _, audit := range audits {
		if audit == sameDir {
			return audit
		}
	}
	dbDir := filepath.Dir(dbPath)
	best := ""
	bestDist := 100000
	for _, audit := range audits {
		rel, err := filepath.Rel(filepath.Dir(filepath.Dir(dbDir)), filepath.Dir(audit))
		if err != nil || strings.HasPrefix(rel, "..") {
			continue
		}
		dist := len(strings.Split(rel, string(os.PathSeparator)))
		if dist < bestDist {
			best = audit
			bestDist = dist
		}
	}
	return best
}

func inferAcceptanceStoreRole(path string) string {
	switch filepath.Base(path) {
	case "hub.db":
		return "mnemonhub"
	default:
		return "mnemond"
	}
}

func inferAcceptanceStoreName(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = path
	}
	parts := splitPath(rel)
	for i, part := range parts {
		if part == "workspaces" && i+1 < len(parts) {
			return parts[i+1]
		}
		if part == "nodes" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	if strings.Contains(rel, "local-workspace") {
		return "local-shared"
	}
	if strings.Contains(rel, "hub") || filepath.Base(path) == "hub.db" {
		return "mnemonhub"
	}
	if len(parts) > 1 {
		return parts[0]
	}
	return filepath.Base(filepath.Dir(path))
}

func splitPath(path string) []string {
	raw := strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	var out []string
	for _, part := range raw {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func countRemoteSyncedObserved(events []acceptanceEventSummary) int {
	count := 0
	for _, ev := range events {
		if strings.Contains(ev.Type, "remote_synced_event") {
			count++
		}
	}
	return count
}

func tokenValue(line, key string) string {
	prefix := key + "="
	for _, field := range strings.Fields(line) {
		if strings.HasPrefix(field, prefix) {
			return strings.TrimPrefix(field, prefix)
		}
	}
	return ""
}

func stringFromAny(values ...any) string {
	for _, v := range values {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intMapFromAny(values ...any) map[string]int {
	for _, value := range values {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		out := map[string]int{}
		for k, v := range raw {
			switch n := v.(type) {
			case float64:
				out[k] = int(n)
			case int:
				out[k] = n
			case json.Number:
				i, _ := n.Int64()
				out[k] = int(i)
			}
		}
		return out
	}
	return nil
}

func writeJSON(w io.Writer, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

func writeAcceptanceObserveText(w io.Writer, report acceptanceObserveReport) {
	fmt.Fprintf(w, "run_root: %s\n", report.RunRoot)
	fmt.Fprintf(w, "generated_at: %s\n", report.GeneratedAt)
	fmt.Fprintf(w, "topology: mode=%s mnemond=%d mnemonhub=%d shared_mnemond=%t per_hostagent=%t\n\n",
		report.Topology.Mode, report.Topology.MnemondStores, report.Topology.MnemonhubStores, report.Topology.SharedMnemond, report.Topology.PerHostagent)
	writeAcceptanceStoreTable(w, report.Stores)
	if len(report.CrossEvents) > 0 {
		fmt.Fprintln(w, "\ncross events:")
		for _, ev := range report.CrossEvents {
			fmt.Fprintf(w, "  hub=%s remote_seq=%d event_subject=%s actor=%s origin=%s decision=%s imported_by=%s\n",
				ev.HubStore, ev.RemoteSeq, ev.EventSubject, ev.Actor, ev.OriginReplicaID, ev.LocalDecisionID, strings.Join(ev.ImportedBy, ","))
		}
	}
	if len(report.HubAudits) > 0 {
		fmt.Fprintln(w, "\nhub audit:")
		for _, audit := range report.HubAudits {
			fmt.Fprintf(w, "  %s lines=%d verbs=%s results=%s\n", audit.Path, audit.Lines, formatCountMap(audit.Verbs), formatCountMap(audit.Results))
		}
	}
	if len(report.Warnings) > 0 {
		fmt.Fprintln(w, "\nwarnings:")
		for _, warning := range report.Warnings {
			fmt.Fprintf(w, "  - %s\n", warning)
		}
	}
}

func writeAcceptanceObserveWatchText(w io.Writer, report acceptanceObserveReport) {
	fmt.Fprintf(w, "[%s] topology mode=%s mnemond=%d mnemonhub=%d shared_mnemond=%t per_hostagent=%t\n\n",
		report.GeneratedAt, report.Topology.Mode, report.Topology.MnemondStores, report.Topology.MnemonhubStores, report.Topology.SharedMnemond, report.Topology.PerHostagent)
	writeAcceptanceStoreTable(w, report.Stores)
	if len(report.CrossEvents) > 0 {
		last := report.CrossEvents[len(report.CrossEvents)-1]
		fmt.Fprintf(w, "\nlatest chain: hub=%s remote_seq=%d event_subject=%s actor=%s imported_by=%s\n",
			last.HubStore, last.RemoteSeq, last.EventSubject, last.Actor, strings.Join(last.ImportedBy, ","))
	}
	if len(report.Warnings) > 0 {
		fmt.Fprintln(w, "\nwarnings:")
		for _, warning := range report.Warnings {
			fmt.Fprintf(w, "  - %s\n", warning)
		}
	}
	fmt.Fprintln(w)
}

func writeAcceptanceStoreTable(w io.Writer, stores []acceptanceStoreInspect) {
	sort.SliceStable(stores, func(i, j int) bool {
		if stores[i].Role != stores[j].Role {
			return stores[i].Role < stores[j].Role
		}
		return stores[i].Name < stores[j].Name
	})
	fmt.Fprintln(w, "store        role        observed accepted synced_out imported derived remote hub_received path")
	for _, st := range stores {
		observed := st.Counts["events"]
		accepted := st.EnvelopeByPhase["accepted"]
		syncedOut := st.Counts["sync_events"]
		imported := importedCount(st)
		derived := 0
		if st.RenderAudit != nil {
			derived = st.RenderAudit.Entries
		}
		remote := st.Counts["sync_remote_events"]
		fmt.Fprintf(w, "%-12s %-11s %-8d %-8d %-10d %-8d %-7d %-6d %-12d %s\n",
			st.Name, st.Role, observed, accepted, syncedOut, imported, derived, remote, remote, st.Path)
	}
}

func importedCount(st acceptanceStoreInspect) int {
	if count := sumCountMap(st.ImportedAcceptedByRef); count > 0 {
		return count
	}
	return st.Counts["remote_synced_observed"]
}

func sumCountMap(m map[string]int) int {
	total := 0
	for _, count := range m {
		total += count
	}
	return total
}

func formatCountMap(m map[string]int) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, m[key]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
