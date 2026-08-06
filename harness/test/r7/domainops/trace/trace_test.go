package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
)

func TestParseStoredEventRequiresExactCanonicalDigestAndColumns(t *testing.T) {
	row := validStoredEvent(t)
	event, err := parseStoredEvent("lead", row)
	if err != nil {
		t.Fatalf("parseStoredEvent() error = %v", err)
	}
	if event.ID != row.ID || event.Consequence != "handling.create" || event.SemanticKind != "work.request" {
		t.Fatalf("parsed Event = %+v", event)
	}
	if event.PayloadBytes != len("bounded test payload") ||
		!slices.Equal(event.Targets, []string{"principal:lead"}) {
		t.Fatalf("parsed Event metadata = payload bytes %d, targets %v",
			event.PayloadBytes, event.Targets)
	}

	tampered := row
	tampered.Digest = agency.Sum([]byte("different")).String()
	if _, err := parseStoredEvent("lead", tampered); err == nil {
		t.Fatal("parseStoredEvent() accepted a mismatched durable digest")
	}

	tampered = row
	tampered.Canonical = append([]byte{}, row.Canonical...)
	tampered.Canonical = bytes.Replace(tampered.Canonical,
		[]byte(`"kind":"work.request"`), []byte(`"kind":"work.changed"`), 1)
	tampered.Digest = agency.Sum(tampered.Canonical).String()
	changed, err := parseStoredEvent("lead", tampered)
	if err != nil {
		t.Fatalf("parseStoredEvent() judged open semantics: %v", err)
	}
	if bytes.Equal(tampered.Canonical, row.Canonical) || changed.SemanticKind != "work.changed" {
		t.Fatal("test did not alter canonical Event semantics")
	}
}

func TestLoadNodeDatabaseValidatesCanonicalOperationReceipt(t *testing.T) {
	path := createAuthorityFixture(t)
	node, err := loadNodeDatabase("lead", path)
	if err != nil {
		t.Fatalf("loadNodeDatabase() error = %v", err)
	}
	if len(node.Events) != 1 || len(node.Operations) != 1 ||
		node.Operations[0].Outcome != "accepted" {
		t.Fatalf("node evidence = %+v", node)
	}

	db := openFixture(t, path)
	if _, err := db.Exec(`UPDATE operations SET receipt_digest = ?`,
		agency.Sum([]byte("tampered receipt")).String()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := loadNodeDatabase("lead", path); err == nil {
		t.Fatal("loadNodeDatabase() accepted a tampered Receipt digest")
	}
}

func TestLoadNodeDatabaseRejectsWrongAuthorityIdentity(t *testing.T) {
	path := createAuthorityFixture(t)
	db := openFixture(t, path)
	if _, err := db.Exec(`PRAGMA application_id = 7`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := loadNodeDatabase("lead", path); err == nil {
		t.Fatal("loadNodeDatabase() accepted a database with the wrong authority identity")
	}
}

func TestLoadNodeDatabaseBindsReceiptActorToEventSource(t *testing.T) {
	path := createAuthorityFixture(t)
	db := openFixture(t, path)
	if _, err := db.Exec(`UPDATE operations SET actor_principal_id = ?`, "principal:other"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := loadNodeDatabase("lead", path); err == nil {
		t.Fatal("loadNodeDatabase() accepted a Receipt actor different from its Event source")
	}
}

func TestLoadNodeDatabaseReadsRealStoppedWALSnapshot(t *testing.T) {
	liveDirectory := t.TempDir()
	if err := os.Chmod(liveDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	livePath := filepath.Join(liveDirectory, "agency.db")
	store, err := authority.Open(context.Background(), livePath)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := agency.NewAgentPrincipalID("principal:wal-proof")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnrollPrincipal(context.Background(), principal); err != nil {
		store.Close()
		t.Fatal(err)
	}
	databaseBytes := readRequiredFile(t, livePath)
	walBytes := readRequiredFile(t, livePath+"-wal")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := t.TempDir()
	snapshotPath := filepath.Join(snapshot, "agency.db")
	if err := os.WriteFile(snapshotPath, databaseBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotPath+"-wal", walBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	node, err := loadNodeDatabase("lead", snapshotPath)
	if err != nil {
		t.Fatalf("loadNodeDatabase(real WAL snapshot) error = %v", err)
	}
	if node.Role != "lead" || len(node.Events) != 0 {
		t.Fatalf("real empty authority evidence = %+v", node)
	}
}

func TestValidateGlobalAuthorityRefsRequiresExactDigests(t *testing.T) {
	root := eventEvidence{ID: "event:root", Digest: agency.Sum([]byte("root")).String()}
	child := eventEvidence{ID: "event:child", Digest: agency.Sum([]byte("child")).String(),
		SubjectHead: &eventRefWire{ID: root.ID, Digest: root.Digest}}
	global := map[string]eventEvidence{root.ID: root, child.ID: child}
	if err := validateGlobalAuthorityRefs(global); err != nil {
		t.Fatalf("validateGlobalAuthorityRefs() error = %v", err)
	}
	child.SubjectHead.Digest = agency.Sum([]byte("wrong")).String()
	if err := validateGlobalAuthorityRefs(global); err == nil {
		t.Fatal("validateGlobalAuthorityRefs() accepted a mismatched subject digest")
	}
}

func TestValidateGlobalDeliveriesRequiresCollectedExactOrigin(t *testing.T) {
	digest := agency.Sum([]byte("origin")).String()
	delivery := deliveryEvidence{OriginEventID: "event:origin", OriginEventDigest: digest}
	nodes := []nodeEvidence{{Role: "data", Deliveries: []deliveryEvidence{delivery}}}
	if err := validateGlobalDeliveries(nodes, nil); err == nil {
		t.Fatal("validateGlobalDeliveries() accepted a missing origin Event")
	}
	global := map[string]eventEvidence{
		delivery.OriginEventID: {ID: delivery.OriginEventID, Digest: digest},
	}
	if err := validateGlobalDeliveries(nodes, global); err != nil {
		t.Fatalf("validateGlobalDeliveries() error = %v", err)
	}
	global[delivery.OriginEventID] = eventEvidence{ID: delivery.OriginEventID,
		Digest: agency.Sum([]byte("different")).String()}
	if err := validateGlobalDeliveries(nodes, global); err == nil {
		t.Fatal("validateGlobalDeliveries() accepted a mismatched origin digest")
	}
}

func TestValidateReportRequiresAllIndependentGates(t *testing.T) {
	report := validReport()
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport() error = %v", err)
	}
	report.Isolation.Passed = false
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted a failed isolation oracle")
	}
	report = validReport()
	report.Isolation.FreshRuntimeBetweenEpisodes = false
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted reused cross-episode Runtime state")
	}
	report = validReport()
	report.Turns[0].PrivateBindingProbes = 1
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted a private binding probe")
	}
	report = validReport()
	report.Turns[0].IntentSubmits = 1
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted an unaccounted Intent submission")
	}
	report = validReport()
	report.Turns[0].BashCalls = 1
	report.Turns[0].SubmitAttempts = 3
	report.Turns[0].IntentSubmits = 1
	report.Turns[0].AcceptedReceipts = 1
	report.Turns[0].SubmitDenials = 2
	report.Turns[0].SubmitControlDenials = []controlDenial{{Code: "context_required", Count: 2}}
	report.Turns[0].PostAcceptDenials = 2
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport() rejected one Effect plus two closed denials: %v", err)
	}
	report.Turns[0].IntentSubmits = 2
	report.Turns[0].AcceptedReceipts = 2
	report.Turns[0].SubmitDenials = 1
	report.Turns[0].SubmitControlDenials[0].Count = 1
	report.Turns[0].PostAcceptDenials = 1
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted two Effects in one turn")
	}
	report = validReport()
	report.Turns[0].BashCalls = 1
	report.Turns[0].SubmitAttempts = 1
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted an unaccounted submit attempt")
	}
}

func TestWriteTraceKeepsRuntimeAndAuthorityEvidenceSeparate(t *testing.T) {
	report := validReport()
	rootDigest := agency.Sum([]byte("root Event")).String()
	remoteDigest := agency.Sum([]byte("remote Event")).String()
	root := eventEvidence{Node: "lead", ID: "event:root", Digest: rootDigest,
		AcceptedAt: time.Date(2026, 8, 4, 1, 0, 10, 0, time.UTC), OriginSequence: 1,
		SourcePrincipal: "principal:lead", RequestDigest: agency.Sum([]byte("root request")).String(),
		SemanticKind: "work.request", Consequence: "handling.create"}
	referenceDigest := agency.Sum([]byte("retained Reference Event")).String()
	artifactDigest := agency.Sum([]byte("retained operating knowledge")).String()
	reference := eventEvidence{Node: "lead", ID: "event:reference", Digest: referenceDigest,
		AcceptedAt: time.Date(2026, 8, 4, 1, 0, 10, 500000000, time.UTC), OriginSequence: 2,
		SourcePrincipal: "principal:lead",
		RequestDigest:   agency.Sum([]byte("reference request")).String(),
		SemanticKind:    "knowledge.keep", Consequence: "reference.publish",
		ReferenceKey: "opaque.test", Artifacts: []string{artifactDigest}}
	evolved := eventEvidence{Node: "lead", ID: "event:evolution",
		Digest:     agency.Sum([]byte("later Event")).String(),
		AcceptedAt: time.Date(2026, 8, 4, 1, 0, 13, 0, time.UTC), OriginSequence: 3,
		SourcePrincipal: "principal:lead",
		RequestDigest:   agency.Sum([]byte("later request")).String(),
		SemanticKind:    "knowledge.use", Consequence: "handling.create",
		Targets:   []string{"principal:lead"},
		Causation: []eventRefWire{{ID: reference.ID, Digest: reference.Digest}}}
	remote := eventEvidence{Node: "data", ID: "event:remote", Digest: remoteDigest,
		AcceptedAt: time.Date(2026, 8, 4, 1, 0, 11, 0, time.UTC), OriginSequence: 1,
		CausalDepth: 1, SourcePrincipal: "principal:lead-surrogate",
		RequestDigest: agency.Sum([]byte("delivery envelope")).String(),
		SemanticKind:  "work.request", Consequence: "handling.create",
		Causation: []eventRefWire{{ID: root.ID, Digest: root.Digest}}}
	terminal := eventEvidence{Node: "data", ID: "event:terminal",
		Digest:     agency.Sum([]byte("terminal Event")).String(),
		AcceptedAt: time.Date(2026, 8, 4, 1, 0, 12, 0, time.UTC), OriginSequence: 2,
		CausalDepth: 1, SourcePrincipal: "principal:data",
		RequestDigest:   agency.Sum([]byte("terminal request")).String(),
		SemanticKind:    "work.result",
		Consequence:     "handling.resolve.completed",
		SubjectHandling: "handling:remote",
		Causation:       []eventRefWire{{ID: remote.ID, Digest: remote.Digest}}}
	proof := evidence{Report: report, Scenario: scenarioEvidence{
		Digest: agency.Sum([]byte("scenario fixture and candidate binaries")).String()}}
	for _, role := range domainRoles {
		proof.Nodes = append(proof.Nodes, nodeEvidence{Role: role})
	}
	for index := range proof.Nodes {
		switch proof.Nodes[index].Role {
		case "lead":
			proof.Nodes[index].Events = []eventEvidence{root, reference, evolved}
			proof.Nodes[index].Artifacts = []artifactEvidence{{Node: "lead",
				Digest: artifactDigest, ByteSize: 28, VerifiedAt: reference.AcceptedAt}}
			proof.Nodes[index].References = []referenceEvidence{{Node: "lead",
				EventID: reference.ID, State: "active", ArtifactDigest: artifactDigest}}
			proof.Nodes[index].Operations = []operationEvidence{{Node: "lead",
				Digest: agency.Sum([]byte("receipt")).String(), Outcome: "accepted",
				RecordedAt: root.AcceptedAt.Add(time.Second), EventID: root.ID, EventDigest: root.Digest}}
			proof.Nodes[index].Handlings = []handlingEvidence{{Node: "lead", ID: "handling:root",
				TargetPrincipal: "principal:lead", HeadEventID: root.ID, State: "open", CreatedSequence: 1},
				{Node: "lead", ID: "handling:evolved", TargetPrincipal: "principal:lead",
					HeadEventID: evolved.ID, State: "open", CreatedSequence: 3}}
		case "data":
			proof.Nodes[index].Events = []eventEvidence{remote, terminal}
			proof.Nodes[index].Handlings = []handlingEvidence{{Node: "data", ID: "handling:remote",
				TargetPrincipal: "principal:data", HeadEventID: remote.ID, State: "open", CreatedSequence: 1}}
			proof.Nodes[index].Deliveries = []deliveryEvidence{{Node: "data", Direction: "inbox",
				ID: "delivery:" + strings.Repeat("a", 64), State: "settled",
				CapturedAt: remote.AcceptedAt.Add(time.Second), OriginEventID: root.ID,
				OriginEventDigest: root.Digest, LocalEventID: remote.ID,
				LocalEventDigest: remote.Digest, Accepted: true}}
			proof.Nodes[index].PeerEffects = 1
		}
	}
	var output bytes.Buffer
	if err := writeTrace(&output, proof); err != nil {
		t.Fatalf("writeTrace() error = %v", err)
	}
	if !strings.Contains(output.String(), `"state":"terminal"`) ||
		strings.Contains(output.String(), `"state":"resolved"`) {
		t.Fatal("trace did not preserve the canonical terminal Handling state")
	}
	assertTraceSeparation(t, output.String())
	if !strings.Contains(output.String(), `"principal":"principal:lead"`) ||
		!strings.Contains(output.String(), `"payload_bytes":0`) ||
		!strings.Contains(output.String(), `"target_count":0`) {
		t.Fatal("trace omitted bounded source, payload-length, or target metadata")
	}
}

func TestScenarioDigestBindsFixtureAndCandidateBinaries(t *testing.T) {
	root := t.TempDir()
	for _, relative := range scenarioFiles {
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("fixture:"+relative+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := filepath.Join(t.TempDir(), "candidate.sha256")
	writeCandidateManifest(t, manifest, "a")
	first, err := loadScenarioEvidence(root, manifest)
	if err != nil {
		t.Fatalf("loadScenarioEvidence() error = %v", err)
	}
	if first.Digest == "" || len(first.Files) != len(scenarioFiles) ||
		len(first.Binaries) != len(candidateBinaryPaths) {
		t.Fatalf("scenario evidence = %+v", first)
	}
	mission := filepath.Join(root, "testdata/r7/domain-ops/mission.md")
	if err := os.WriteFile(mission, []byte("changed mission\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := loadScenarioEvidence(root, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest == second.Digest {
		t.Fatal("scenario digest ignored a changed mission")
	}
	writeCandidateManifest(t, manifest, "b")
	third, err := loadScenarioEvidence(root, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if second.Digest == third.Digest {
		t.Fatal("scenario digest ignored changed candidate binaries")
	}
	extra := filepath.Join(root, "testdata/r7/domain-ops/world/unbound.go")
	if err := os.WriteFile(extra, []byte("package world\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadScenarioEvidence(root, manifest); err == nil {
		t.Fatal("scenario identity ignored an unbound fixture file")
	}
}

func TestFailureTracePreservesAuthorityButCannotPass(t *testing.T) {
	report := validFailureReport()
	scenario := scenarioEvidence{Digest: agency.Sum([]byte("failed scenario")).String()}
	root := eventEvidence{Node: "lead", ID: "event:failed-root",
		Digest:     agency.Sum([]byte("failed root Event")).String(),
		AcceptedAt: time.Date(2026, 8, 4, 1, 0, 10, 0, time.UTC), OriginSequence: 1,
		SourcePrincipal: "principal:lead", SemanticKind: "ops.ask",
		Consequence: "handling.create", PayloadBytes: 127,
		Targets: []string{"data"}}
	nodes := []nodeEvidence{{Role: "lead", Events: []eventEvidence{root},
		Handlings: []handlingEvidence{{Node: "lead", ID: "handling:failed",
			TargetPrincipal: "principal:lead", HeadEventID: root.ID, State: "open",
			CreatedSequence: 1}}}}
	var output bytes.Buffer
	if err := writeFailureTrace(&output, report, scenario, nodes); err != nil {
		t.Fatalf("writeFailureTrace() error = %v", err)
	}
	trace := output.String()
	if !strings.Contains(trace, `"status":"failed"`) ||
		!strings.Contains(trace, `"semantic_kind":"ops.ask"`) ||
		!strings.Contains(trace, `"principal":"principal:lead"`) ||
		!strings.Contains(trace, `"payload_bytes":127`) ||
		!strings.Contains(trace, `"targets":["data"]`) {
		t.Fatalf("failed trace omitted status or bounded authority summary:\n%s", trace)
	}
	if strings.Contains(trace, `"status":"passed"`) || strings.Contains(trace, `"payload":`) {
		t.Fatal("failed trace claimed success or retained semantic payload")
	}
}

func TestFailureReportRejectsProviderMaterialAndNoncanonicalOutcome(t *testing.T) {
	report := validFailureReport()
	if err := validateFailureReport(report); err != nil {
		t.Fatalf("validateFailureReport() error = %v", err)
	}
	report.RawProviderStreamsRetained = true
	if err := validateFailureReport(report); err == nil {
		t.Fatal("failure report retained provider streams")
	}
	report = validFailureReport()
	report.Status = "passed"
	if err := validateFailureReport(report); err == nil {
		t.Fatal("failure report accepted a passed outcome")
	}
}

func writeCandidateManifest(t *testing.T, path, digit string) {
	t.Helper()
	var manifest strings.Builder
	for _, binary := range candidateBinaryPaths {
		manifest.WriteString(strings.Repeat(digit, 64))
		manifest.WriteString("  ")
		manifest.WriteString(binary)
		manifest.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(manifest.String()), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertTraceSeparation(t *testing.T, trace string) {
	t.Helper()
	runtimeIDs := make(map[string]struct{})
	r8Facts := 0
	foundR8Gate := false
	for _, line := range strings.Split(strings.TrimSpace(trace), "\n") {
		var record testTraceRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		factR8, hasR8Gate := inspectTestTraceRecord(t, record, runtimeIDs)
		r8Facts += factR8
		foundR8Gate = foundR8Gate || hasR8Gate
	}
	if !foundR8Gate {
		t.Fatal("trace result omitted the explicit R8 not_applicable gate")
	}
	if r8Facts != 0 {
		t.Fatalf("R7-only trace contains %d R8 facts", r8Facts)
	}
}

type testTraceRecord struct {
	Record string   `json:"record"`
	ID     string   `json:"id"`
	Kind   string   `json:"kind"`
	Causes []string `json:"causes"`
	Gates  []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"gates"`
}

func inspectTestTraceRecord(t *testing.T, record testTraceRecord,
	runtimeIDs map[string]struct{},
) (int, bool) {
	t.Helper()
	if strings.HasPrefix(record.Kind, "runtime.") {
		runtimeIDs[record.ID] = struct{}{}
		if len(record.Causes) != 0 {
			t.Fatalf("runtime Fact %q has inferred causes", record.ID)
		}
	}
	if strings.HasPrefix(record.Kind, "r7.") {
		for _, cause := range record.Causes {
			if _, inferred := runtimeIDs[cause]; inferred {
				t.Fatalf("R7 Fact %q infers a runtime-to-authority edge", record.ID)
			}
		}
	}
	r8Facts := 0
	if strings.HasPrefix(record.Kind, "r8.") {
		r8Facts = 1
	}
	foundGate := false
	for _, gate := range record.Gates {
		foundGate = foundGate || gate.ID == "r8.applicability" && gate.Status == "not_applicable"
	}
	return r8Facts, foundGate
}

func validStoredEvent(t *testing.T) storedEventRow {
	t.Helper()
	accepted := time.Date(2026, 8, 4, 1, 2, 3, 4, time.UTC)
	requestDigest := agency.Sum([]byte("request")).String()
	var wire eventWire
	wire.SchemaVersion = 2
	wire.Machine.EventID = "event:one"
	wire.Machine.AcceptedAt = accepted.Format(time.RFC3339Nano)
	wire.Machine.OriginSequence = 1
	wire.Machine.SourcePrincipal = "principal:lead"
	wire.Machine.OperationKey = "operation:one"
	wire.Machine.RequestDigest = requestDigest
	wire.Machine.Consequence = "handling.create"
	wire.Machine.Targets = []targetWire{{Destination: "local", LocalPrincipal: "principal:lead"}}
	wire.Semantic.Kind = "work.request"
	wire.Semantic.Payload = "bounded test payload"
	canonical, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return storedEventRow{ID: wire.Machine.EventID, Digest: agency.Sum(canonical).String(),
		OriginSequence: 1, SourcePrincipal: wire.Machine.SourcePrincipal,
		RequestDigest: requestDigest, CausalDepth: 0,
		AcceptedAt: accepted.Format("2006-01-02T15:04:05.000000000Z"), Canonical: canonical}
}

func createAuthorityFixture(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, "agency.db")
	db := openFixture(t, path)
	schema := `
PRAGMA application_id = 1296978487;
PRAGMA user_version = 9;
CREATE TABLE events(event_id TEXT PRIMARY KEY,event_digest TEXT,origin_sequence INTEGER,
 source_principal_id TEXT,request_digest TEXT,causal_depth INTEGER,accepted_at TEXT,canonical_json BLOB);
CREATE TABLE verified_artifacts(digest TEXT PRIMARY KEY,byte_size INTEGER,verified_at TEXT);
CREATE TABLE event_artifacts(event_id TEXT,artifact_digest TEXT);
CREATE TABLE operations(actor_principal_id TEXT,operation_key TEXT,request_digest TEXT,
 outcome TEXT,event_id TEXT,receipt_digest TEXT,receipt_json BLOB,recorded_at TEXT);
CREATE TABLE handlings(handling_id TEXT,target_principal_id TEXT,head_event_id TEXT,state TEXT,
 outcome TEXT,created_sequence INTEGER);
CREATE TABLE reference_lineage(event_id TEXT,reference_key TEXT,previous_event_id TEXT,
 state TEXT,artifact_digest TEXT);
CREATE TABLE peer_outbox(delivery_id TEXT,route_id TEXT,origin_event_id TEXT,envelope_digest TEXT,
 delivery_json BLOB,state TEXT,created_at TEXT,settled_at TEXT,receipt_digest TEXT,receipt_json BLOB);
CREATE TABLE peer_inbox(delivery_id TEXT,route_id TEXT,envelope_digest TEXT,delivery_json BLOB,
 state TEXT,received_at TEXT,settled_at TEXT,local_event_id TEXT,receipt_digest TEXT,receipt_json BLOB);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	event := validStoredEvent(t)
	if _, err := db.Exec(`INSERT INTO events VALUES(?,?,?,?,?,?,?,?)`, event.ID, event.Digest,
		event.OriginSequence, event.SourcePrincipal, event.RequestDigest, event.CausalDepth,
		event.AcceptedAt, event.Canonical); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO handlings VALUES(?,?,?,?,?,?)`, "handling:one",
		"principal:lead", event.ID, "open", nil, 1); err != nil {
		t.Fatal(err)
	}
	recorded := time.Date(2026, 8, 4, 1, 2, 4, 0, time.UTC)
	receiptWire := struct {
		SchemaVersion int          `json:"schema_version"`
		OperationKey  string       `json:"operation_key"`
		RequestDigest string       `json:"request_digest"`
		Outcome       string       `json:"outcome"`
		RecordedAt    string       `json:"recorded_at"`
		Event         eventRefWire `json:"event"`
	}{1, "operation:one", event.RequestDigest, "accepted", recorded.Format(time.RFC3339Nano),
		eventRefWire{ID: event.ID, Digest: event.Digest}}
	receipt, err := json.Marshal(receiptWire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)`, "principal:lead",
		"operation:one", event.RequestDigest, "accepted", event.ID,
		agency.Sum(receipt).String(), receipt,
		recorded.Format("2006-01-02T15:04:05.000000000Z")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func openFixture(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func readRequiredFile(t *testing.T, path string) []byte {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(value) == 0 {
		t.Fatalf("%s is empty", path)
	}
	return value
}

func validReport() liveReport {
	var report liveReport
	report.Schema, report.Version, report.Status = "mnemon.r7.domain-ops.live-report", 4, "passed"
	report.Model, report.Rounds = "deepseek-v4-flash", 1
	report.Run = runReport{ID: "domain-ops-test", StartedAt: "2026-08-04T01:00:00Z",
		FinishedAt: "2026-08-04T01:01:00Z", CandidateDigest: agency.Sum([]byte("candidate")).String()}
	report.Isolation.Passed = true
	report.Isolation.FreshRuntimeBetweenEpisodes = true
	sequence := int64(0)
	for episodeNumber := 1; episodeNumber <= 2; episodeNumber++ {
		episode := episodeReport{ID: fmt.Sprintf("episode-%d", episodeNumber)}
		episode.Baseline, episode.IncidentCharges = receiptEvidence(
			fmt.Sprintf("incident-%d", episodeNumber), 4, 2, 1, &sequence)
		episode.Baseline.Observed.Gateway.Route = []string{"east", "west"}[episodeNumber-1]
		episode.Baseline.Observed.Ledger = ledgerStatus{Charges: 8, ActiveCharges: 8,
			UniqueBusinesses: 4, DuplicateBusinesses: 4}
		episode.Recovery, episode.RecoveryCharges = receiptEvidence(
			fmt.Sprintf("recovery-%d", episodeNumber), 6, 1, 0, &sequence)
		episode.Stability, episode.StabilityCharges = receiptEvidence(
			fmt.Sprintf("stability-%d", episodeNumber), 6, 1, 0, &sequence)
		episode.IncidentAfter = domainResult{Role: "data", Result: ledgerStatus{
			Charges: 8, ActiveCharges: 4, VoidedCharges: 4, UniqueBusinesses: 4}}
		report.World.Episodes = append(report.World.Episodes, episode)
	}
	report.Protocol.AcceptedPeerEffects = 1
	for _, role := range domainRoles {
		count := 0
		if role == "data" {
			count = 1
		}
		report.Protocol.ByReceiver = append(report.Protocol.ByReceiver,
			peerEffectSummary{Role: role, AcceptedPeerEffects: count})
	}
	for _, phase := range []string{"episode-1-initial-lead", "episode-1-round-1",
		"episode-1-round-post-outcome", "episode-2-initial-lead", "episode-2-round-1"} {
		barrier := deliveryQuiescenceSummary{Phase: phase, Status: "quiescent", Attempts: 1}
		for _, role := range domainRoles {
			barrier.Nodes = append(barrier.Nodes, deliveryNodeOccupancySummary{Role: role})
		}
		report.Protocol.DeliveryQuiescence = append(report.Protocol.DeliveryQuiescence, barrier)
	}
	for episode := 1; episode <= 2; episode++ {
		settlement := firstAttentionSettlement{Episode: fmt.Sprintf("episode-%d", episode),
			Status: "settled", TurnLimit: firstAttentionTurnLimit}
		for _, role := range domainRoles {
			settlement.Final = append(settlement.Final, firstAttentionNode{Role: role})
		}
		report.Protocol.FirstAttention = append(report.Protocol.FirstAttention, settlement)
	}
	for episode := 1; episode <= 2; episode++ {
		report.Turns = append(report.Turns, turnSummary{Role: "lead",
			Turn:       fmt.Sprintf("episode-%d-initial-lead", episode),
			CapturedAt: "2026-08-04T01:00:30Z", HookCues: 1, AgentEnd: true})
		for _, role := range domainRoles {
			report.Turns = append(report.Turns, turnSummary{Role: role,
				Turn:       fmt.Sprintf("episode-%d-round-1-%s", episode, role),
				CapturedAt: "2026-08-04T01:00:45Z", HookCues: 1, AgentEnd: true})
		}
	}
	for _, role := range domainRoles {
		report.Turns = append(report.Turns, turnSummary{Role: role,
			Turn:       "episode-1-round-post-outcome-" + role,
			CapturedAt: "2026-08-04T01:00:50Z", HookCues: 1, AgentEnd: true})
	}
	populateValidEvolution(&report)
	return report
}

func populateValidEvolution(report *liveReport) {
	referenceDigest := agency.Sum([]byte("retained Reference Event")).String()
	report.Protocol.Evolution.Boundary.ActiveHeadCount = 1
	for _, role := range domainRoles {
		boundary := evolutionBoundaryNode{Role: role, MaxOriginSequence: 0}
		if role == "lead" {
			boundary.ConsolidationAfterSequence = 1
			boundary.MaxOriginSequence = 2
			boundary.ActiveHeads = []evolutionReferenceHead{{
				EventID: "event:reference", EventDigest: referenceDigest}}
		}
		report.Protocol.Evolution.Boundary.Nodes = append(
			report.Protocol.Evolution.Boundary.Nodes, boundary)
		effect := evolutionNodeSummary{Role: role,
			BoundarySequence: boundary.MaxOriginSequence,
			ActiveHeadCount:  len(boundary.ActiveHeads)}
		if role == "lead" {
			effect.AcceptedReferenceUses = 1
			effect.Matches = []evolutionMatchReport{{EventID: "event:evolution",
				ReferenceEventID: "event:reference", ReferenceDigest: referenceDigest}}
		}
		report.Protocol.Evolution.Effects = append(report.Protocol.Evolution.Effects, effect)
	}
	report.Protocol.Evolution.AcceptedReferenceUses = 1
}

func validFailureReport() failureReport {
	var report failureReport
	report.Schema, report.Version, report.Status =
		"mnemon.r7.domain-ops.failure-report", 3, "failed"
	report.Model = "deepseek-v4-flash"
	report.Run = runReport{ID: "domain-ops-failed", StartedAt: "2026-08-04T01:00:00Z",
		FinishedAt:      "2026-08-04T01:01:00Z",
		CandidateDigest: agency.Sum([]byte("candidate")).String()}
	report.Failure.Code = "scenario.reconciliation"
	report.Failure.ObservedAt = "2026-08-04T01:00:59Z"
	report.Turns = []turnSummary{{Role: "lead", Turn: "initial-lead",
		CapturedAt: "2026-08-04T01:00:30Z", HookCues: 1, AgentEnd: true}}
	return report
}

func freshSummary(prefix string, count int) loadSummary {
	result := loadSummary{Prefix: prefix, Sent: count, Accepted: count,
		Observed: monitorStatus{Ledger: ledgerStatus{Charges: count,
			ActiveCharges: count, UniqueBusinesses: count}}}
	for index := 1; index <= count; index++ {
		result.Receipts = append(result.Receipts,
			serviceReceipt{BusinessID: fmt.Sprintf("%s-%d", prefix, index), CaptureID: int64(index)})
	}
	return result
}

func receiptEvidence(prefix string, count, copies, voided int,
	sequence *int64,
) (loadSummary, domainChargeResult) {
	summary := freshSummary(prefix, count)
	charges := domainChargeResult{Role: "data"}
	for index := range summary.Receipts {
		for copyIndex := 0; copyIndex < copies; copyIndex++ {
			(*sequence)++
			state, reason := "active", ""
			if copyIndex >= copies-voided {
				state, reason = "voided", "fixture reconciliation"
			}
			if copyIndex == 0 {
				summary.Receipts[index].CaptureID = *sequence
			}
			charges.Result = append(charges.Result, chargeRecord{Sequence: *sequence,
				BusinessID: summary.Receipts[index].BusinessID,
				AttemptKey: fmt.Sprintf("%s-attempt-%d-%d", prefix, index, copyIndex),
				State:      state, VoidReason: reason})
		}
	}
	return summary, charges
}
