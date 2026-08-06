package attach

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestLoadHasOneFixedCueAndNoAuthorityOrSecretSurface(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	guide, cue, extension := projection.Guide(), projection.HookCue(), projection.PiExtension()
	if len(guide) == 0 || len(guide) > MaxGuideBytes || cue == "" || len(cue) > MaxCueBytes ||
		len(extension) == 0 || len(extension) > MaxExtensionBytes {
		t.Fatalf("asset sizes = guide %d, cue %d, extension %d", len(guide), len(cue), len(extension))
	}
	if !strings.Contains(cue, ".pi/skills/mnemond/SKILL.md") {
		t.Fatal("fixed cue does not name the installed, bounded guide projection")
	}
	assertGuideTerminalSurface(t, string(guide))
	source := string(extension)
	for _, required := range []string{
		`pi.on("before_agent_start"`, `execFileSync("mnemon-harness"`,
		`["hook", "attach", "--json"]`, `["hook", "end", "--json"]`,
		`pi.on("session_shutdown"`, `randomBytes(32).toString("base64url")`,
		`stdio: ["pipe", "ignore", "ignore"]`, `input: boundaryEnvelope(boundary)`,
		`timeout: ATTACH_TIMEOUT_MS`, `content: HOOK_CUE`, `display: false`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Pi extension lacks %q", required)
		}
	}
	if strings.Count(source, "content:") != 1 || !strings.Contains(source, cue) {
		t.Fatal("fixed cue is not the extension's unique model-content source")
	}
	for _, forbidden := range []string{`pi.on("turn_end"`, `pi.on("agent_end"`} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("Pi extension uses non-Host boundary callback %q", forbidden)
		}
	}
	all := strings.ToLower(string(guide) + "\n" + cue + "\n" + source)
	for _, forbidden := range []string{
		"review", "workflow", "case", "contract-net", "blackboard", "memory.wiki",
		"--event-id", "--operation-id", "--principal", "--fence", "--peer-id",
		"deepseek", "api_key", "api-key", "authorization:", "bearer ", "sk-",
		"process.env", "content: output", "content: result", "json.parse(",
		"model:", "provider:",
	} {
		if strings.Contains(all, forbidden) {
			t.Fatalf("projection contains forbidden surface %q", forbidden)
		}
	}
	guide[0] ^= 0xff
	extension[0] ^= 0xff
	if bytes.Equal(guide, projection.Guide()) || bytes.Equal(extension, projection.PiExtension()) {
		t.Fatal("Load returned mutable embedded assets")
	}
}

func assertGuideTerminalSurface(t *testing.T, guide string) {
	t.Helper()
	for _, required := range []string{
		"`mnemon-harness` is already on",
		"mnemon-harness agent current --json",
		"mnemon-harness artifact capture --json < PATH",
		"mnemon-harness artifact read \"$HANDLE\"",
		"mnemon-harness agent submit --json <<'JSON'",
		"exactly one nonempty",
		"no Markdown",
		"VIEW_TARGET", "VIEW_REPLY_TARGET", "CURRENT_HANDLE", "CAPTURE_HANDLE",
	} {
		if !strings.Contains(guide, required) {
			t.Errorf("guide lacks complete, bounded terminal surface %q", required)
		}
	}
	normalized := strings.Join(strings.Fields(guide), " ")
	for _, required := range []string{
		"Current is the local anchor; self creates responsibility, not reply keepalive",
		"self anchors the outcome. Sending is not completion",
		"Artifact is only the completed floor: locally verify the requested contribution first",
		"`current.facts.reply_required` is machine-owned",
		"When true and current asks for evidence, action, or a decision, return one correlated terminal disposition",
		"including declined or unresolved; never close silently",
		"When false, no response is owed to the authenticated sender: do not echo receipt",
		"New remote work remains allowed under ordinary anchor rules",
		"A report, duplicate/stale input, Receipt, or correlated response closes locally if no work remains; never acknowledge it",
		"If evidence is missing but a View target can obtain it, advance and ask that target rather than claim completion",
		"References stay local; publish/supersede never sends them to peers",
	} {
		if !strings.Contains(normalized, required) {
			t.Errorf("guide lacks response convergence rule %q", required)
		}
	}
	if strings.Contains(guide, "$INTENT_JSON") {
		t.Error("guide relies on an undefined cross-tool shell variable")
	}
}

func TestGuideResponseExampleAtomicallyClosesAndReturnsCorrelatedEvidence(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?m)^(\{"kind":"work\.response"[^\n]+\})$`).
		FindStringSubmatch(string(projection.Guide()))
	if len(match) != 2 {
		t.Fatal("guide lacks one complete work.response example")
	}
	intent, err := agency.ParseAgentIntentJSON([]byte(match[1]))
	if err != nil {
		t.Fatalf("guide work.response is not a valid AgentIntent: %v", err)
	}
	successors := intent.Successors()
	if intent.Consequence() != agency.ConsequenceResolveCompleted ||
		intent.SubjectHandling().IsZero() || len(successors) != 1 || successors[0].IsSelf() ||
		successors[0].Alias().IsZero() || intent.CorrelationHandle().IsZero() || len(intent.Artifacts()) == 0 {
		t.Fatal("guide work.response does not close its subject while returning correlated evidence")
	}
	bindGuideTerminalIntent(t, intent, "completed")
}

func TestGuideDeclineExampleReturnsCorrelatedDisposition(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?m)^(\{"kind":"work\.declined"[^\n]+\})$`).
		FindStringSubmatch(string(projection.Guide()))
	if len(match) != 2 {
		t.Fatal("guide lacks one complete work.declined example")
	}
	intent, err := agency.ParseAgentIntentJSON([]byte(match[1]))
	if err != nil {
		t.Fatalf("guide work.declined is not a valid AgentIntent: %v", err)
	}
	if intent.Consequence() != agency.ConsequenceResolveDeclined ||
		intent.SubjectHandling().IsZero() || len(intent.Successors()) != 1 ||
		intent.Successors()[0].IsSelf() || intent.Successors()[0].Alias().IsZero() ||
		intent.CorrelationHandle().IsZero() {
		t.Fatal("guide work.declined does not close while returning a correlated disposition")
	}
	bindGuideTerminalIntent(t, intent, "declined")
}

func bindGuideTerminalIntent(t *testing.T, intent agency.AgentIntent, suffix string) {
	t.Helper()
	operation, err := agency.NewOperationKey("operation:guide-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	candidates := guideTerminalCandidates(t, intent, operation, suffix)
	view := guideTerminalView(t, intent)
	if _, err = agency.BindIntent(agency.BoundIntentSpec{Intent: intent,
		OperationKey: operation, View: view, Candidates: candidates}); err != nil {
		t.Fatalf("copyable guide terminal Intent cannot bind to imported View: %v", err)
	}
}

func guideTerminalView(t *testing.T, intent agency.AgentIntent) agency.ViewAuthority {
	t.Helper()
	principal, err := agency.NewAgentPrincipalID("agent:guide-responder")
	if err != nil {
		t.Fatal(err)
	}
	attachmentID, err := agency.NewAttachmentID("attachment:guide-responder")
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Unix(1, 0).UTC()
	attachment, err := agency.NewAttachment(attachmentID, principal, false,
		issuedAt, issuedAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	head := guideEventRef(t, "event:guide-current", "guide current")
	handlingID, err := agency.NewHandlingID("handling:guide-current")
	if err != nil {
		t.Fatal(err)
	}
	subject, err := agency.NewSubjectBinding(intent.SubjectHandling(), handlingID, head, 1)
	if err != nil {
		t.Fatal(err)
	}
	replyEvent := guideEventRef(t, "event:guide-request", "guide request")
	replyOffer, err := agency.NewProvenanceOffer(intent.CorrelationHandle(), replyEvent)
	if err != nil {
		t.Fatal(err)
	}
	targets := intent.Successors()
	if len(targets) != 1 {
		t.Fatal("guide terminal Intent does not have one reply target")
	}
	routeID, err := agency.NewRouteID("route:guide-requester")
	if err != nil {
		t.Fatal(err)
	}
	remoteAlias, err := agency.NewOpaqueHandle("peer:guide-requester")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := agency.ResolveRemoteTarget(targets[0], routeID, remoteAlias)
	if err != nil {
		t.Fatal(err)
	}
	view, err := agency.NewViewAuthority(agency.MachineViewSpec{
		Attachment: attachment, Consequences: []agency.Consequence{intent.Consequence()},
		Subjects: []agency.SubjectBinding{subject}, Targets: []agency.ResolvedTarget{resolved},
		ReplyTo: intent.CorrelationHandle(), ReplyTarget: targets[0],
		Provenance: []agency.ProvenanceOffer{replyOffer},
	})
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func guideTerminalCandidates(t *testing.T, intent agency.AgentIntent,
	operation agency.OperationKey, suffix string,
) []agency.CapturedCandidate {
	t.Helper()
	candidates := make([]agency.CapturedCandidate, 0, len(intent.Artifacts()))
	for _, input := range intent.Artifacts() {
		candidate, err := agency.NewCapturedCandidate(operation, input,
			agency.Sum([]byte("guide "+suffix+" evidence")))
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func guideEventRef(t *testing.T, idValue, content string) agency.EventRef {
	t.Helper()
	id, err := agency.NewEventID(idValue)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := agency.NewEventRef(id, agency.Sum([]byte(content)))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestGuideProgressExampleAdvancesExistingAnchorWithoutSuccessor(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?m)^(\{"kind":"work\.progress"[^\n]+\})$`).
		FindStringSubmatch(string(projection.Guide()))
	if len(match) != 2 {
		t.Fatal("guide lacks one complete work.progress example")
	}
	intent, err := agency.ParseAgentIntentJSON([]byte(match[1]))
	if err != nil {
		t.Fatalf("guide work.progress is not a valid AgentIntent: %v", err)
	}
	if intent.Consequence() != agency.ConsequenceAdvanceHandling ||
		intent.SubjectHandling().IsZero() || len(intent.Successors()) != 0 {
		t.Fatal("guide work.progress does not advance only its existing local anchor")
	}
}

func TestGuideTracksCanonicalAgentIntentFieldsAndClosedShapes(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	guide := string(projection.Guide())
	inputs := []string{
		`{"kind":"generic.signal","payload":"bounded","consequence":"handling.create","successors":[{"self":true},{"alias":"target:offered"}],"artifacts":[{"kind":"candidate","handle":"artifact:candidate"},{"kind":"view_handle","handle":"artifact:offered"}],"causation_handles":["event:cause"],"correlation_handle":"event:correlation"}`,
		`{"kind":"generic.signal","payload":"bounded","consequence":"handling.advance","subject_handling":"handling:current"}`,
		`{"kind":"generic.signal","payload":"bounded","consequence":"reference.publish","reference_key":"knowledge.current","artifacts":[{"kind":"candidate","handle":"artifact:candidate"}]}`,
		`{"kind":"generic.signal","payload":"bounded","consequence":"reference.supersede","reference_head":"reference:head","artifacts":[{"kind":"view_handle","handle":"artifact:offered"}]}`,
	}
	fields := make(map[string]struct{})
	for _, input := range inputs {
		intent, err := agency.ParseAgentIntentJSON([]byte(input))
		if err != nil {
			t.Fatalf("real AgentIntent schema rejected drift fixture: %v", err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(intent.CanonicalJSON(), &object); err != nil {
			t.Fatal(err)
		}
		for field := range object {
			fields[field] = struct{}{}
		}
	}
	if len(fields) != 10 {
		t.Fatalf("canonical AgentIntent fixture fields = %v; want complete 10-field surface", fields)
	}
	for field := range fields {
		if !strings.Contains(guide, "`"+field+"`") {
			t.Errorf("guide lacks canonical AgentIntent field %q", field)
		}
	}
	for _, consequence := range []agency.Consequence{
		agency.ConsequenceCreateHandlings,
		agency.ConsequenceAdvanceHandling,
		agency.ConsequenceResolveCompleted,
		agency.ConsequenceResolveDeclined,
		agency.ConsequenceResolveUnresolved,
		agency.ConsequencePublishReference,
		agency.ConsequenceSupersedeReference,
		agency.ConsequenceRetractReference,
	} {
		if !strings.Contains(guide, "`"+consequence.String()+"`") {
			t.Errorf("guide lacks closed consequence %q", consequence.String())
		}
	}
	for _, required := range []string{
		`{"self":true}`, `{"alias":"<View-offered target>"}`,
		`{"kind":"candidate","handle":"<captured handle>"}`,
		`{"kind":"view_handle","handle":"<View-offered handle>"}`,
	} {
		if !strings.Contains(guide, required) {
			t.Errorf("guide lacks canonical nested shape %q", required)
		}
	}
}

func TestGuideFirstSubmitExampleIsAValidCompleteIntent(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	guide := string(projection.Guide())
	match := regexp.MustCompile(`(?s)mnemon-harness agent submit --json <<'JSON'\n([^\n]+)\nJSON`).
		FindStringSubmatch(guide)
	if len(match) != 2 {
		t.Fatal("guide lacks one complete quoted-heredoc submit example")
	}
	intent, err := agency.ParseAgentIntentJSON([]byte(match[1]))
	if err != nil {
		t.Fatalf("guide's first submit example is not a valid AgentIntent: %v", err)
	}
	if intent.Consequence() != agency.ConsequenceCreateHandlings || len(intent.Successors()) == 0 {
		t.Fatal("guide's first submit example is not a complete root Intent")
	}
	for _, forbidden := range []string{`VIEW_OFFERED_CONSEQUENCE`, `"kind":"MEANING"`,
		`"reference_key":"NEW_KEY"`} {
		if strings.Contains(guide, forbidden) {
			t.Fatalf("guide contains an invalid copyable placeholder %q", forbidden)
		}
	}
}

func TestPiHookTimeoutCoversEnsureAndCleanupWithinOneFixedBound(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	source := string(projection.PiExtension())
	match := regexp.MustCompile(`const ATTACH_TIMEOUT_MS = ([0-9]+);`).FindStringSubmatch(source)
	if len(match) != 2 {
		t.Fatalf("Pi extension has no single literal attach timeout: %q", source)
	}
	timeout, err := strconv.Atoi(match[1])
	if err != nil {
		t.Fatal(err)
	}
	const (
		ensureMillis  = 3000
		cleanupMillis = 1000
		fixedBound    = 5000
	)
	if timeout != fixedBound || timeout <= ensureMillis+cleanupMillis {
		t.Fatalf("Pi attach timeout = %dms; want fixed %dms above %dms ensure+cleanup",
			timeout, fixedBound, ensureMillis+cleanupMillis)
	}
	if strings.Count(source, "ATTACH_TIMEOUT_MS") != 2 {
		t.Fatal("Pi extension does not use exactly one declared attach timeout")
	}
}

func TestPiHookRetriesOnePrivateBoundaryAndEmitsNoCueOnFailure(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	source := string(projection.PiExtension())
	for _, required := range []string{
		"const ATTACH_ATTEMPTS = 2;",
		"for (let attempt = 0; attempt < ATTACH_ATTEMPTS; attempt += 1)",
		"if (runBoundary([\"hook\", \"attach\", \"--json\"], boundary)) return true;",
		"if (!attachBoundary(boundary)) return undefined;",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Pi bounded attachment retry lacks %q", required)
		}
	}
	if strings.Count(source, "const boundary = randomBytes(32)") != 1 {
		t.Fatal("Pi attachment retry can mint more than one boundary nonce")
	}
}

func TestPiHookBoundsGovernedToolAttemptsAndRestoresAtSettlement(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	source := string(projection.PiExtension())
	for _, required := range []string{
		"const MAX_TOOL_CALL_ATTEMPTS_PER_RUN = 16;",
		`pi.on("tool_call"`, `pi.on("turn_start"`, `pi.on("agent_settled"`,
		"toolCallAttempts < MAX_TOOL_CALL_ATTEMPTS_PER_RUN",
		"savedActiveTools = [...pi.getActiveTools()];",
		"ownsToolOverride = true;",
		"pi.setActiveTools([]);",
		"return { block: true, reason: ATTENTION_EXHAUSTED_REASON };",
		"if (postBudgetTurns > 1) abortOnce(ctx);",
		"pi.setActiveTools(savedActiveTools);",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Pi bounded attention lacks %q", required)
		}
	}
	if strings.Count(source, `pi.on("agent_settled"`) != 1 ||
		strings.Contains(source, `pi.on("agent_end"`) {
		t.Fatal("Pi attention may reset only after the complete run settles")
	}
	if strings.Count(source, "resetAttention()") != 4 ||
		!strings.Contains(source, "if (!resetAttention()) return undefined;") {
		t.Fatal("Pi attention is not reset at run start, settlement, and shutdown")
	}
	before := regexp.MustCompile(`(?s)pi\.on\("before_agent_start".*?if \(!resetAttention\(\)\) return undefined;.*?` +
		`if \(!attachBoundary\(boundary\)\) return undefined;.*?governedRun = true;`)
	if !before.MatchString(source) {
		t.Fatal("Pi attachment failure can inherit or activate a governed tool budget")
	}
	restore := regexp.MustCompile(`(?s)try \{\s*pi\.setActiveTools\(savedActiveTools\);\s*` +
		`ownsToolOverride = false;\s*savedActiveTools = undefined;\s*return true;\s*` +
		`} catch \{.*?return false;\s*}`)
	if !restore.MatchString(source) {
		t.Fatal("Pi failed restore can discard the exact tool snapshot or open a new run")
	}
	reason := regexp.MustCompile(`(?s)const ATTENTION_EXHAUSTED_REASON =\s*"([^"]+)";`).
		FindStringSubmatch(source)
	if len(reason) != 2 || len(reason[1]) > 192 {
		t.Fatalf("Pi attention diagnostic is absent or unbounded: %q", reason)
	}
	for _, forbidden := range []string{"accepted", "completed", "receipt", "event", "handling"} {
		if strings.Contains(strings.ToLower(reason[1]), forbidden) {
			t.Fatalf("Pi attention diagnostic claims protocol meaning %q", forbidden)
		}
	}
}

func TestInstallPiIsProjectLocalExactAndPreservesAdjacentFiles(t *testing.T) {
	workspace := testWorkspace(t)
	legacy := filepath.Join(workspace, ".pi", "skills", "mnemon", "SKILL.md")
	custom := filepath.Join(workspace, ".pi", "extensions", "custom.ts")
	writeTestFile(t, legacy, []byte("legacy memory\n"), 0o644)
	writeTestFile(t, custom, []byte("custom extension\n"), 0o644)

	receipt, err := InstallPi(workspace)
	if err != nil {
		t.Fatal(err)
	}
	assertInstallPaths(t, workspace, receipt)
	if receipt.Replayed || receipt.Revision == "" {
		t.Fatalf("first InstallPi receipt = %#v", receipt)
	}
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	assertFile(t, receipt.GuidePath, projection.Guide(), projectedMode)
	assertFile(t, receipt.ExtensionPath, projection.PiExtension(), projectedMode)
	assertFile(t, receipt.JournalPath, mustPlan(t, workspace).journalBytes, journalMode)
	assertFile(t, legacy, []byte("legacy memory\n"), 0o644)
	assertFile(t, custom, []byte("custom extension\n"), 0o644)
	assertMode(t, filepath.Dir(receipt.JournalPath), 0o700)
	if err := VerifyPi(workspace); err != nil {
		t.Fatalf("VerifyPi() = %v", err)
	}
}

func TestInstallPiExactReplayDoesNotRewriteOwnedFiles(t *testing.T) {
	workspace := testWorkspace(t)
	first, err := InstallPi(workspace)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{first.GuidePath, first.ExtensionPath, first.JournalPath}
	identities := make([]os.FileInfo, len(paths))
	for index, path := range paths {
		identities[index], err = os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	second, err := InstallPi(workspace)
	if err != nil || !second.Replayed || second.Revision != first.Revision {
		t.Fatalf("replay InstallPi() = (%#v, %v)", second, err)
	}
	for index, path := range paths {
		current, statErr := os.Lstat(path)
		if statErr != nil || !os.SameFile(identities[index], current) {
			t.Fatalf("replay rewrote %s: %v", path, statErr)
		}
	}
}

func TestInstallPiRecoversEveryDurableInterruption(t *testing.T) {
	stages := []string{
		"after_journal",
		"after_file:.pi/extensions/mnemond.ts",
		"after_file:.pi/skills/mnemond/SKILL.md",
	}
	interrupted := errors.New("test interruption")
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			workspace := testWorkspace(t)
			boundary := func(current string) error {
				if current == stage {
					return interrupted
				}
				return nil
			}
			if _, err := installPi(workspace, boundary); !errors.Is(err, interrupted) {
				t.Fatalf("interrupted install = %v", err)
			}
			receipt, err := InstallPi(workspace)
			if err != nil {
				t.Fatalf("recovered InstallPi() = (%#v, %v)", receipt, err)
			}
			if err := VerifyPi(workspace); err != nil {
				t.Fatalf("recovered VerifyPi() = %v", err)
			}
		})
	}
}

func TestInstallPiRecoversBoundedCrashStages(t *testing.T) {
	t.Run("incomplete journal stage", func(t *testing.T) {
		workspace := testWorkspace(t)
		plan := mustPlan(t, workspace)
		if err := ensureJournalDirectory(plan); err != nil {
			t.Fatal(err)
		}
		stage := stagePath(filepath.Dir(plan.journalPath), plan.journalPath)
		if err := os.WriteFile(stage, plan.journalBytes[:8], 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallPi(workspace); err != nil {
			t.Fatalf("InstallPi(incomplete stage) = %v", err)
		}
		assertAbsent(t, stage)
		if err := VerifyPi(workspace); err != nil {
			t.Fatalf("VerifyPi(incomplete stage recovery) = %v", err)
		}
	})

	t.Run("complete unlinked projected stage", func(t *testing.T) {
		workspace := testWorkspace(t)
		plan := mustPlan(t, workspace)
		if err := beginInstall(plan); err != nil {
			t.Fatal(err)
		}
		if err := ensureProjectionDirectories(plan); err != nil {
			t.Fatal(err)
		}
		file := plan.files[0]
		stage, err := prepareStage(filepath.Dir(plan.journalPath), file.path,
			file.content, projectedMode)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := InstallPi(workspace); err != nil {
			t.Fatalf("InstallPi(unlinked stage) = %v", err)
		}
		assertAbsent(t, stage)
		if err := VerifyPi(workspace); err != nil {
			t.Fatalf("VerifyPi(unlinked stage recovery) = %v", err)
		}
	})

	t.Run("linked projected stage", func(t *testing.T) {
		workspace := testWorkspace(t)
		plan := mustPlan(t, workspace)
		if err := beginInstall(plan); err != nil {
			t.Fatal(err)
		}
		if err := ensureProjectionDirectories(plan); err != nil {
			t.Fatal(err)
		}
		file := plan.files[0]
		stage, err := prepareStage(filepath.Dir(plan.journalPath), file.path,
			file.content, projectedMode)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Link(stage, file.path); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallPi(workspace); err != nil {
			t.Fatalf("InstallPi(linked stage) = %v", err)
		}
		assertAbsent(t, stage)
		if err := VerifyPi(workspace); err != nil {
			t.Fatalf("VerifyPi(linked stage recovery) = %v", err)
		}
	})
}

func TestInstallPiRejectsUnownedTargetAndOwnedDrift(t *testing.T) {
	t.Run("unowned target", func(t *testing.T) {
		workspace := testWorkspace(t)
		target := filepath.Join(workspace, ".pi", "extensions", "mnemond.ts")
		writeTestFile(t, target, []byte("user file\n"), projectedMode)
		if _, err := InstallPi(workspace); !errors.Is(err, ErrDrift) {
			t.Fatalf("InstallPi(unowned) = %v", err)
		}
		assertFile(t, target, []byte("user file\n"), projectedMode)
	})
	t.Run("content drift", func(t *testing.T) {
		workspace := testWorkspace(t)
		receipt, err := InstallPi(workspace)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(receipt.GuidePath, []byte("drift\n"), projectedMode); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallPi(workspace); !errors.Is(err, ErrDrift) {
			t.Fatalf("InstallPi(content drift) = %v", err)
		}
		assertFile(t, receipt.GuidePath, []byte("drift\n"), projectedMode)
	})
	t.Run("mode drift", func(t *testing.T) {
		workspace := testWorkspace(t)
		receipt, err := InstallPi(workspace)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(receipt.ExtensionPath, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := VerifyPi(workspace); !errors.Is(err, ErrDrift) {
			t.Fatalf("VerifyPi(mode drift) = %v", err)
		}
	})
	t.Run("missing owned file recovers", func(t *testing.T) {
		workspace := testWorkspace(t)
		receipt, err := InstallPi(workspace)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(receipt.GuidePath); err != nil {
			t.Fatal(err)
		}
		recovered, err := InstallPi(workspace)
		if err != nil || recovered.Replayed {
			t.Fatalf("InstallPi(missing file) = (%#v, %v)", recovered, err)
		}
		if err := VerifyPi(workspace); err != nil {
			t.Fatalf("VerifyPi(recovered missing file) = %v", err)
		}
	})
}

func TestInstallPiRejectsSymlinkedProjectionAncestors(t *testing.T) {
	workspace := testWorkspace(t)
	outside := testWorkspace(t)
	if err := os.Symlink(outside, filepath.Join(workspace, ".pi")); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallPi(workspace); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("InstallPi(symlinked .pi) = %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("symlink target changed: entries=%v err=%v", entries, err)
	}
}

func TestOwnerDriftFailsClosed(t *testing.T) {
	path := filepath.Join(testWorkspace(t), "owned")
	if err := os.WriteFile(path, []byte("owned"), projectedMode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	foreign := foreignOwnerInfo{FileInfo: info, uid: uint32(os.Geteuid() + 1)}
	if err := validateSafeFile(foreign, projectedMode, 64); err == nil {
		t.Fatal("foreign-owner file passed validation")
	}
	directory, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	foreignDirectory := foreignOwnerInfo{FileInfo: directory, uid: uint32(os.Geteuid() + 1)}
	if err := validateSafeDirectory(foreignDirectory, false); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("foreign-owner directory validation = %v", err)
	}
}

type foreignOwnerInfo struct {
	os.FileInfo
	uid uint32
}

func (info foreignOwnerInfo) Sys() any {
	stat := *info.FileInfo.Sys().(*syscall.Stat_t)
	stat.Uid = info.uid
	return &stat
}

func assertInstallPaths(t *testing.T, workspace string, receipt InstallReceipt) {
	t.Helper()
	if receipt.GuidePath != filepath.Join(workspace, ".pi", "skills", "mnemond", "SKILL.md") ||
		receipt.ExtensionPath != filepath.Join(workspace, ".pi", "extensions", "mnemond.ts") ||
		receipt.JournalPath != filepath.Join(workspace, ".mnemon", "harness", "attach", "pi",
			"ownership.json") {
		t.Fatalf("InstallPi paths = %#v", receipt)
	}
}

func mustPlan(t *testing.T, workspace string) installPlan {
	t.Helper()
	plan, err := prepareInstall(workspace)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testWorkspace(t *testing.T) string {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return workspace
}

func writeTestFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
}

func assertFile(t *testing.T, path string, expected []byte, mode os.FileMode) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(content, expected) {
		t.Fatalf("file %s = %q, err=%v; want %q", path, content, err, expected)
	}
	assertMode(t, path, mode)
}

func assertMode(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != mode || !ownedByCurrentUser(info) {
		t.Fatalf("path %s = (%v, %v); want owner mode %04o", path, info, err, mode)
	}
}

func assertAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path %s remains: %v", path, err)
	}
}
