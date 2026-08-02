package assets

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

func TestR7ProjectionIsBoundedAndPatternNeutral(t *testing.T) {
	projection, err := LoadR7Projection()
	if err != nil {
		t.Fatal(err)
	}
	guide := projection.Guide()
	if len(guide) == 0 || len(guide) > MaxR7GuideBytes ||
		len(projection.HookCue()) > MaxR7HookCueBytes {
		t.Fatalf("projection sizes = guide %d, cue %d", len(guide), len(projection.HookCue()))
	}
	for _, required := range [][]byte{
		[]byte("name: mnemond"),
		[]byte("View -> Intent -> Receipt"),
		[]byte("agent current --json"),
		[]byte("Artifact"),
		[]byte("Reference"),
		[]byte("replayed"),
	} {
		if !bytes.Contains(guide, required) {
			t.Fatalf("R7 guide lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		"review.request", "contract-net", "blackboard", "--event-id",
		"--operation-id", "--principal", "--fence", "--peer-id",
	} {
		if strings.Contains(strings.ToLower(string(guide)), forbidden) ||
			strings.Contains(strings.ToLower(projection.HookCue()), forbidden) {
			t.Fatalf("R7 projection contains pattern or authority surface %q", forbidden)
		}
	}
	guide[0] ^= 0xff
	if bytes.Equal(guide, projection.Guide()) {
		t.Fatal("R7 projection returned mutable guide bytes")
	}
}

func TestR7PiProjectionKeepsHookContextFixedAndPrivate(t *testing.T) {
	projection, err := LoadR7Projection()
	if err != nil {
		t.Fatal(err)
	}
	source := string(projection.PiExtension())
	if len(source) == 0 || len(source) > MaxR7PiExtensionBytes {
		t.Fatalf("Pi extension size = %d", len(source))
	}

	for _, required := range []string{
		`import { execFileSync } from "node:child_process"`,
		`import type { ExtensionAPI } from "@earendil-works/pi-coding-agent"`,
		`pi.on("before_agent_start"`,
		`execFileSync("mnemon-harness", ["hook", "attach", "--json"]`,
		`stdio: ["ignore", "ignore", "ignore"]`,
		`maxBuffer: MAX_OUTPUT_BYTES`,
		`timeout: ATTACH_TIMEOUT_MS`,
		`content: HOOK_CUE`,
		`display: false`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("R7 Pi extension lacks %q", required)
		}
	}
	if !strings.Contains(source, `const HOOK_CUE = "`+projection.HookCue()+`";`) {
		t.Fatal("R7 Pi extension cue differs from the canonical Hook cue")
	}

	for _, forbidden := range []string{
		`exec(`,
		`execSync(`,
		`shell:`,
		`agent_end`,
		`agent_idle`,
		`session_before_compact`,
		`DEEPSEEK_API_KEY`,
		`process.env.HOME ??`,
		`content: raw`,
		`content: output`,
		`content: result`,
		`JSON.parse(`,
		`stdout`,
		`stderr`,
		`event_id`,
		`eventId`,
		`payload`,
		`transcript`,
		`credential`,
		`console.`,
		`MNEMON_HARNESS_SOCKET`,
		`MNEMON_HARNESS_EXECUTABLE`,
		`--socket`,
		`process.env`,
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("R7 Pi extension contains forbidden surface %q", forbidden)
		}
	}
	if got := strings.Count(source, "content:"); got != 1 {
		t.Fatalf("R7 Pi extension has %d model-content sources; want the fixed cue only", got)
	}

	copyOfSource := projection.PiExtension()
	copyOfSource[0] ^= 0xff
	if bytes.Equal(copyOfSource, projection.PiExtension()) {
		t.Fatal("R7 projection returned mutable Pi extension bytes")
	}
}

func TestR7ProjectionContainsNoProviderCredentialSurface(t *testing.T) {
	projection, err := LoadR7Projection()
	if err != nil {
		t.Fatal(err)
	}
	all := bytes.Join([][]byte{
		projection.Guide(), []byte(projection.HookCue()), projection.PiExtension(),
	}, []byte("\n"))
	for _, expression := range []string{
		`(?i)deepseek`,
		`(?i)api[_-]?key`,
		`(?i)authorization\s*:`,
		`(?i)bearer\s+[a-z0-9._-]+`,
		`(?i)sk-[a-z0-9]{16,}`,
	} {
		if regexp.MustCompile(expression).Find(all) != nil {
			t.Fatalf("R7 projection matches forbidden credential surface %q", expression)
		}
	}
}
