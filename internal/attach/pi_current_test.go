package attach

import (
	"bytes"
	"strings"
	"testing"
)

func TestPiCurrentProjectionIsBoundedAndImmutable(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	extension := projection.PiCurrentExtension()
	if len(extension) == 0 || len(extension) > MaxExtensionBytes {
		t.Fatalf("Pi Current extension size = %d", len(extension))
	}
	extension[0] ^= 0xff
	if bytes.Equal(extension, projection.PiCurrentExtension()) {
		t.Fatal("Load returned a mutable Pi Current extension")
	}
}

func TestPiCurrentIsTheOnlyViewSurfaceInThePiGuide(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	guide := string(projection.Guide())
	for _, required := range []string{
		"Read this Pi turn's View once with `mnemond_current {}`; never use bash or retry.",
	} {
		if !strings.Contains(guide, required) {
			t.Fatalf("Pi guide lacks its exclusive Current surface %q", required)
		}
	}
	for _, fallback := range []string{
		"mnemon agency agent current", "mnemon agency agent submit",
	} {
		if strings.Contains(guide, fallback) {
			t.Fatalf("Pi guide exposes CLI fallback %q", fallback)
		}
	}
}

func TestPiCurrentUsesOneNativeBoundedToolWithoutShellInference(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	source := string(projection.PiCurrentExtension())
	for _, required := range []string{
		`const CURRENT_TOOL = "mnemond_current";`,
		`This is Pi's only Current surface; do not retry through bash.`,
		`const CURRENT_TIMEOUT_MS = 5000;`,
		`const CURRENT_SHUTDOWN_GRACE_MS = 100;`,
		`const CURRENT_ATTEMPTS = 2;`,
		`const MAX_CURRENT_OUTPUT_BYTES = (16 << 10) + 1;`,
		`properties: {}`, `additionalProperties: false`,
		`execFile("mnemon", ["agency", "agent", "current", "--json"]`,
		`return await readCurrent(signal);`,
		`error instanceof CurrentInterruptedError`,
		`shell: false`, `setTimeout(interrupt, CURRENT_TIMEOUT_MS)`,
		`setTimeout(() => signalOwnedChild(child, "SIGKILL")`,
		`signalOwnedChild(child, "SIGTERM")`, `clearTimeout(timeout)`, `clearTimeout(killTimer)`,
		`signal.removeEventListener("abort", interrupt)`,
		`maxBuffer: MAX_CURRENT_OUTPUT_BYTES`, `child.stdin.end();`,
		`stdout.endsWith("\n")`, `stdout.indexOf("\n") !== stdout.length - 1`,
		`value = JSON.parse(raw)`, `view.schema !== "mnemon.agent.view"`,
		`view.version !== 7`, `details: { schema: "mnemon.pi.current", version: 1, status }`,
		`event.toolName !== CURRENT_TOOL`, `details.status !== "projected"`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Pi native Current lacks %q", required)
		}
	}
	if strings.Count(source, "JSON.parse(raw)") != 1 {
		t.Fatal("Pi native Current does not parse exactly one framed JSON document")
	}
	for _, forbidden := range []string{
		`exec("`, `execSync(`, `spawn(`, `shell: true`, `process.env`,
		`.includes("mnemon`, `.match(`,
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("Pi native Current infers authority from an unsafe surface %q", forbidden)
		}
	}
}
