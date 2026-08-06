package attach

import (
	"strings"
	"testing"
)

func TestPiEffectSettlementUsesOneNativeBoundedToolWithoutShellInference(t *testing.T) {
	projection, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	source := string(projection.PiExtension())
	for _, required := range []string{
		`const EFFECT_SETTLEMENT_TOOL = "mnemond_submit";`,
		"const MAX_EFFECT_SETTLEMENT_ATTEMPTS = 2;",
		`pi.registerTool({`, `name: EFFECT_SETTLEMENT_TOOL`,
		`execFile("mnemon-harness", ["agent", "submit", "--json"]`,
		`shell: false`, `child.stdin.end(encoded);`,
		`_event.toolName === EFFECT_SETTLEMENT_TOOL`,
		`effectSettlementAttempts >= MAX_EFFECT_SETTLEMENT_ATTEMPTS`,
		`details?.schema !== "mnemon.pi.effect"`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("Pi Effect settlement lacks %q", required)
		}
	}
	for _, forbidden := range []string{
		`exec("`, `execSync(`, `spawn(`, `.includes("mnemon-harness`,
		`.includes("submit`, `.match(`,
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("Pi Effect settlement infers authority from command text %q", forbidden)
		}
	}
}
