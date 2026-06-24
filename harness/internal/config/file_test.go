package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

func TestLoadConfigRoundTrips(t *testing.T) {
	f, err := Load(writeConfig(t, `{
  "local": {"store_path": ".mnemon/harness/local/governed.db", "endpoint": "http://127.0.0.1:8787"},
  "channel": {"binding_file": ".mnemon/harness/channel/bindings.json"},
  "capabilities": {
    "memory": {"enabled": true, "resource_ref": "memory/project", "rule_ref": "native:memory"}
  },
  "background": {"sync": "disabled", "event_view_refresh": "manual"}
}`))
	if err != nil {
		t.Fatalf("load valid config: %v", err)
	}
	if f.Local.Endpoint != "http://127.0.0.1:8787" {
		t.Fatalf("endpoint not parsed: %q", f.Local.Endpoint)
	}
	mem, ok := f.Capabilities["memory"]
	if !ok || !mem.Enabled || mem.RuleRef != "native:memory" {
		t.Fatalf("memory capability not parsed: %+v", mem)
	}
}

func TestLoadConfigFailsClosedOnUnknownKey(t *testing.T) {
	_, err := Load(writeConfig(t, `{"local": {}, "channel": {}, "capabilities": {}, "background": {}, "mystery": true}`))
	if err == nil {
		t.Fatal("an unknown top-level key must be rejected (fail-closed)")
	}
}

func TestLoadConfigRejectsBadRuleRefAndRetiredMirrorMode(t *testing.T) {
	if _, err := Load(writeConfig(t, `{"capabilities": {"x": {"enabled": true, "rule_ref": "memory"}}}`)); err == nil {
		t.Fatal("a non-native rule_ref must be rejected")
	}
	if _, err := Load(writeConfig(t, `{"capabilities": {"x": {"enabled": true, "rule_ref": "native:memory", "mirror_mode": "weird"}}}`)); err == nil {
		t.Fatal("retired mirror_mode must be rejected as an unknown capability field")
	}
}
