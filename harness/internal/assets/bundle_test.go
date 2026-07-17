package assets

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestManagedBundleBindsExactTeamworkOnlyAssets(t *testing.T) {
	bundle, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	manifest := bundle.Manifest()
	if manifest.SchemaVersion != 1 || len(manifest.Files) != 13 || !validDigestText(manifest.AssetRevision) {
		t.Fatalf("manifest = %#v", manifest)
	}
	wantActions := []string{"accept", "cancel", "close", "decline", "deliver", "offer", "rework"}
	if got := sortedActionNames(bundle.actions); !reflect.DeepEqual(got, wantActions) {
		t.Fatalf("action catalog = %v, want %v", got, wantActions)
	}
	for _, action := range wantActions {
		schema, ok := bundle.Action(action)
		if !ok || schema.Receipt.Action != "teamwork."+action {
			t.Fatalf("Action(%q) = (%#v, %t)", action, schema, ok)
		}
		schema.AllowedContext[0] = "tampered"
		fresh, _ := bundle.Action(action)
		if fresh.AllowedContext[0] == "tampered" {
			t.Fatalf("Action(%q) returned mutable state", action)
		}
	}
	for _, forbidden := range []string{"memory", "evolution", "mcp", "capability"} {
		for _, record := range manifest.Files {
			if strings.Contains(strings.ToLower(record.Path), forbidden) {
				t.Fatalf("manifest contains forbidden ability path %q", record.Path)
			}
		}
	}
}

func TestManagedBundleSelectsHostVariantAndServesExactBytes(t *testing.T) {
	bundle, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []Host{HostCodex, HostClaudeCode} {
		files, err := bundle.FilesFor(host)
		if err != nil || len(files) != 11 {
			t.Fatalf("FilesFor(%s) = (%v, %v)", host, files, err)
		}
		registration, ok := bundle.Registration(host)
		wantSkillTarget := map[Host]string{
			HostCodex: ".agents/skills/mnemon-harness", HostClaudeCode: ".claude/skills/mnemon-harness",
		}[host]
		if !ok || registration.Host != host || registration.SkillTarget != wantSkillTarget ||
			registration.Value.Hook.Command != "{{HOOK_PATH}}" {
			t.Fatalf("Registration(%s) = (%#v, %t)", host, registration, ok)
		}
		hookPath := "hosts/" + string(host) + "/hook.sh"
		hook, err := bundle.Read(hookPath)
		if err != nil || len(hook) > 256 || bytes.Contains(hook, []byte("Event")) ||
			bytes.Contains(hook, []byte("pending")) {
			t.Fatalf("Read(%s) = (%q, %v)", hookPath, hook, err)
		}
	}
	if _, err := bundle.FilesFor("unknown"); err == nil {
		t.Fatal("unknown Host selected an asset variant")
	}
	if _, err := bundle.Read("manifest.json"); err == nil {
		t.Fatal("manifest was treated as a listed projection file")
	}
	manifestBytes := bundle.ManifestBytes()
	sourceManifest, err := os.ReadFile(filepath.Join("managed", "manifest.json"))
	if err != nil || !bytes.Equal(manifestBytes, sourceManifest) {
		t.Fatalf("ManifestBytes() did not preserve source bytes: %v", err)
	}
	manifestBytes[0] ^= 0xff
	if bytes.Equal(manifestBytes, bundle.ManifestBytes()) {
		t.Fatal("ManifestBytes() returned mutable bundle state")
	}
}

func TestManagedSourceModesAndSkillGuideResponsibilities(t *testing.T) {
	bundle, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range bundle.Manifest().Files {
		info, err := os.Stat(filepath.Join("managed", filepath.FromSlash(record.Path)))
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0o644)
		if strings.HasSuffix(record.Path, "/hook.sh") {
			want = 0o755
		}
		if info.Mode().Perm() != want {
			t.Fatalf("source mode %s = %04o, want %04o", record.Path, info.Mode().Perm(), want)
		}
	}
	skill, _ := bundle.Read("SKILL.md")
	guide, _ := bundle.Read("guides/teamwork/GUIDE.md")
	for _, required := range [][]byte{[]byte("agent current --json"), []byte("teamwork offer"),
		[]byte("agent resolve"), []byte("context_file"), []byte("guides/teamwork/GUIDE.md")} {
		if !bytes.Contains(skill, required) {
			t.Fatalf("SKILL.md lacks %q", required)
		}
	}
	for _, required := range [][]byte{[]byte("accept"), []byte("decline"), []byte("deliver"),
		[]byte("rework"), []byte("close"), []byte("retry"), []byte("Artifact")} {
		if !bytes.Contains(guide, required) {
			t.Fatalf("GUIDE.md lacks %q", required)
		}
	}
	for _, forbidden := range [][]byte{[]byte("--event-id"), []byte("--operation-id"),
		[]byte("--peer-id"), []byte("--principal"), []byte("--mode"), []byte("--payload")} {
		if bytes.Contains(skill, forbidden) || bytes.Contains(guide, forbidden) {
			t.Fatalf("managed documents teach model-owned authority flag %q", forbidden)
		}
	}
}
