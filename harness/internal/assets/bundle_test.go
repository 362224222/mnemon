package assets

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestManagedBundleBindsExactTeamworkOnlyAssets(t *testing.T) {
	bundle := mustLoadManagedBundle(t)
	manifest := bundle.Manifest()
	assertManagedManifestBinding(t, bundle, manifest)
	wantPaths := manifestTeamworkActionPaths(t, manifest)
	assertManagedTeamworkActionPathProjection(t, bundle, wantPaths)
	assertManagedTeamworkActionSources(t, bundle, wantPaths)
	assertManagedTeamworkActionPathRejection(t, bundle)
	assertManagedAbilityScope(t, manifest)
	assertZeroManagedBundle(t, wantPaths[0])
}

func mustLoadManagedBundle(t *testing.T) Bundle {
	t.Helper()
	bundle, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func assertManagedManifestBinding(t *testing.T, bundle Bundle, manifest Manifest) {
	t.Helper()
	if manifest.SchemaVersion != 1 || len(manifest.Files) != 13 || !validDigestText(manifest.AssetRevision) {
		t.Fatalf("manifest = %#v", manifest)
	}
	if bundle.Revision() != manifest.AssetRevision {
		t.Fatalf("Revision() = %q, want %q", bundle.Revision(), manifest.AssetRevision)
	}
}

func manifestTeamworkActionPaths(t *testing.T, manifest Manifest) []string {
	t.Helper()
	wantPaths := make([]string, 0)
	for _, record := range manifest.Files {
		if strings.HasPrefix(record.Path, teamworkActionPathRoot) {
			wantPaths = append(wantPaths, record.Path)
		}
	}
	if len(wantPaths) == 0 {
		t.Fatal("manifest has no Teamwork action sources")
	}
	return wantPaths
}

func assertManagedTeamworkActionPathProjection(t *testing.T, bundle Bundle, wantPaths []string) {
	t.Helper()
	paths := bundle.TeamworkActionPaths()
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("TeamworkActionPaths() = %v, want %v", paths, wantPaths)
	}
	paths[0] = "tampered"
	if !reflect.DeepEqual(bundle.TeamworkActionPaths(), wantPaths) {
		t.Fatal("TeamworkActionPaths() returned mutable bundle state")
	}
}

func assertManagedTeamworkActionSources(t *testing.T, bundle Bundle, paths []string) {
	t.Helper()
	for _, path := range paths {
		assertManagedTeamworkActionSource(t, bundle, path)
	}
}

func assertManagedTeamworkActionSource(t *testing.T, bundle Bundle, path string) {
	t.Helper()
	raw, readErr := bundle.ReadTeamworkAction(path)
	if readErr != nil {
		t.Fatalf("ReadTeamworkAction(%q) error = %v", path, readErr)
	}
	source, sourceErr := os.ReadFile(filepath.Join("managed", filepath.FromSlash(path)))
	record, ok := bundle.record(path)
	if sourceErr != nil || !ok || !bytes.Equal(raw, source) || digestBytes(raw) != record.Digest {
		t.Fatalf("ReadTeamworkAction(%q) did not preserve manifest-bound bytes: source error %v", path, sourceErr)
	}
	raw[0] ^= 0xff
	fresh, freshErr := bundle.ReadTeamworkAction(path)
	if freshErr != nil || bytes.Equal(raw, fresh) {
		t.Fatalf("ReadTeamworkAction(%q) returned mutable bytes: %v", path, freshErr)
	}
}

func assertManagedTeamworkActionPathRejection(t *testing.T, bundle Bundle) {
	t.Helper()
	for _, path := range []string{"SKILL.md", "actions/teamwork", "actions/teamwork/unknown.json", "../offer.json", ""} {
		if raw, readErr := bundle.ReadTeamworkAction(path); readErr == nil || raw != nil {
			t.Fatalf("ReadTeamworkAction(%q) = (%q, %v), want rejection", path, raw, readErr)
		}
	}
}

func assertManagedAbilityScope(t *testing.T, manifest Manifest) {
	t.Helper()
	for _, forbidden := range []string{"memory", "evolution", "mcp", "capability"} {
		for _, record := range manifest.Files {
			if strings.Contains(strings.ToLower(record.Path), forbidden) {
				t.Fatalf("manifest contains forbidden ability path %q", record.Path)
			}
		}
	}
}

func assertZeroManagedBundle(t *testing.T, actionPath string) {
	t.Helper()
	var zero Bundle
	if zero.Revision() != "" || zero.TeamworkActionPaths() != nil {
		t.Fatalf("zero Bundle exposes action state: %#v", zero)
	}
	if raw, readErr := zero.ReadTeamworkAction(actionPath); readErr == nil || raw != nil {
		t.Fatalf("zero Bundle ReadTeamworkAction() = (%q, %v)", raw, readErr)
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

func TestManagedHooksPreserveCueAndMapFailureToBlockingExit(t *testing.T) {
	const cue = "[mnemon:wake] Managed work is pending. Use the Mnemon Harness skill to process one Event.\n"
	for _, host := range []Host{HostCodex, HostClaudeCode} {
		host := host
		t.Run(string(host), func(t *testing.T) {
			hookPath, err := filepath.Abs(filepath.Join("managed", "hosts", string(host), "hook.sh"))
			if err != nil {
				t.Fatal(err)
			}
			fakePath := filepath.Join(t.TempDir(), "mnemon-harness")
			if err := os.WriteFile(fakePath, []byte("#!/bin/sh\nprintf '%s\\n' '"+
				strings.TrimSuffix(cue, "\n")+"'\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, exitCode := runManagedHook(t, hookPath, filepath.Dir(fakePath))
			if stdout != cue || stderr != "" || exitCode != 0 {
				t.Fatalf("successful Hook = (stdout %q, stderr %q, exit %d)", stdout, stderr, exitCode)
			}

			if err := os.WriteFile(fakePath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, exitCode = runManagedHook(t, hookPath, filepath.Dir(fakePath))
			if stdout != "" || stderr != "" || exitCode != 0 {
				t.Fatalf("empty successful Hook = (stdout %q, stderr %q, exit %d)", stdout, stderr, exitCode)
			}

			if err := os.WriteFile(fakePath, []byte("#!/bin/sh\nprintf '%s\\n' 'failure stdout must be discarded'\nexit 17\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, exitCode = runManagedHook(t, hookPath, filepath.Dir(fakePath))
			if stdout != "" || stderr == "" || exitCode != 2 {
				t.Fatalf("failed Hook = (stdout %q, stderr %q, exit %d)", stdout, stderr, exitCode)
			}
			if stderr != "mnemon-harness hook check failed; managed Agent execution is blocked\n" {
				t.Fatalf("failed Hook stderr = %q", stderr)
			}

			if err := os.WriteFile(fakePath, []byte("#!/bin/sh\nprintf '%s\\n' 'signal stdout must be discarded'\nkill -TERM $$\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, exitCode = runManagedHook(t, hookPath, filepath.Dir(fakePath))
			if stdout != "" || stderr == "" || exitCode != 2 {
				t.Fatalf("signaled Hook = (stdout %q, stderr %q, exit %d)", stdout, stderr, exitCode)
			}
		})
	}
}

func TestValidateHookRequiresExactFailClosedWrapper(t *testing.T) {
	if err := validateHook([]byte(hookBody)); err != nil {
		t.Fatalf("validateHook(canonical) error = %v", err)
	}
	for name, content := range map[string]string{
		"direct exec":    "#!/bin/sh\nset -eu\nexec mnemon-harness hook check\n",
		"wrong exit":     strings.Replace(hookBody, "exit 2\n", "exit 1\n", 1),
		"missing stderr": strings.Replace(hookBody, " >&2\n", "\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateHook([]byte(content)); err == nil {
				t.Fatal("validateHook() accepted a noncanonical wrapper")
			}
		})
	}
}

func runManagedHook(t *testing.T, hookPath, path string) (string, string, int) {
	t.Helper()
	command := exec.Command(hookPath)
	command.Env = []string{"PATH=" + path}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err == nil {
		return stdout.String(), stderr.String(), 0
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("run Hook: %v", err)
	}
	return stdout.String(), stderr.String(), exitError.ExitCode()
}
