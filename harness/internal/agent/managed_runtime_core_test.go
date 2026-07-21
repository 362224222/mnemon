package agent

import "testing"

func TestManagedRuntimeCoreClosesEnvironmentStorage(t *testing.T) {
	environment := []string{"PATH=/usr/bin:/bin"}
	core, stage, err := newManagedRuntimeCore(CodexWakeAdapterOptions{
		Executable: "/usr/bin/runtime", Workspace: t.TempDir(), Environment: environment,
		Starter: &fakeCodexStarter{}, Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(),
		Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection,
	})
	if err != nil || stage != "" || core == nil {
		t.Fatalf("newManagedRuntimeCore() = (%#v, %q, %v)", core, stage, err)
	}
	environment[0] = "PATH=/mutated"
	if len(core.environment) != 1 || core.environment[0] != "PATH=/usr/bin:/bin" {
		t.Fatalf("closed environment = %q", core.environment)
	}
}
