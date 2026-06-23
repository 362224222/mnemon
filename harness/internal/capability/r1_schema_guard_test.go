package capability

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestR1DeferredCapabilityAssetsRemainDeferred(t *testing.T) {
	entries, err := fs.ReadDir(assets.FS, "capabilities")
	if err != nil {
		t.Fatalf("read embedded capabilities: %v", err)
	}
	present := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		present[strings.TrimSuffix(entry.Name(), ".json")] = true
	}

	for _, name := range []string{"assignment_status", "assignment_expired", "poc_role", "ic_role"} {
		if present[name] {
			t.Fatalf("%s must remain deferred in R1; model it as a render cue or later capability, not a built-in asset", name)
		}
	}
}
