package cli

import "testing"

func TestTakeJSONFlagWorksBeforeOrAfterChannelName(t *testing.T) {
	t.Parallel()
	args, enabled := takeJSONFlag([]string{"review", "--json"})
	if !enabled || len(args) != 1 || args[0] != "review" {
		t.Fatalf("takeJSONFlag() = %#v, %v", args, enabled)
	}
}
