package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadChannelTokenFileRejectsBroadPermissionsBeforeParsing(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "invite")
	if err := os.WriteFile(path, []byte("mnch1_not-a-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readChannelTokenFile(path); err == nil {
		t.Fatal("readChannelTokenFile accepted broadly readable token file")
	}
}
