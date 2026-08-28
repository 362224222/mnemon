package memory

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/internal/memory/embed"
)

func TestNewReturnsComposableMemoryRoot(t *testing.T) {
	oldVersion, oldRootVersion := version, rootCmd.Version
	t.Cleanup(func() {
		version = oldVersion
		rootCmd.Version = oldRootVersion
	})

	cmd := New("test-version")
	if cmd.Use != "mnemon" {
		t.Fatalf("root use = %q, want mnemon", cmd.Use)
	}
	if cmd.Version != "test-version" {
		t.Fatalf("root version = %q, want test-version", cmd.Version)
	}
	for _, name := range []string{"remember", "recall", "setup", "store"} {
		if child, _, err := cmd.Find([]string{name}); err != nil || child == cmd {
			t.Fatalf("memory command %q is not registered", name)
		}
	}
}

func TestOpenDBRejectsInvalidStoreNameFromEnv(t *testing.T) {
	t.Setenv("MNEMON_STORE", "../outside")

	oldDataDir, oldStoreName, oldReadOnly := dataDir, storeName, readOnly
	t.Cleanup(func() {
		dataDir, storeName, readOnly = oldDataDir, oldStoreName, oldReadOnly
	})
	dataDir = t.TempDir()
	storeName = ""
	readOnly = false

	db, err := openDB()
	if err == nil {
		if db != nil {
			db.Close()
		}
		t.Fatal("expected invalid store name error")
	}
	if !strings.Contains(err.Error(), "invalid store name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenDBRejectsInvalidStoreNameFromFlag(t *testing.T) {
	oldDataDir, oldStoreName, oldReadOnly := dataDir, storeName, readOnly
	t.Cleanup(func() {
		dataDir, storeName, readOnly = oldDataDir, oldStoreName, oldReadOnly
	})
	dataDir = t.TempDir()
	storeName = "../outside"
	readOnly = false

	db, err := openDB()
	if err == nil {
		if db != nil {
			db.Close()
		}
		t.Fatal("expected invalid store name error")
	}
	if !strings.Contains(err.Error(), "invalid store name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestResolveEmbedModelChain exercises the cmd → embed pipeline for the
// --embed-model flag, mirroring how cobra will hand the value off at runtime.
// The test runs against embed.NewClientWithModel directly so it does not
// require a live provider. Environment variables are not consulted; the
// precedence is --embed-model flag > embed.yml model > built-in default.
func TestResolveEmbedModelChain(t *testing.T) {
	oldEmbedModel := embedModel
	t.Cleanup(func() { embedModel = oldEmbedModel })

	cases := []struct {
		name      string
		flagValue string
		want      string
	}{
		{
			name:      "flag wins over default",
			flagValue: "flag-model",
			want:      "flag-model",
		},
		{
			name:      "empty flag falls through to built-in default",
			flagValue: "",
			want:      embed.DefaultModel,
		},
		{
			name:      "flag value passes through verbatim",
			flagValue: "nomic-embed-text-v2-moe:latest",
			want:      "nomic-embed-text-v2-moe:latest",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			embedModel = tc.flagValue
			client := embed.NewClientWithModel(resolveEmbedModel())
			if got := client.Model(); got != tc.want {
				t.Errorf("model resolution: want %q, got %q", tc.want, got)
			}
		})
	}
}
