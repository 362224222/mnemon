package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupHost(t *testing.T, root, host string) {
	t.Helper()
	var out, errw bytes.Buffer
	if _, err := New(root).Setup(context.Background(), &out, &errw, SetupOptions{
		Host:        host,
		Loops:       []string{"memory"},
		Principal:   "codex@project",
		ControlURL:  "http://127.0.0.1:8787",
		ProjectRoot: root,
	}); err != nil {
		t.Fatalf("setup %s: %v\n%s", host, err, errw.String())
	}
}

func TestSetupConfigOmitsBackgroundProjection(t *testing.T) {
	root := t.TempDir()
	setupHost(t, root, "codex")
	setupHost(t, root, "claude-code")
	raw, err := os.ReadFile(filepath.Join(root, ".mnemon", "harness", "local", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"hosts"`) || strings.Contains(string(raw), `"mirror_mode"`) {
		t.Fatalf("setup config must not declare background projection state:\n%s", raw)
	}
}

// T1 权限地板:setup 后私密目录 0700、token 0600;预先以 0755 存在的目录在重跑时被校正
// (local run 先于 setup 的窗口);同用户读写不受影响(本测试自身即同用户)。
func TestSetupTightensPrivateDirPermissions(t *testing.T) {
	root := t.TempDir()
	// 模拟 local run 先行:channel 目录先以宽权限存在
	pre := filepath.Join(root, ".mnemon", "harness", "channel")
	if err := os.MkdirAll(pre, 0o755); err != nil {
		t.Fatal(err)
	}
	setupHost(t, root, "codex")
	for _, rel := range []string{
		".mnemon/harness", ".mnemon/harness/local", ".mnemon/harness/channel",
		".mnemon/harness/channel/credentials",
	} {
		st, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if st.Mode().Perm() != 0o700 {
			t.Fatalf("%s: mode %o, want 0700", rel, st.Mode().Perm())
		}
	}
	tok := filepath.Join(root, ".mnemon", "harness", "channel", "credentials", "codex-project.token")
	if st, err := os.Stat(tok); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("token mode: %v %o, want 0600", err, st.Mode().Perm())
	}
}
