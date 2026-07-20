package localapi

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestEnsureProfileCredentialCreateLoadAndVerify(t *testing.T) {
	nodeState := newCredentialNodeState(t)
	digest, created, err := EnsureProfileCredential(nodeState)
	if err != nil || !created || digest.IsZero() {
		t.Fatalf("EnsureProfileCredential() = (%s, %t, %v)", digest.String(), created, err)
	}
	profiles := filepath.Join(nodeState, profileCredentialDirectory)
	assertCredentialPath(t, profiles, true, ownerDirectoryMode)
	path := filepath.Join(profiles, profileCredentialFilename())
	assertCredentialPath(t, path, false, ownerRegularFileMode)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != profileTokenBytes || raw[len(raw)-1] != '\n' {
		t.Fatalf("credential wire length/newline = %d/%q", len(raw), raw[len(raw)-1:])
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(string(raw[:len(raw)-1]))
	if err != nil || len(decoded) != opaqueSecretBytes || model.Sum(decoded) != digest {
		t.Fatalf("credential wire does not bind returned digest: len=%d, err=%v", len(decoded), err)
	}
	clear(decoded)
	if err := VerifyProfileCredential(nodeState, digest); err != nil {
		t.Fatalf("VerifyProfileCredential() error = %v", err)
	}
	firstInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	loaded, createdAgain, err := EnsureProfileCredential(nodeState)
	if err != nil || createdAgain || loaded != digest {
		t.Fatalf("second EnsureProfileCredential() = (%s, %t, %v), want (%s, false, nil)",
			loaded.String(), createdAgain, err, digest.String())
	}
	secondInfo, err := os.Lstat(path)
	if err != nil || !os.SameFile(firstInfo, secondInfo) {
		t.Fatalf("second ensure replaced credential inode: %v", err)
	}
	assertOnlyCredentialEntries(t, profiles)
}

func TestEnsureProfileCredentialConcurrentThreadsConverge(t *testing.T) {
	nodeState := newCredentialNodeState(t)
	const callers = 32
	start := make(chan struct{})
	results := make(chan credentialEnsureResult, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for index := 0; index < callers; index++ {
		go func() {
			ready.Done()
			<-start
			digest, created, err := EnsureProfileCredential(nodeState)
			results <- credentialEnsureResult{digest: digest, created: created, err: err}
		}()
	}
	ready.Wait()
	close(start)
	var want model.Digest
	createdCount := 0
	for index := 0; index < callers; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent EnsureProfileCredential() error = %v", result.err)
		}
		if want.IsZero() {
			want = result.digest
		}
		if result.digest != want {
			t.Fatalf("concurrent digest = %s, want %s", result.digest.String(), want.String())
		}
		if result.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created results = %d, want exactly one", createdCount)
	}
	if err := VerifyProfileCredential(nodeState, want); err != nil {
		t.Fatalf("concurrent credential does not verify: %v", err)
	}
	assertOnlyCredentialEntries(t, filepath.Join(nodeState, profileCredentialDirectory))
}

func TestEnsureProfileCredentialConcurrentProcessesConverge(t *testing.T) {
	nodeState := newCredentialNodeState(t)
	gate := filepath.Join(t.TempDir(), "start")
	const callers = 6
	type processCall struct {
		command *exec.Cmd
		output  bytes.Buffer
		result  string
	}
	calls := make([]processCall, callers)
	for index := range calls {
		calls[index].result = filepath.Join(t.TempDir(), fmt.Sprintf("result-%d", index))
		calls[index].command = exec.Command(os.Args[0], "-test.run=^TestEnsureProfileCredentialProcessHelper$")
		calls[index].command.Env = append(os.Environ(),
			"MNEMON_CREDENTIAL_HELPER=1",
			"MNEMON_CREDENTIAL_NODE_STATE="+nodeState,
			"MNEMON_CREDENTIAL_GATE="+gate,
			"MNEMON_CREDENTIAL_RESULT="+calls[index].result,
		)
		calls[index].command.Stdout = &calls[index].output
		calls[index].command.Stderr = &calls[index].output
		if err := calls[index].command.Start(); err != nil {
			t.Fatalf("start helper %d: %v", index, err)
		}
	}
	if err := os.WriteFile(gate, []byte("go\n"), ownerRegularFileMode); err != nil {
		t.Fatal(err)
	}
	var want model.Digest
	createdCount := 0
	for index := range calls {
		if err := calls[index].command.Wait(); err != nil {
			t.Fatalf("helper %d error = %v\n%s", index, err, calls[index].output.String())
		}
		raw, err := os.ReadFile(calls[index].result)
		if err != nil {
			t.Fatalf("read helper %d result: %v\n%s", index, err, calls[index].output.String())
		}
		fields := strings.Fields(string(raw))
		if len(fields) != 2 {
			t.Fatalf("helper %d result = %q", index, raw)
		}
		digest, err := model.ParseDigest(fields[0])
		if err != nil || (fields[1] != "true" && fields[1] != "false") {
			t.Fatalf("helper %d result = %q, err=%v", index, raw, err)
		}
		if want.IsZero() {
			want = digest
		}
		if digest != want {
			t.Fatalf("helper %d digest = %s, want %s", index, digest.String(), want.String())
		}
		if fields[1] == "true" {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("process created results = %d, want exactly one", createdCount)
	}
	if err := VerifyProfileCredential(nodeState, want); err != nil {
		t.Fatalf("process credential does not verify: %v", err)
	}
}

func TestEnsureProfileCredentialProcessHelper(t *testing.T) {
	if os.Getenv("MNEMON_CREDENTIAL_HELPER") != "1" {
		return
	}
	gate := os.Getenv("MNEMON_CREDENTIAL_GATE")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Lstat(gate); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for process concurrency gate")
		}
		time.Sleep(time.Millisecond)
	}
	digest, created, err := EnsureProfileCredential(os.Getenv("MNEMON_CREDENTIAL_NODE_STATE"))
	if err != nil {
		t.Fatal(err)
	}
	result := []byte(fmt.Sprintf("%s %t\n", digest.String(), created))
	if err := os.WriteFile(os.Getenv("MNEMON_CREDENTIAL_RESULT"), result, ownerRegularFileMode); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureProfileCredentialCleansOnlySafeReservedStaging(t *testing.T) {
	t.Run("before publication", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		profiles := installCredentialProfiles(t, nodeState)
		staged := filepath.Join(profiles,
			profileCredentialTempPrefix+strings.Repeat("0", 32)+profileCredentialTempSuffix)
		if err := os.WriteFile(staged, []byte("crash-leftover"), ownerRegularFileMode); err != nil {
			t.Fatal(err)
		}
		nearMiss := filepath.Join(profiles,
			profileCredentialTempPrefix+strings.Repeat("g", 32)+profileCredentialTempSuffix)
		if err := os.WriteFile(nearMiss, []byte("not reserved"), ownerRegularFileMode); err != nil {
			t.Fatal(err)
		}
		if _, created, err := EnsureProfileCredential(nodeState); err != nil || !created {
			t.Fatalf("EnsureProfileCredential() = (_, %t, %v)", created, err)
		}
		if _, err := os.Lstat(staged); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reserved staging remains: %v", err)
		}
		if raw, err := os.ReadFile(nearMiss); err != nil || string(raw) != "not reserved" {
			t.Fatalf("near-miss staging changed: %q, %v", raw, err)
		}
	})

	t.Run("after publication", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		digest, _, err := EnsureProfileCredential(nodeState)
		if err != nil {
			t.Fatal(err)
		}
		profiles := filepath.Join(nodeState, profileCredentialDirectory)
		staged := filepath.Join(profiles,
			profileCredentialTempPrefix+strings.Repeat("1", 32)+profileCredentialTempSuffix)
		if err := os.Link(filepath.Join(profiles, profileCredentialFilename()), staged); err != nil {
			t.Fatal(err)
		}
		loaded, created, err := EnsureProfileCredential(nodeState)
		if err != nil || created || loaded != digest {
			t.Fatalf("restart EnsureProfileCredential() = (%s, %t, %v)", loaded.String(), created, err)
		}
		if _, err := os.Lstat(staged); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("published staging remains: %v", err)
		}
	})

	t.Run("symlink fails closed without following", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		profiles := installCredentialProfiles(t, nodeState)
		target := filepath.Join(t.TempDir(), "outside")
		if err := os.WriteFile(target, []byte("do not follow"), ownerRegularFileMode); err != nil {
			t.Fatal(err)
		}
		staged := filepath.Join(profiles,
			profileCredentialTempPrefix+strings.Repeat("2", 32)+profileCredentialTempSuffix)
		if err := os.Symlink(target, staged); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureProfileCredential(nodeState); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("unsafe staging error = %v", err)
		}
		if raw, err := os.ReadFile(target); err != nil || string(raw) != "do not follow" {
			t.Fatalf("staging symlink target changed: %q, %v", raw, err)
		}
		if _, err := os.Lstat(filepath.Join(profiles, profileCredentialFilename())); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unsafe staging allowed credential publication: %v", err)
		}
	})
}

func TestEnsureProfileCredentialPreservesInvalidExistingPath(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		mode os.FileMode
	}{
		{name: "empty", raw: nil, mode: ownerRegularFileMode},
		{name: "malformed", raw: []byte("not-a-token\n"), mode: ownerRegularFileMode},
		{name: "missing newline", raw: []byte(encodedCredential(0x41)), mode: ownerRegularFileMode},
		{name: "padded", raw: []byte(encodedCredential(0x42) + "=\n"), mode: ownerRegularFileMode},
		{name: "extra newline", raw: []byte(encodedCredential(0x43) + "\n\n"), mode: ownerRegularFileMode},
		{name: "broad mode", raw: []byte(encodedCredential(0x44) + "\n"), mode: 0o644},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			nodeState := newCredentialNodeState(t)
			profiles := installCredentialProfiles(t, nodeState)
			path := filepath.Join(profiles, profileCredentialFilename())
			if err := os.WriteFile(path, test.raw, test.mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, test.mode); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := EnsureProfileCredential(nodeState); !errors.Is(err, ErrUnsafeClientState) {
				t.Fatalf("EnsureProfileCredential() error = %v", err)
			}
			after, err := os.Lstat(path)
			current, readErr := os.ReadFile(path)
			if err != nil || readErr != nil || !os.SameFile(before, after) || !bytes.Equal(current, test.raw) {
				t.Fatalf("invalid credential was replaced: stat=%v read=%v raw=%q", err, readErr, current)
			}
		})
	}

	t.Run("symlink", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		profiles := installCredentialProfiles(t, nodeState)
		target := filepath.Join(t.TempDir(), "outside.token")
		want := []byte(encodedCredential(0x51) + "\n")
		if err := os.WriteFile(target, want, ownerRegularFileMode); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(profiles, profileCredentialFilename())
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureProfileCredential(nodeState); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("symlink credential error = %v", err)
		}
		if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("credential symlink was replaced: %v, %v", info, err)
		}
		if raw, err := os.ReadFile(target); err != nil || !bytes.Equal(raw, want) {
			t.Fatalf("credential symlink target changed: %q, %v", raw, err)
		}
	})
}

func TestEnsureProfileCredentialRejectsUnsafeDirectories(t *testing.T) {
	t.Run("relative Node state", func(t *testing.T) {
		if _, _, err := EnsureProfileCredential("relative/node"); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("relative path error = %v", err)
		}
	})
	t.Run("Node state mode", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		if err := os.Chmod(nodeState, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureProfileCredential(nodeState); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("Node mode error = %v", err)
		}
	})
	t.Run("Node state symlink", func(t *testing.T) {
		realState := newCredentialNodeState(t)
		link := filepath.Join(t.TempDir(), "node-link")
		if err := os.Symlink(realState, link); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureProfileCredential(link); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("Node symlink error = %v", err)
		}
	})
	t.Run("Profile mode", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		profiles := installCredentialProfiles(t, nodeState)
		if err := os.Chmod(profiles, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureProfileCredential(nodeState); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("Profile mode error = %v", err)
		}
	})
	t.Run("Profile symlink", func(t *testing.T) {
		nodeState := newCredentialNodeState(t)
		outside := filepath.Join(t.TempDir(), "outside-profiles")
		if err := os.Mkdir(outside, ownerDirectoryMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(nodeState, profileCredentialDirectory)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := EnsureProfileCredential(nodeState); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("Profile symlink error = %v", err)
		}
		entries, err := os.ReadDir(outside)
		if err != nil || len(entries) != 0 {
			t.Fatalf("Profile symlink target changed: %v, %v", entries, err)
		}
	})
}

type credentialEnsureResult struct {
	digest  model.Digest
	created bool
	err     error
}

func newCredentialNodeState(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(path, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return path
}

func installCredentialProfiles(t *testing.T, nodeState string) string {
	t.Helper()
	path := filepath.Join(nodeState, profileCredentialDirectory)
	if err := os.Mkdir(path, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return path
}

func encodedCredential(fill byte) string {
	return base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{fill}, opaqueSecretBytes))
}

func assertCredentialPath(t *testing.T, path string, directory bool, mode os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != mode || info.IsDir() != directory ||
		(!directory && !info.Mode().IsRegular()) {
		t.Fatalf("path %q mode/type = %v, want directory=%t mode=%04o", path, info.Mode(), directory, mode)
	}
	owner, err := fileOwnerUID(info)
	if err != nil || owner != uint32(os.Geteuid()) {
		t.Fatalf("path %q owner = %d, want %d, err=%v", path, owner, os.Geteuid(), err)
	}
}

func assertOnlyCredentialEntries(t *testing.T, profiles string) {
	t.Helper()
	entries, err := os.ReadDir(profiles)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != profileCredentialFilename() {
		t.Fatalf("Profile entries = %v, want only %s", entries, profileCredentialFilename())
	}
}
