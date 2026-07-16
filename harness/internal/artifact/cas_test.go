package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCASPutReadReplayAndOwnerOnlyLayout(t *testing.T) {
	cas := openTestCAS(t)
	content := []byte("immutable Artifact object")
	digest := model.Sum(content)
	first, err := cas.Put(digest, content)
	if err != nil || first.Replayed || first.Digest != digest || first.Size != uint64(len(content)) {
		t.Fatalf("first CAS Put = (%#v, %v)", first, err)
	}
	replay, err := cas.Put(digest, append([]byte{}, content...))
	if err != nil || !replay.Replayed {
		t.Fatalf("CAS replay = (%#v, %v)", replay, err)
	}
	read, err := cas.Read(digest, len(content))
	if err != nil || string(read) != string(content) {
		t.Fatalf("CAS Read = (%q, %v)", read, err)
	}
	path, _ := cas.objectPath(digest, false)
	for _, check := range []struct {
		path string
		mode os.FileMode
	}{{cas.root, casDirectoryMode}, {cas.temp, casDirectoryMode},
		{filepath.Dir(path), casDirectoryMode}, {path, casObjectMode}} {
		info, err := os.Lstat(check.path)
		if err != nil {
			t.Fatalf("stat %s: %v", check.path, err)
		}
		if info.Mode().Perm() != check.mode {
			t.Fatalf("mode %s = %v, want %04o", check.path, info.Mode().Perm(), check.mode)
		}
	}
	if temps, err := cas.TempFiles(); err != nil || len(temps) != 0 {
		t.Fatalf("temps after promotion = (%v, %v)", temps, err)
	}
}

func TestCASDetectsDigestMismatchAndExistingCorruption(t *testing.T) {
	cas := openTestCAS(t)
	content := []byte("expected")
	digest := model.Sum(content)
	if _, err := cas.Put(model.Sum([]byte("other")), content); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("wrong supplied digest error = %v", err)
	}
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	path, _ := cas.objectPath(digest, false)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Read(digest, len(content)); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("mode drift Read error = %v", err)
	}
	if err := os.Chmod(path, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Put(digest, content); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("existing corruption Put error = %v", err)
	}
	if _, err := cas.Read(digest, len(content)); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("existing corruption Read error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(cas.root, "outside"), path); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Put(digest, content); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("symlink object error = %v", err)
	}
}

func TestCASConcurrentNoOverwritePromotionIsSingleEffect(t *testing.T) {
	cas := openTestCAS(t)
	content := make([]byte, BlockSize)
	for index := range content {
		content[index] = byte(index % 251)
	}
	digest := model.Sum(content)
	start := make(chan struct{})
	results := make(chan PutResult, 8)
	errs := make(chan error, 8)
	var wait sync.WaitGroup
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := cas.Put(digest, content)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Put error = %v", err)
		}
	}
	created := 0
	for result := range results {
		if !result.Replayed {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("concurrent creator count = %d, want 1", created)
	}
	if got, err := cas.Read(digest, BlockSize); err != nil || len(got) != BlockSize {
		t.Fatalf("concurrent object read = (%d, %v)", len(got), err)
	}
	if temps, err := cas.TempFiles(); err != nil || len(temps) != 0 {
		t.Fatalf("concurrent promotion temps = (%v, %v)", temps, err)
	}
}

func TestCASTempsAreRecognizableButNeverAuthority(t *testing.T) {
	cas := openTestCAS(t)
	name := "cas-0123456789abcdef0123456789abcdef.tmp"
	tempPath := filepath.Join(cas.temp, name)
	content := []byte("crash leftover")
	if err := os.WriteFile(tempPath, content, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if temps, err := cas.TempFiles(); err != nil || len(temps) != 1 || temps[0] != name {
		t.Fatalf("TempFiles() = (%v, %v)", temps, err)
	}
	if _, err := cas.Read(model.Sum(content), len(content)); err == nil {
		t.Fatal("orphan temp became readable CAS authority")
	}
	if _, err := cas.Put(model.Sum(content), content); err != nil {
		t.Fatal(err)
	}
	if temps, err := cas.TempFiles(); err != nil || len(temps) != 1 {
		t.Fatalf("unrelated crash temp was consumed: (%v, %v)", temps, err)
	}
}

func TestCASRejectsSymlinkRootAndOversizeObjects(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, casDirectoryMode); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCAS(link); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("symlink CAS root error = %v", err)
	}
	cas := openTestCAS(t)
	oversize := make([]byte, maxCASObjectSize+1)
	if _, err := cas.Put(model.Sum(oversize), oversize); !errors.Is(err, ErrCASInput) {
		t.Fatalf("oversize Put error = %v", err)
	}
}

func openTestCAS(t *testing.T) *CAS {
	t.Helper()
	cas, err := NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}
