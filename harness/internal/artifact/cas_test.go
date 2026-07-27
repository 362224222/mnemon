package artifact

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

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

func TestCASRecoversDurablePromotionTempWithoutAcceptingForeignLinks(t *testing.T) {
	cas := openTestCAS(t)
	content := []byte("promotion crash recovery")
	digest := model.Sum(content)
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	final := mustCASObjectPath(t, cas, digest)
	temp := filepath.Join(cas.temp, "cas-11111111111111111111111111111111.tmp")
	if err := os.Link(final, temp); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Read(digest, len(content)); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("Read accepted an unfinished promotion link: %v", err)
	}
	replay, err := cas.Put(digest, content)
	if err != nil || !replay.Replayed {
		t.Fatalf("promotion replay = (%#v, %v)", replay, err)
	}
	if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered promotion temp remains: %v", err)
	}
	if got, err := cas.Read(digest, len(content)); err != nil || string(got) != string(content) {
		t.Fatalf("recovered final Read = (%q, %v)", got, err)
	}

	// Orphan pruning can close the same crash state even when no operation
	// returns to replay Put.
	temp = filepath.Join(cas.temp, "cas-22222222222222222222222222222222.tmp")
	if err := os.Link(final, temp); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(temp, old, old); err != nil {
		t.Fatal(err)
	}
	removed, err := cas.PruneTempsBefore(time.Now().Add(-time.Hour), 1)
	if err != nil || len(removed) != 1 || removed[0] != filepath.Base(temp) {
		t.Fatalf("promotion temp prune = (%v, %v)", removed, err)
	}
	if got, err := cas.Read(digest, len(content)); err != nil || string(got) != string(content) {
		t.Fatalf("prune-recovered final Read = (%q, %v)", got, err)
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

func TestCASPruneTempsIsBoundedSafeAndSynchronized(t *testing.T) {
	cas := openTestCAS(t)
	cutoff := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	fixture := createCASTempFixture(t, cas, cutoff)
	oldNames, exact, near := fixture.oldNames, fixture.exact, fixture.near

	removed, err := cas.PruneTempsBefore(cutoff, 2)
	if err != nil || fmt.Sprint(removed) != fmt.Sprint(oldNames[:2]) {
		t.Fatalf("bounded temp prune = (%v, %v), want %v", removed, err, oldNames[:2])
	}
	removed, err = cas.PruneTempsBefore(cutoff, 10)
	if err != nil || len(removed) != 1 || removed[0] != oldNames[2] {
		t.Fatalf("remaining temp prune = (%v, %v)", removed, err)
	}
	for _, path := range []string{exact, near} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("temp prune removed protected path %s: %v", path, err)
		}
	}

	unsafe := filepath.Join(cas.temp, "cas-00000000000000000000000000000006.tmp")
	if err := os.Symlink(near, unsafe); err != nil {
		t.Fatal(err)
	}
	var unsafeErr error
	for cycle := 0; cycle < 8 && !errors.Is(unsafeErr, ErrCASCorruption); cycle++ {
		_, unsafeErr = cas.PruneTempsBefore(time.Now().Add(time.Hour), 10)
	}
	if !errors.Is(unsafeErr, ErrCASCorruption) {
		t.Fatalf("unsafe recognizable temp was not found after cursor wrap: %v", unsafeErr)
	}
	if _, err := os.Lstat(unsafe); err != nil {
		t.Fatalf("unsafe temp was removed: %v", err)
	}

	// The registry temp mutex is shared across instances and covers pruning's
	// selection/removal interval as well as Put's temp lifetime.
	second, err := NewCAS(cas.root)
	if err != nil {
		t.Fatal(err)
	}
	cas.coordination.temp.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, pruneErr := second.PruneTempsBefore(cutoff, 1)
		done <- pruneErr
	}()
	<-started
	for index := 0; index < 256; index++ {
		runtime.Gosched()
	}
	select {
	case err := <-done:
		cas.coordination.temp.Unlock()
		t.Fatalf("prune crossed active temp mutex: %v", err)
	default:
	}
	cas.coordination.temp.Unlock()
	if err := <-done; !errors.Is(err, ErrCASCorruption) {
		// The still-present unsafe recognizable symlink is intentionally
		// detected once the synchronization barrier opens.
		t.Fatalf("post-barrier prune error = %v", err)
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
