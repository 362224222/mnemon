package artifact

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	}{{cas.root, casDirectoryMode}, {cas.temp, casDirectoryMode}, {cas.trash, casDirectoryMode},
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

func TestCASLifecycleBarrierIsSharedByCanonicalRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "objects", "sha256")
	first, err := NewCAS(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCAS(root)
	if err != nil {
		t.Fatal(err)
	}
	other := openTestCAS(t)
	if first.coordination != second.coordination {
		t.Fatal("separate CAS instances did not share root coordination")
	}
	if first.coordination == other.coordination {
		t.Fatal("different CAS roots shared lifecycle coordination")
	}

	use, err := first.AcquireUse()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	acquired := make(chan *CASLease, 1)
	errs := make(chan error, 1)
	go func() {
		close(started)
		lease, acquireErr := second.AcquireExclusive()
		if acquireErr != nil {
			errs <- acquireErr
			return
		}
		acquired <- lease
	}()
	<-started
	waitForCASWriter(t, &first.coordination.lifecycle)
	select {
	case lease := <-acquired:
		lease.Release()
		t.Fatal("exclusive lifecycle crossed an outer use lease")
	default:
	}

	otherExclusive, err := other.AcquireExclusive()
	if err != nil {
		t.Fatal(err)
	}
	otherExclusive.Release()
	content := []byte("outer use may compose Put and a later Store checkpoint")
	if _, err := first.Put(model.Sum(content), content); err != nil {
		t.Fatalf("Put recursively acquired the lifecycle barrier: %v", err)
	}
	use.Release()
	use.Release()
	var exclusive *CASLease
	select {
	case err := <-errs:
		t.Fatal(err)
	case exclusive = <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("exclusive lifecycle did not acquire after use release")
	}
	// Exclusive callers use the same plain byte methods without recursively
	// acquiring their own write barrier.
	secondContent := []byte("exclusive lifecycle byte operation")
	if _, err := second.Put(model.Sum(secondContent), secondContent); err != nil {
		t.Fatalf("Put under exclusive lifecycle = %v", err)
	}
	exclusive.Release()
	exclusive.Release()
}

func TestCASDigestBarriersSerializeTombstoneAndAllowOtherShards(t *testing.T) {
	cas := openTestCAS(t)
	second, err := NewCAS(cas.root)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("digest barrier object")
	digest := model.Sum(content)
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	lock, err := cas.digestLock(digest)
	if err != nil {
		t.Fatal(err)
	}
	secondLock, err := second.digestLock(digest)
	if err != nil || secondLock != lock {
		t.Fatalf("same-root digest lock = (%p, %v), want %p", secondLock, err, lock)
	}

	// Holding the same read barrier models the complete protected Read
	// section. A tombstone writer cannot pass it.
	lock.RLock()
	started := make(chan struct{})
	done := make(chan error, 1)
	token := testCASToken(1)
	go func() {
		close(started)
		_, tombstoneErr := second.Tombstone(digest, token)
		done <- tombstoneErr
	}()
	<-started
	waitForCASWriter(t, lock)
	select {
	case err := <-done:
		t.Fatalf("tombstone crossed a digest read barrier: %v", err)
	default:
	}
	lock.RUnlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := cas.PurgeTombstone(digest, token); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}

	// A held digest write section models Put promotion. Tombstone cannot enter
	// until it leaves, while a different digest shard remains independently
	// usable.
	lock.Lock()
	started = make(chan struct{})
	done = make(chan error, 1)
	token = testCASToken(2)
	go func() {
		close(started)
		_, tombstoneErr := second.Tombstone(digest, token)
		done <- tombstoneErr
	}()
	<-started
	for index := 0; index < 256; index++ {
		runtime.Gosched()
	}
	select {
	case err := <-done:
		lock.Unlock()
		t.Fatalf("tombstone crossed a digest write barrier: %v", err)
	default:
	}
	otherContent, otherDigest := testCASDifferentShard(digest)
	otherLock, err := cas.digestLock(otherDigest)
	if err != nil {
		lock.Unlock()
		t.Fatal(err)
	}
	if otherLock == lock {
		lock.Unlock()
		t.Fatal("different test digest unexpectedly shared a shard")
	}
	if _, err := cas.Put(otherDigest, otherContent); err != nil {
		lock.Unlock()
		t.Fatalf("different digest was serialized globally: %v", err)
	}
	lock.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCASTombstoneCrashStatesReplayAndBoundedListing(t *testing.T) {
	cas := openTestCAS(t)
	content := []byte("recoverable CAS tombstone")
	digest := model.Sum(content)
	token := testCASToken(11)
	if _, err := cas.Put(digest, content); err != nil {
		t.Fatal(err)
	}
	status, err := cas.InspectTombstone(digest, token)
	if err != nil || status.State != CASTombstoneFinalOnly || status.Closed {
		t.Fatalf("final-only status = (%#v, %v)", status, err)
	}
	final, _ := cas.objectPath(digest, false)
	if err := os.Link(final, cas.tombstonePath(digest, token)); err != nil {
		t.Fatal(err)
	}
	status, err = cas.InspectTombstone(digest, token)
	if err != nil || status.State != CASTombstoneFinalAndTrash || status.Closed {
		t.Fatalf("linked recovery status = (%#v, %v)", status, err)
	}
	status, err = cas.Tombstone(digest, token)
	if err != nil || status.State != CASTombstoneTrashOnly || !status.Closed {
		t.Fatalf("closed tombstone = (%#v, %v)", status, err)
	}
	if replay, err := cas.Tombstone(digest, token); err != nil || replay != status {
		t.Fatalf("tombstone replay = (%#v, %v), want %#v", replay, err, status)
	}
	if _, err := cas.Read(digest, len(content)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tombstoned final Read = %v", err)
	}

	// Add two more closed objects in reverse lexical creation order. Listing
	// remains bounded and sorted by digest/token, independent of directory order.
	descriptors := []CASTombstoneDescriptor{{Digest: digest, Token: token,
		State: CASTombstoneTrashOnly, Closed: true}}
	for index := 0; index < 2; index++ {
		value := []byte(fmt.Sprintf("listed tombstone %d", 2-index))
		valueDigest := model.Sum(value)
		valueToken := testCASToken(byte(20 + index))
		if _, err := cas.Put(valueDigest, value); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.Tombstone(valueDigest, valueToken); err != nil {
			t.Fatal(err)
		}
		descriptors = append(descriptors, CASTombstoneDescriptor{Digest: valueDigest,
			Token: valueToken, State: CASTombstoneTrashOnly, Closed: true})
	}
	sort.Slice(descriptors, func(left, right int) bool {
		if descriptors[left].Digest != descriptors[right].Digest {
			return descriptors[left].Digest.String() < descriptors[right].Digest.String()
		}
		return fmt.Sprintf("%x", descriptors[left].Token) < fmt.Sprintf("%x", descriptors[right].Token)
	})
	listed, err := cas.ListTombstones(2)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListTombstones = (%#v, %v)", listed, err)
	}
	for index := range listed {
		if listed[index] != descriptors[index] {
			t.Fatalf("tombstone[%d] = %#v, want %#v", index, listed[index], descriptors[index])
		}
	}

	status, err = cas.PurgeTombstone(digest, token)
	if err != nil || status.State != CASTombstoneAbsent || !status.Closed {
		t.Fatalf("purged tombstone = (%#v, %v)", status, err)
	}
	if replay, err := cas.PurgeTombstone(digest, token); err != nil || replay != status {
		t.Fatalf("purge replay = (%#v, %v), want %#v", replay, err, status)
	}
	if _, err := cas.Tombstone(digest, token); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("absent Tombstone error = %v", err)
	}
}

func TestCASTombstoneFailsClosedOnFilesystemDrift(t *testing.T) {
	t.Run("different inode", func(t *testing.T) {
		cas := openTestCAS(t)
		content := []byte("different inode tombstone")
		digest := model.Sum(content)
		token := testCASToken(31)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cas.tombstonePath(digest, token), content, casObjectMode); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.InspectTombstone(digest, token); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("different-inode status error = %v", err)
		}
	})

	t.Run("extra hard link", func(t *testing.T) {
		cas := openTestCAS(t)
		content := []byte("unexpected external hard link")
		digest := model.Sum(content)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		final, _ := cas.objectPath(digest, false)
		if err := os.Link(final, filepath.Join(t.TempDir(), "foreign")); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.Read(digest, len(content)); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("hard-linked Read error = %v", err)
		}
		if _, err := cas.Put(digest, content); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("hard-linked replay error = %v", err)
		}
		if _, err := cas.Tombstone(digest, testCASToken(32)); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("hard-linked Tombstone error = %v", err)
		}
	})

	t.Run("foreign token", func(t *testing.T) {
		cas := openTestCAS(t)
		content := []byte("foreign tombstone token")
		digest := model.Sum(content)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.Tombstone(digest, testCASToken(41)); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.InspectTombstone(digest, testCASToken(42)); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("foreign-token status error = %v", err)
		}
	})

	t.Run("corrupted bytes and mode", func(t *testing.T) {
		cas := openTestCAS(t)
		content := []byte("corrupt before tombstone")
		digest := model.Sum(content)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		final, _ := cas.objectPath(digest, false)
		if err := os.WriteFile(final, []byte("CORRUPT before tombstone"), casObjectMode); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.Tombstone(digest, testCASToken(51)); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("corrupt Tombstone error = %v", err)
		}
		if err := os.Chmod(final, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.InspectTombstone(digest, testCASToken(51)); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("mode-drift status error = %v", err)
		}
	})

	t.Run("unsafe trash directory", func(t *testing.T) {
		cas := openTestCAS(t)
		if err := os.Remove(cas.trash); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), cas.trash); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.ListTombstones(1); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("symlink trash listing error = %v", err)
		}
	})
}

func TestCASListTombstonesRejectsUnsafeEntries(t *testing.T) {
	t.Run("noncanonical name", func(t *testing.T) {
		cas := openTestCAS(t)
		if err := os.WriteFile(filepath.Join(cas.trash, "unexpected"), []byte("x"), casObjectMode); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.ListTombstones(1); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("noncanonical tombstone listing error = %v", err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *CAS, string)
	}{
		{name: "mode", mutate: func(t *testing.T, _ *CAS, path string) {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "hash", mutate: func(t *testing.T, _ *CAS, path string) {
			if err := os.WriteFile(path, []byte("changed tombstone bytes"), casObjectMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "hard link", mutate: func(t *testing.T, _ *CAS, path string) {
			if err := os.Link(path, filepath.Join(t.TempDir(), "foreign")); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cas := openTestCAS(t)
			content := []byte("unsafe listed tombstone " + test.name)
			digest := model.Sum(content)
			token := testCASToken(byte(70 + len(test.name)))
			if _, err := cas.Put(digest, content); err != nil {
				t.Fatal(err)
			}
			if _, err := cas.Tombstone(digest, token); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, cas, cas.tombstonePath(digest, token))
			if _, err := cas.ListTombstones(1); !errors.Is(err, ErrCASCorruption) {
				t.Fatalf("unsafe tombstone listing error = %v", err)
			}
		})
	}

	t.Run("symlink", func(t *testing.T) {
		cas := openTestCAS(t)
		content := []byte("symlink tombstone")
		digest := model.Sum(content)
		token := testCASToken(91)
		outside := filepath.Join(t.TempDir(), "outside")
		if err := os.WriteFile(outside, content, casObjectMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, cas.tombstonePath(digest, token)); err != nil {
			t.Fatal(err)
		}
		if _, err := cas.ListTombstones(1); !errors.Is(err, ErrCASCorruption) {
			t.Fatalf("symlink tombstone listing error = %v", err)
		}
	})
}

func TestCASObjectListingIsStrictBoundedAndCanonical(t *testing.T) {
	cas := openTestCAS(t)
	cutoff := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	type object struct {
		content []byte
		digest  model.Digest
		old     bool
	}
	objects := make([]object, 0, 6)
	for index := 0; index < 6; index++ {
		content := []byte(fmt.Sprintf("candidate object %d", index))
		digest := model.Sum(content)
		if _, err := cas.Put(digest, content); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, object{content: content, digest: digest, old: index < 4})
		path, _ := cas.objectPath(digest, false)
		modified := cutoff.Add(-time.Duration(index+1) * time.Minute)
		if index == 4 {
			modified = cutoff
		}
		if index == 5 {
			modified = cutoff.Add(time.Minute)
		}
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatal(err)
		}
	}
	want := append([]object(nil), objects[:4]...)
	sort.Slice(want, func(left, right int) bool {
		return want[left].digest.String() < want[right].digest.String()
	})
	listed, err := cas.ListObjectsBefore(cutoff, 2)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListObjectsBefore = (%#v, %v)", listed, err)
	}
	for index := range listed {
		if listed[index].Digest != want[index].digest || listed[index].Size != uint64(len(want[index].content)) ||
			!listed[index].ModifiedAt.Before(cutoff) || listed[index].ModifiedAt.Location() != time.UTC {
			t.Fatalf("candidate[%d] = %#v, want digest=%s size=%d", index, listed[index],
				want[index].digest, len(want[index].content))
		}
	}
	if _, err := cas.ListObjectsBefore(cutoff, 0); !errors.Is(err, ErrCASInput) {
		t.Fatalf("zero listing limit error = %v", err)
	}

	// A noncanonical object inside a canonical shard is never silently treated
	// as a GC candidate.
	shard := filepath.Dir(mustCASObjectPath(t, cas, objects[0].digest))
	if err := os.WriteFile(filepath.Join(shard, "not-a-digest"), []byte("x"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.ListObjectsBefore(time.Now().Add(time.Hour), 10); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("noncanonical object listing error = %v", err)
	}
	if err := os.Remove(filepath.Join(shard, "not-a-digest")); err != nil {
		t.Fatal(err)
	}
	objectPath := mustCASObjectPath(t, cas, objects[0].digest)
	if err := os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, objects[0].content, casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, objectPath); err != nil {
		t.Fatal(err)
	}
	if _, err := cas.ListObjectsBefore(time.Now().Add(time.Hour), 10); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("symlink object listing error = %v", err)
	}
}

func TestCASObjectListingRejectsUnsafeRootEntries(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *CAS)
	}{
		{name: "unknown file", mutate: func(t *testing.T, cas *CAS) {
			if err := os.WriteFile(filepath.Join(cas.root, "unexpected"), []byte("x"), casObjectMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "uppercase shard", mutate: func(t *testing.T, cas *CAS) {
			if err := os.Mkdir(filepath.Join(cas.root, "AA"), casDirectoryMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "near-miss shard", mutate: func(t *testing.T, cas *CAS) {
			if err := os.Mkdir(filepath.Join(cas.root, "0"), casDirectoryMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "file shard", mutate: func(t *testing.T, cas *CAS) {
			if err := os.WriteFile(filepath.Join(cas.root, "ab"), []byte("x"), casObjectMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink shard", mutate: func(t *testing.T, cas *CAS) {
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.Mkdir(outside, casDirectoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(cas.root, "cd")); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cas := openTestCAS(t)
			test.mutate(t, cas)
			if _, err := cas.ListObjectsBefore(time.Now().Add(time.Hour), 1); !errors.Is(err, ErrCASCorruption) {
				t.Fatalf("unsafe root listing error = %v", err)
			}
		})
	}
}

func TestCASPruneTempsIsBoundedSafeAndSynchronized(t *testing.T) {
	cas := openTestCAS(t)
	cutoff := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	oldNames := []string{
		"cas-00000000000000000000000000000001.tmp",
		"cas-00000000000000000000000000000002.tmp",
		"cas-00000000000000000000000000000003.tmp",
	}
	for _, name := range oldNames {
		path := filepath.Join(cas.temp, name)
		if err := os.WriteFile(path, []byte(name), casObjectMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, cutoff.Add(-time.Minute), cutoff.Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	exact := filepath.Join(cas.temp, "cas-00000000000000000000000000000004.tmp")
	if err := os.WriteFile(exact, []byte("exact"), casObjectMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(exact, cutoff, cutoff); err != nil {
		t.Fatal(err)
	}
	near := filepath.Join(cas.temp, "cas-00000000000000000000000000000005.partial")
	if err := os.WriteFile(near, []byte("near miss"), casObjectMode); err != nil {
		t.Fatal(err)
	}

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
	if _, err := cas.PruneTempsBefore(time.Now().Add(time.Hour), 10); !errors.Is(err, ErrCASCorruption) {
		t.Fatalf("unsafe recognizable temp error = %v", err)
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

func waitForCASWriter(t *testing.T, lock *sync.RWMutex) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lock.TryRLock() {
			lock.RUnlock()
			runtime.Gosched()
			continue
		}
		return
	}
	t.Fatal("writer did not reach the shared barrier")
}

func testCASToken(seed byte) [32]byte {
	var token [32]byte
	for index := range token {
		token[index] = seed + byte(index)
	}
	return token
}

func testCASDifferentShard(digest model.Digest) ([]byte, model.Digest) {
	for index := 0; ; index++ {
		content := []byte(fmt.Sprintf("different CAS lock shard %d", index))
		candidate := model.Sum(content)
		if casDigestShard(candidate) != casDigestShard(digest) {
			return content, candidate
		}
	}
}

func mustCASObjectPath(t *testing.T, cas *CAS, digest model.Digest) string {
	t.Helper()
	path, err := cas.objectPath(digest, false)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func openTestCAS(t *testing.T) *CAS {
	t.Helper()
	cas, err := NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}
