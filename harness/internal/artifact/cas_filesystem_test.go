package artifact

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type casTempFixture struct {
	oldNames []string
	exact    string
	near     string
}

func createCASTempFixture(t *testing.T, cas *CAS, cutoff time.Time) casTempFixture {
	t.Helper()
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
	return casTempFixture{oldNames: oldNames, exact: exact, near: near}
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
	return openCASAt(t, filepath.Join(t.TempDir(), "objects", "sha256"))
}

func openCASAt(t *testing.T, root string) *CAS {
	t.Helper()
	cas, err := NewCAS(root)
	if err != nil {
		t.Fatal(err)
	}
	return cas
}
