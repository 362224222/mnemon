package localapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPendingJournalCanonicalOwnerOnlyFile(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	key := repeatedOpaqueBytes(0x71)
	locator := repeatedOpaqueBytes(0x72)
	clockTime := time.Date(2026, 7, 16, 12, 34, 56, 123456789,
		time.FixedZone("CST", 8*60*60))
	store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
		Random: bytes.NewReader(append(append([]byte(nil), key...), locator...)),
		Clock:  func() time.Time { return clockTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	requestDigest := model.Sum([]byte("canonical action input"))
	contextDigest := model.Sum([]byte("context file bytes"))
	journal, reused, err := store.FindOrCreate(requestDigest, &contextDigest)
	if err != nil || reused {
		t.Fatalf("FindOrCreate() = %#v, reused=%v, %v", journal, reused, err)
	}
	wantKey := base64.RawURLEncoding.EncodeToString(key)
	wantLocator := base64.RawURLEncoding.EncodeToString(locator)
	wantPath := filepath.Join(nodeState, "operations", wantLocator+pendingJournalSuffix)
	if journal.Path() != wantPath || journal.OperationKeyHeader() != wantKey ||
		journal.OperationKeyHash() != model.Sum(key) || journal.RequestDigest() != requestDigest {
		t.Fatalf("journal identity = (%q, %q, %s, %s)", journal.Path(),
			journal.OperationKeyHeader(), journal.OperationKeyHash(), journal.RequestDigest())
	}
	if got, ok := journal.ContextFileDigest(); !ok || got != contextDigest {
		t.Fatalf("context digest = %s, %v", got, ok)
	}
	if !journal.CreatedAt().Equal(clockTime) || journal.CreatedAt().Location() != time.UTC {
		t.Fatalf("created_at = %s (%v)", journal.CreatedAt(), journal.CreatedAt().Location())
	}
	assertOwnerPath(t, filepath.Join(nodeState, "operations"), true, ownerDirectoryMode)
	assertOwnerPath(t, wantPath, false, ownerRegularFileMode)
	raw, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatal(err)
	}
	want, err := model.CanonicalMarshal(pendingJournalWire{
		SchemaVersion: SchemaVersion, OperationKey: wantKey, RequestDigest: requestDigest.String(),
		ContextFileDigest: stringPointer(contextDigest.String()),
		CreatedAt:         clockTime.UTC().Format(time.RFC3339Nano),
	})
	if err != nil || !bytes.Equal(raw, want) || bytes.HasSuffix(raw, []byte("\n")) {
		t.Fatalf("journal bytes = %s, want %s, %v", raw, want, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	wantFields := []string{"context_file_digest", "created_at", "operation_key", "request_digest", "schema_version"}
	gotFields := make([]string, 0, len(fields))
	for field := range fields {
		gotFields = append(gotFields, field)
	}
	sort.Strings(gotFields)
	if strings.Join(gotFields, ",") != strings.Join(wantFields, ",") {
		t.Fatalf("pending fields = %v", gotFields)
	}
}

func TestPendingJournalResponseLossReuseAndConfirmedIdenticalOffer(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	firstKey, firstLocator := repeatedOpaqueBytes(0x11), repeatedOpaqueBytes(0x12)
	secondKey, secondLocator := repeatedOpaqueBytes(0x21), repeatedOpaqueBytes(0x22)
	entropy := append(append(append(append([]byte(nil), firstKey...), firstLocator...), secondKey...), secondLocator...)
	createdTimes := []time.Time{
		time.Date(2026, 7, 16, 1, 2, 3, 4, time.UTC),
		time.Date(2026, 7, 16, 1, 2, 4, 5, time.UTC),
	}
	clockIndex := 0
	store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
		Random: bytes.NewReader(entropy),
		Clock: func() time.Time {
			value := createdTimes[clockIndex]
			clockIndex++
			return value
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	requestDigest := model.Sum([]byte("byte-identical contextless offer"))
	first, reused, err := store.FindOrCreate(requestDigest, nil)
	if err != nil || reused {
		t.Fatalf("first journal = %#v, %v, %v", first, reused, err)
	}
	replay, reused, err := store.FindOrCreate(requestDigest, nil)
	if err != nil || !reused || replay.Path() != first.Path() ||
		replay.OperationKeyHeader() != first.OperationKeyHeader() {
		t.Fatalf("response-loss replay = %#v, %v, %v", replay, reused, err)
	}
	if clockIndex != 1 {
		t.Fatalf("reuse invoked creation clock %d times", clockIndex)
	}
	if err := store.RemoveTerminal(replay); err != nil {
		t.Fatal(err)
	}
	second, reused, err := store.FindOrCreate(requestDigest, nil)
	if err != nil || reused {
		t.Fatalf("confirmed identical second offer = %#v, %v, %v", second, reused, err)
	}
	if second.OperationKeyHeader() == first.OperationKeyHeader() || second.Path() == first.Path() {
		t.Fatal("confirmed identical second offer reused its terminal operation identity")
	}
	if second.OperationKeyHeader() != base64.RawURLEncoding.EncodeToString(secondKey) ||
		filepath.Base(second.Path()) != base64.RawURLEncoding.EncodeToString(secondLocator)+pendingJournalSuffix {
		t.Fatalf("second identity = %q, %q", second.OperationKeyHeader(), second.Path())
	}
}

func TestPendingJournalReusesAcrossClientRestartWithoutEntropy(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	requestDigest := model.Sum([]byte("lost response"))
	contextDigest := model.Sum([]byte("stable context"))
	firstStore, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
		Random: bytes.NewReader(append(repeatedOpaqueBytes(0x31), repeatedOpaqueBytes(0x32)...)),
		Clock:  func() time.Time { return time.Date(2026, 7, 16, 2, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := firstStore.FindOrCreate(requestDigest, &contextDigest)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
		Random: errorReader{},
		Clock: func() time.Time {
			t.Fatal("reused journal must not read the creation clock")
			return time.Time{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, reused, err := secondStore.FindOrCreate(requestDigest, &contextDigest)
	if err != nil || !reused || replayed.Path() != first.Path() ||
		replayed.OperationKeyHeader() != first.OperationKeyHeader() {
		t.Fatalf("restart replay = %#v, %v, %v", replayed, reused, err)
	}
}

func TestPendingJournalConcurrentSameInputHasOneIdentity(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	store, err := NewPendingJournalStore(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	requestDigest := model.Sum([]byte("concurrent action"))
	const callers = 16
	type result struct {
		journal PendingJournal
		reused  bool
		err     error
	}
	results := make(chan result, callers)
	var start sync.WaitGroup
	start.Add(1)
	for index := 0; index < callers; index++ {
		go func() {
			start.Wait()
			journal, reused, err := store.FindOrCreate(requestDigest, nil)
			results <- result{journal, reused, err}
		}()
	}
	start.Done()
	var path, key string
	creators := 0
	for index := 0; index < callers; index++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if !result.reused {
			creators++
		}
		if index == 0 {
			path, key = result.journal.Path(), result.journal.OperationKeyHeader()
		} else if result.journal.Path() != path || result.journal.OperationKeyHeader() != key {
			t.Fatalf("concurrent identity = %q/%q, want %q/%q", result.journal.Path(),
				result.journal.OperationKeyHeader(), path, key)
		}
	}
	if creators != 1 {
		t.Fatalf("journal creators = %d, want 1", creators)
	}
}

func TestPendingJournalRejectsNoncanonicalUntrustedFiles(t *testing.T) {
	t.Parallel()
	validRequest := model.Sum([]byte("valid request"))
	validContext := model.Sum([]byte("valid context"))
	validTime := "2026-07-16T03:04:05.000000006Z"
	validWire := pendingJournalWire{
		SchemaVersion: SchemaVersion,
		OperationKey:  testOpaqueValue(0x41),
		RequestDigest: validRequest.String(),
		CreatedAt:     validTime,
	}
	validRaw, err := model.CanonicalMarshal(validWire)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name  string
		build func(*testing.T, string)
	}{
		{"unknown-field", func(t *testing.T, operations string) {
			writePendingMap(t, operations, 0x42, map[string]any{
				"schema_version": 1, "operation_key": validWire.OperationKey,
				"request_digest": validWire.RequestDigest, "context_file_digest": nil,
				"created_at": validTime, "content": "must never be stored",
			})
		}},
		{"missing-nullable-field", func(t *testing.T, operations string) {
			writePendingMap(t, operations, 0x42, map[string]any{
				"schema_version": 1, "operation_key": validWire.OperationKey,
				"request_digest": validWire.RequestDigest, "created_at": validTime,
			})
		}},
		{"unsupported-version", func(t *testing.T, operations string) {
			wire := validWire
			wire.SchemaVersion = 2
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"padded-operation-key", func(t *testing.T, operations string) {
			wire := validWire
			wire.OperationKey += "="
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"locator-reuses-key", func(t *testing.T, operations string) {
			writePendingWire(t, operations, 0x41, validWire, ownerRegularFileMode)
		}},
		{"invalid-request-digest", func(t *testing.T, operations string) {
			wire := validWire
			wire.RequestDigest = "sha256:" + strings.Repeat("A", 64)
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"zero-request-digest", func(t *testing.T, operations string) {
			wire := validWire
			wire.RequestDigest = "sha256:" + strings.Repeat("0", 64)
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"invalid-context-digest", func(t *testing.T, operations string) {
			wire := validWire
			wire.ContextFileDigest = stringPointer("sha256:bad")
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"zero-context-digest", func(t *testing.T, operations string) {
			wire := validWire
			wire.ContextFileDigest = stringPointer("sha256:" + strings.Repeat("0", 64))
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"non-utc-time", func(t *testing.T, operations string) {
			wire := validWire
			wire.CreatedAt = "2026-07-16T03:04:05.000000006+00:00"
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"redundant-time-fraction", func(t *testing.T, operations string) {
			wire := validWire
			wire.CreatedAt = "2026-07-16T03:04:05.000Z"
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"invalid-time", func(t *testing.T, operations string) {
			wire := validWire
			wire.CreatedAt = "not-a-time"
			writePendingWire(t, operations, 0x42, wire, ownerRegularFileMode)
		}},
		{"noncanonical-json", func(t *testing.T, operations string) {
			writePendingRaw(t, operations, 0x42, append(validRaw, '\n'), ownerRegularFileMode)
		}},
		{"wrong-mode", func(t *testing.T, operations string) {
			writePendingRaw(t, operations, 0x42, validRaw, 0o644)
		}},
		{"invalid-locator", func(t *testing.T, operations string) {
			mustWrite(t, filepath.Join(operations, "not-base64.pending"), validRaw, ownerRegularFileMode)
		}},
		{"symlink", func(t *testing.T, operations string) {
			target := filepath.Join(operations, "target")
			mustWrite(t, target, validRaw, ownerRegularFileMode)
			path := pendingFixturePath(operations, 0x42)
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{"device-shape", func(t *testing.T, operations string) {
			if err := os.Mkdir(pendingFixturePath(operations, 0x42), ownerRegularFileMode); err != nil {
				t.Fatal(err)
			}
		}},
		{"oversized", func(t *testing.T, operations string) {
			writePendingRaw(t, operations, 0x42, bytes.Repeat([]byte{'x'}, maxPendingJournalBytes+1), ownerRegularFileMode)
		}},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nodeState := newClientNodeState(t)
			store, err := NewPendingJournalStore(nodeState)
			if err != nil {
				t.Fatal(err)
			}
			test.build(t, filepath.Join(nodeState, "operations"))
			if _, _, err := store.FindOrCreate(model.Sum([]byte("unrelated request")), &validContext); !errors.Is(err, ErrUnsafeClientState) {
				t.Fatalf("unsafe pending journal error = %v", err)
			}
		})
	}
}

func TestPendingJournalRemovalRequiresExactTerminalIdentity(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
		Random: bytes.NewReader(append(repeatedOpaqueBytes(0x51), repeatedOpaqueBytes(0x52)...)),
		Clock:  func() time.Time { return time.Date(2026, 7, 16, 4, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, _, err := store.FindOrCreate(model.Sum([]byte("terminal identity")), nil)
	if err != nil {
		t.Fatal(err)
	}
	moved := journal.Path() + ".original"
	if err := os.Rename(journal.Path(), moved); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(moved)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, journal.Path(), raw, ownerRegularFileMode)
	if err := store.RemoveTerminal(journal); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("replacement removal error = %v", err)
	}
	if current, err := os.ReadFile(journal.Path()); err != nil || !bytes.Equal(current, raw) {
		t.Fatalf("replacement journal was removed = %s, %v", current, err)
	}
	mustRemove(t, journal.Path())
	if err := os.Rename(moved, journal.Path()); err != nil {
		t.Fatal(err)
	}
	tamperedExpected := journal
	tamperedExpected.requestDigest = model.Sum([]byte("another operation"))
	if err := store.RemoveTerminal(tamperedExpected); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("mismatched terminal identity error = %v", err)
	}
	if _, err := os.Lstat(journal.Path()); err != nil {
		t.Fatalf("mismatched removal deleted journal: %v", err)
	}
	if err := store.RemoveTerminal(journal); err != nil {
		t.Fatalf("exact terminal removal = %v", err)
	}
}

func TestPendingJournalContextAndInputAreExactReuseKeys(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	entropy := make([]byte, 0, 6*opaqueSecretBytes)
	for fill := byte(1); fill <= 6; fill++ {
		entropy = append(entropy, repeatedOpaqueBytes(fill)...)
	}
	store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
		Random: bytes.NewReader(entropy),
		Clock:  func() time.Time { return time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	requestA := model.Sum([]byte("request-a"))
	requestB := model.Sum([]byte("request-b"))
	contextA := model.Sum([]byte("context-a"))
	contextB := model.Sum([]byte("context-b"))
	a, _, err := store.FindOrCreate(requestA, &contextA)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := store.FindOrCreate(requestA, &contextB)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := store.FindOrCreate(requestB, &contextA)
	if err != nil {
		t.Fatal(err)
	}
	if a.Path() == b.Path() || a.Path() == c.Path() || b.Path() == c.Path() {
		t.Fatal("different request/context identities shared a pending journal")
	}
	replay, reused, err := store.FindOrCreate(requestA, &contextA)
	if err != nil || !reused || replay.Path() != a.Path() {
		t.Fatalf("exact reuse = %#v, %v, %v", replay, reused, err)
	}
}

func TestPendingJournalEntropyAndClockFailClosed(t *testing.T) {
	t.Parallel()
	t.Run("independent-locator", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		same := repeatedOpaqueBytes(0x61)
		key, locator := repeatedOpaqueBytes(0x62), repeatedOpaqueBytes(0x63)
		entropy := append(append(append(append([]byte(nil), same...), same...), key...), locator...)
		store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
			Random: bytes.NewReader(entropy),
			Clock:  func() time.Time { return time.Date(2026, 7, 16, 6, 0, 0, 0, time.UTC) },
		})
		if err != nil {
			t.Fatal(err)
		}
		journal, _, err := store.FindOrCreate(model.Sum([]byte("independent")), nil)
		if err != nil {
			t.Fatal(err)
		}
		if journal.OperationKeyHeader() != base64.RawURLEncoding.EncodeToString(key) ||
			filepath.Base(journal.Path()) != base64.RawURLEncoding.EncodeToString(locator)+pendingJournalSuffix {
			t.Fatalf("independent retry identity = %q, %q", journal.OperationKeyHeader(), journal.Path())
		}
	})
	t.Run("entropy-failure", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
			Random: errorReader{},
			Clock:  func() time.Time { return time.Date(2026, 7, 16, 6, 1, 0, 0, time.UTC) },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.FindOrCreate(model.Sum([]byte("entropy")), nil); err == nil {
			t.Fatal("entropy failure was accepted")
		}
	})
	t.Run("zero-clock", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
			Random: bytes.NewReader(append(repeatedOpaqueBytes(0x71), repeatedOpaqueBytes(0x72)...)),
			Clock:  func() time.Time { return time.Time{} },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.FindOrCreate(model.Sum([]byte("zero clock")), nil); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("zero clock error = %v", err)
		}
	})
}

func TestPendingJournalRejectsUnsafeOperationsDirectoryAndDuplicates(t *testing.T) {
	t.Parallel()
	t.Run("mode", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		store, err := NewPendingJournalStore(nodeState)
		if err != nil {
			t.Fatal(err)
		}
		mustChmod(t, filepath.Join(nodeState, "operations"), 0o755)
		if _, _, err := store.FindOrCreate(model.Sum([]byte("request")), nil); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("unsafe operations mode error = %v", err)
		}
	})
	t.Run("node-state-mode", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		store, err := NewPendingJournalStore(nodeState)
		if err != nil {
			t.Fatal(err)
		}
		mustChmod(t, nodeState, 0o755)
		if _, _, err := store.FindOrCreate(model.Sum([]byte("request")), nil); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("unsafe Node state mode error = %v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		target := filepath.Join(nodeState, "target")
		if err := os.Mkdir(target, ownerDirectoryMode); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(nodeState, "operations")); err != nil {
			t.Fatal(err)
		}
		if _, err := NewPendingJournalStore(nodeState); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("symlink operations directory error = %v", err)
		}
	})
	t.Run("duplicate-input", func(t *testing.T) {
		nodeState := newClientNodeState(t)
		store, err := NewPendingJournalStore(nodeState, PendingJournalOptions{
			Random: bytes.NewReader(append(repeatedOpaqueBytes(0x11), repeatedOpaqueBytes(0x12)...)),
			Clock:  func() time.Time { return time.Date(2026, 7, 16, 7, 0, 0, 0, time.UTC) },
		})
		if err != nil {
			t.Fatal(err)
		}
		request := model.Sum([]byte("duplicate"))
		first, _, err := store.FindOrCreate(request, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(first.Path())
		if err != nil {
			t.Fatal(err)
		}
		writePendingRaw(t, filepath.Join(nodeState, "operations"), 0x13, raw, ownerRegularFileMode)
		if _, _, err := store.FindOrCreate(request, nil); !errors.Is(err, ErrUnsafeClientState) {
			t.Fatalf("duplicate pending input error = %v", err)
		}
	})
}

func writePendingWire(t *testing.T, operations string, locatorFill byte, wire pendingJournalWire,
	mode os.FileMode,
) {
	t.Helper()
	raw, err := model.CanonicalMarshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	writePendingRaw(t, operations, locatorFill, raw, mode)
}

func writePendingMap(t *testing.T, operations string, locatorFill byte, wire map[string]any) {
	t.Helper()
	raw, err := model.CanonicalMarshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	writePendingRaw(t, operations, locatorFill, raw, ownerRegularFileMode)
}

func writePendingRaw(t *testing.T, operations string, locatorFill byte, raw []byte, mode os.FileMode) {
	t.Helper()
	mustWrite(t, pendingFixturePath(operations, locatorFill), raw, mode)
}

func pendingFixturePath(operations string, locatorFill byte) string {
	return filepath.Join(operations,
		base64.RawURLEncoding.EncodeToString(repeatedOpaqueBytes(locatorFill))+pendingJournalSuffix)
}

func repeatedOpaqueBytes(fill byte) []byte {
	raw := make([]byte, opaqueSecretBytes)
	for index := range raw {
		raw[index] = fill
	}
	return raw
}

func stringPointer(value string) *string { return &value }

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("injected entropy failure") }
