package localapi

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	pendingJournalSuffix   = ".pending"
	maxPendingJournalBytes = 1024
	maxEntropyAttempts     = 32
)

type PendingJournalOptions struct {
	Random io.Reader
	Clock  func() time.Time
}

// PendingJournalStore owns the model-invisible response-loss journal beneath
// one Node state directory. A store contains no canonical domain state.
type PendingJournalStore struct {
	nodeState     string
	operationsDir string
	ownerUID      uint32
	random        io.Reader
	clock         func() time.Time
}

// PendingJournal is one verified operation replay handle. The operation key
// is deliberately exposed only as a request-header value.
type PendingJournal struct {
	path              string
	operationKey      [opaqueSecretBytes]byte
	requestDigest     model.Digest
	contextFileDigest model.Digest
	hasContext        bool
	createdAt         time.Time
	fileDigest        model.Digest
	identity          os.FileInfo
}

func (j PendingJournal) Path() string                { return j.path }
func (j PendingJournal) RequestDigest() model.Digest { return j.requestDigest }
func (j PendingJournal) CreatedAt() time.Time        { return j.createdAt }
func (j PendingJournal) OperationKeyHeader() string {
	return base64.RawURLEncoding.EncodeToString(j.operationKey[:])
}
func (j PendingJournal) OperationKeyHash() model.Digest { return model.Sum(j.operationKey[:]) }
func (j PendingJournal) ContextFileDigest() (model.Digest, bool) {
	return j.contextFileDigest, j.hasContext
}

// NewPendingJournalStore creates or verifies the owner-only operations
// directory. Supplying options is only needed by deterministic tests.
func NewPendingJournalStore(nodeState string, supplied ...PendingJournalOptions) (*PendingJournalStore, error) {
	if len(supplied) > 1 {
		return nil, errors.New("local API: at most one pending journal option set is allowed")
	}
	options := PendingJournalOptions{}
	if len(supplied) == 1 {
		options = supplied[0]
	}
	if options.Random == nil {
		options.Random = cryptorand.Reader
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	operationsDir, ownerUID, err := ensureOwnerSubdirectory(nodeState, "operations")
	if err != nil {
		return nil, err
	}
	return &PendingJournalStore{
		nodeState:     nodeState,
		operationsDir: operationsDir,
		ownerUID:      ownerUID,
		random:        options.Random,
		clock:         options.Clock,
	}, nil
}

// FindOrCreate returns the unique pending operation for an exact canonical
// request and optional context-file digest. reused is true after response loss
// or another retry has already established the same pending operation.
func (s *PendingJournalStore) FindOrCreate(requestDigest model.Digest,
	contextFileDigest *model.Digest,
) (journal PendingJournal, reused bool, err error) {
	if s == nil || s.random == nil || s.clock == nil {
		return PendingJournal{}, false, errors.New("local API: pending journal store is unavailable")
	}
	if requestDigest.IsZero() {
		return PendingJournal{}, false, unsafeClientState("pending request digest is zero")
	}
	contextDigest, hasContext, err := validateOptionalContextDigest(contextFileDigest)
	if err != nil {
		return PendingJournal{}, false, err
	}
	if err := s.validateLayout(); err != nil {
		return PendingJournal{}, false, err
	}

	err = withOwnerDirectoryLock(s.operationsDir, s.ownerUID, func() error {
		journals, err := s.readAllLocked()
		if err != nil {
			return err
		}
		var match *PendingJournal
		for index := range journals {
			candidate := journals[index]
			if candidate.requestDigest == requestDigest && sameContextDigest(candidate, contextDigest, hasContext) {
				if match != nil {
					return unsafeClientState("duplicate pending journals exist for one operation input")
				}
				copy := candidate
				match = &copy
			}
		}
		if match != nil {
			journal, reused = *match, true
			return nil
		}

		createdAt, createdAtWire, err := canonicalJournalTime(s.clock())
		if err != nil {
			return err
		}
		key, locator, path, err := s.drawIndependentIdentityLocked(journals)
		if err != nil {
			return err
		}
		keyWire := base64.RawURLEncoding.EncodeToString(key[:])
		requestWire := requestDigest.String()
		var contextWire *string
		if hasContext {
			value := contextDigest.String()
			contextWire = &value
		}
		payload, err := model.CanonicalMarshal(pendingJournalWire{
			SchemaVersion:     SchemaVersion,
			OperationKey:      keyWire,
			RequestDigest:     requestWire,
			ContextFileDigest: contextWire,
			CreatedAt:         createdAtWire,
		})
		if err != nil || len(payload) > maxPendingJournalBytes {
			return unsafeClientState("pending journal cannot be encoded canonically")
		}
		if err := atomicWriteOwnerFile(s.operationsDir, path, payload, s.ownerUID); err != nil {
			return err
		}
		created, err := readPendingJournalFile(s.operationsDir, path, s.ownerUID)
		if err != nil {
			return err
		}
		if created.requestDigest != requestDigest || !sameContextDigest(created, contextDigest, hasContext) ||
			created.createdAt != createdAt || subtle.ConstantTimeCompare(created.operationKey[:], key[:]) != 1 ||
			base64.RawURLEncoding.EncodeToString(locator[:])+pendingJournalSuffix != filepath.Base(path) {
			return unsafeClientState("published pending journal changed identity")
		}
		journal, reused = created, false
		return nil
	})
	if err != nil {
		return PendingJournal{}, false, err
	}
	return journal, reused, nil
}

// RemoveTerminal removes only the exact journal inode and operation identity
// that produced a validated terminal accepted or rejected receipt.
func (s *PendingJournalStore) RemoveTerminal(expected PendingJournal) error {
	if s == nil {
		return errors.New("local API: pending journal store is unavailable")
	}
	if expected.identity == nil || expected.fileDigest.IsZero() || expected.requestDigest.IsZero() ||
		expected.createdAt.IsZero() {
		return unsafeClientState("pending journal removal lacks an expected identity")
	}
	if err := s.validateLayout(); err != nil {
		return err
	}
	if _, err := parsePendingJournalPath(s.operationsDir, expected.path); err != nil {
		return err
	}
	return withOwnerDirectoryLock(s.operationsDir, s.ownerUID, func() error {
		current, err := readPendingJournalFile(s.operationsDir, expected.path, s.ownerUID)
		if err != nil {
			return err
		}
		if !os.SameFile(current.identity, expected.identity) || current.fileDigest != expected.fileDigest ||
			current.requestDigest != expected.requestDigest || current.createdAt != expected.createdAt ||
			!sameContextDigest(current, expected.contextFileDigest, expected.hasContext) ||
			subtle.ConstantTimeCompare(current.operationKey[:], expected.operationKey[:]) != 1 {
			return unsafeClientState("pending journal identity changed before removal")
		}
		if err := os.Remove(expected.path); err != nil {
			return fmt.Errorf("remove terminal pending journal: %w", err)
		}
		if err := syncOwnerDirectory(s.operationsDir); err != nil {
			return fmt.Errorf("persist terminal pending removal: %w", err)
		}
		return nil
	})
}

func (s *PendingJournalStore) validateLayout() error {
	ownerUID, err := validateNodeStateDirectory(s.nodeState)
	if err != nil {
		return err
	}
	if ownerUID != s.ownerUID {
		return unsafeClientState("pending journal Node owner changed")
	}
	_, err = validateOwnerDirectoryPath(s.operationsDir, s.ownerUID)
	return err
}

type pendingJournalWire struct {
	SchemaVersion     int     `json:"schema_version"`
	OperationKey      string  `json:"operation_key"`
	RequestDigest     string  `json:"request_digest"`
	ContextFileDigest *string `json:"context_file_digest"`
	CreatedAt         string  `json:"created_at"`
}

func (s *PendingJournalStore) readAllLocked() ([]PendingJournal, error) {
	entries, err := os.ReadDir(s.operationsDir)
	if err != nil {
		return nil, fmt.Errorf("read pending journal directory: %w", err)
	}
	result := make([]PendingJournal, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), pendingJournalSuffix) {
			continue
		}
		path := filepath.Join(s.operationsDir, entry.Name())
		journal, err := readPendingJournalFile(s.operationsDir, path, s.ownerUID)
		if err != nil {
			return nil, err
		}
		for index := range result {
			if subtle.ConstantTimeCompare(result[index].operationKey[:], journal.operationKey[:]) == 1 {
				return nil, unsafeClientState("multiple pending journals reuse one operation key")
			}
		}
		result = append(result, journal)
	}
	return result, nil
}

func readPendingJournalFile(operationsDir, path string, ownerUID uint32) (PendingJournal, error) {
	locator, err := parsePendingJournalPath(operationsDir, path)
	if err != nil {
		return PendingJournal{}, err
	}
	raw, identity, err := readOwnerRegularFile(path, maxPendingJournalBytes, ownerUID)
	if err != nil {
		return PendingJournal{}, err
	}
	if len(raw) == 0 || raw[0] != '{' {
		return PendingJournal{}, unsafeClientState("pending journal is not a canonical JSON object")
	}
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil || !bytes.Equal(raw, canonical) {
		return PendingJournal{}, unsafeClientState("pending journal bytes are not canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire pendingJournalWire
	if err := decoder.Decode(&wire); err != nil {
		return PendingJournal{}, unsafeClientState("pending journal does not match schema v1")
	}
	if err := expectJSONEOF(decoder); err != nil {
		return PendingJournal{}, unsafeClientState("pending journal contains a trailing value")
	}
	closed, err := model.CanonicalMarshal(wire)
	if err != nil || !bytes.Equal(raw, closed) {
		return PendingJournal{}, unsafeClientState("pending journal omits or changes a schema v1 field")
	}
	if wire.SchemaVersion != SchemaVersion {
		return PendingJournal{}, unsafeClientState("pending journal has an unsupported schema version")
	}
	key, err := decodeOpaqueSecret(wire.OperationKey)
	if err != nil {
		return PendingJournal{}, unsafeClientState("pending journal operation key is not canonical")
	}
	defer clear(key)
	if subtle.ConstantTimeCompare(key, locator[:]) == 1 {
		return PendingJournal{}, unsafeClientState("pending journal locator reuses its operation key")
	}
	requestDigest, err := model.ParseDigest(wire.RequestDigest)
	if err != nil || requestDigest.IsZero() {
		return PendingJournal{}, unsafeClientState("pending journal request digest is invalid")
	}
	contextDigest, hasContext, err := parseOptionalContextDigest(wire.ContextFileDigest)
	if err != nil {
		return PendingJournal{}, err
	}
	createdAt, canonicalTime, err := canonicalJournalTimeFromWire(wire.CreatedAt)
	if err != nil || canonicalTime != wire.CreatedAt {
		return PendingJournal{}, unsafeClientState("pending journal creation time is not canonical")
	}
	result := PendingJournal{
		path:              path,
		requestDigest:     requestDigest,
		contextFileDigest: contextDigest,
		hasContext:        hasContext,
		createdAt:         createdAt,
		fileDigest:        model.Sum(raw),
		identity:          identity,
	}
	copy(result.operationKey[:], key)
	return result, nil
}

func (s *PendingJournalStore) drawIndependentIdentityLocked(existing []PendingJournal) (
	key [opaqueSecretBytes]byte, locator [opaqueSecretBytes]byte, path string, err error,
) {
	for attempt := 0; attempt < maxEntropyAttempts; attempt++ {
		key, err = drawOpaqueBytes(s.random)
		if err != nil {
			return key, locator, "", fmt.Errorf("generate operation key: %w", err)
		}
		duplicateKey := false
		for index := range existing {
			if subtle.ConstantTimeCompare(existing[index].operationKey[:], key[:]) == 1 {
				duplicateKey = true
				break
			}
		}
		if duplicateKey {
			continue
		}
		locator, err = drawOpaqueBytes(s.random)
		if err != nil {
			return key, locator, "", fmt.Errorf("generate pending locator: %w", err)
		}
		if subtle.ConstantTimeCompare(key[:], locator[:]) == 1 {
			continue
		}
		path = filepath.Join(s.operationsDir,
			base64.RawURLEncoding.EncodeToString(locator[:])+pendingJournalSuffix)
		if _, statErr := os.Lstat(path); errors.Is(statErr, os.ErrNotExist) {
			return key, locator, path, nil
		} else if statErr != nil {
			return key, locator, "", fmt.Errorf("%w: inspect pending journal destination", ErrUnsafeClientState)
		}
	}
	return key, locator, "", errors.New("local API: cannot allocate an independent pending operation identity")
}

func parsePendingJournalPath(operationsDir, path string) ([opaqueSecretBytes]byte, error) {
	var locator [opaqueSecretBytes]byte
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != operationsDir {
		return locator, unsafeClientState("pending journal path escaped its operations directory")
	}
	base := filepath.Base(path)
	if !strings.HasSuffix(base, pendingJournalSuffix) {
		return locator, unsafeClientState("pending journal path has the wrong suffix")
	}
	encoded := strings.TrimSuffix(base, pendingJournalSuffix)
	raw, err := decodeOpaqueSecret(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != encoded {
		clear(raw)
		return locator, unsafeClientState("pending journal locator is not canonical")
	}
	copy(locator[:], raw)
	clear(raw)
	return locator, nil
}

func validateOptionalContextDigest(value *model.Digest) (model.Digest, bool, error) {
	if value == nil {
		return model.Digest{}, false, nil
	}
	if value.IsZero() {
		return model.Digest{}, false, unsafeClientState("context-file digest is zero")
	}
	return *value, true, nil
}

func parseOptionalContextDigest(value *string) (model.Digest, bool, error) {
	if value == nil {
		return model.Digest{}, false, nil
	}
	digest, err := model.ParseDigest(*value)
	if err != nil || digest.IsZero() {
		return model.Digest{}, false, unsafeClientState("pending journal context-file digest is invalid")
	}
	return digest, true, nil
}

func sameContextDigest(journal PendingJournal, digest model.Digest, present bool) bool {
	return journal.hasContext == present && (!present || journal.contextFileDigest == digest)
}

func drawOpaqueBytes(random io.Reader) ([opaqueSecretBytes]byte, error) {
	var value [opaqueSecretBytes]byte
	_, err := io.ReadFull(random, value[:])
	return value, err
}

func canonicalJournalTime(value time.Time) (time.Time, string, error) {
	if value.IsZero() {
		return time.Time{}, "", unsafeClientState("pending journal clock returned zero")
	}
	canonical := value.Round(0).UTC()
	if !time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return time.Time{}, "", unsafeClientState("pending journal time is outside Unix nanoseconds")
	}
	wire := canonical.Format(time.RFC3339Nano)
	parsed, err := time.Parse(time.RFC3339Nano, wire)
	if err != nil || !parsed.Equal(canonical) {
		return time.Time{}, "", unsafeClientState("pending journal time cannot round-trip")
	}
	return canonical, wire, nil
}

func canonicalJournalTimeFromWire(wire string) (time.Time, string, error) {
	parsed, err := time.Parse(time.RFC3339Nano, wire)
	if err != nil {
		return time.Time{}, "", unsafeClientState("pending journal creation time is invalid")
	}
	return canonicalJournalTime(parsed)
}

func expectJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON input contains trailing data")
	}
	return nil
}
