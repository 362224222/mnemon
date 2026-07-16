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
	pendingJournalSuffix           = ".pending"
	terminalJournalSuffix          = ".terminal"
	presentedJournalSuffix         = ".presented"
	maxPendingJournalBytes         = 1024
	maxEntropyAttempts             = 32
	maxPresentedJournalTombstones  = 4096
	presentedJournalRetentionGrace = 24 * time.Hour
)

type journalState uint8

const (
	journalStateInvalid journalState = iota
	journalStatePending
	journalStateTerminal
	journalStatePresented
)

func (state journalState) suffix() string {
	switch state {
	case journalStatePending:
		return pendingJournalSuffix
	case journalStateTerminal:
		return terminalJournalSuffix
	case journalStatePresented:
		return presentedJournalSuffix
	default:
		return ""
	}
}

func (state journalState) valid() bool { return state.suffix() != "" }

type PendingJournalOptions struct {
	Random io.Reader
	Clock  func() time.Time

	// The retention controls are deliberately private test seams. Production
	// always retains presented evidence for the conservative defaults above.
	retentionClock     func() time.Time
	presentedRetention time.Duration
	maxPresented       int
}

// PendingJournalStore owns the model-invisible response-loss journal beneath
// one Node state directory. A store contains no canonical domain state.
type PendingJournalStore struct {
	nodeState          string
	operationsDir      string
	ownerUID           uint32
	random             io.Reader
	clock              func() time.Time
	retentionClock     func() time.Time
	presentedRetention time.Duration
	maxPresented       int
}

// PendingJournal is one verified operation replay handle. The operation key
// is deliberately exposed only as a request-header value.
type PendingJournal struct {
	path              string
	locator           [opaqueSecretBytes]byte
	state             journalState
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
	if options.retentionClock == nil {
		options.retentionClock = time.Now
	}
	if options.presentedRetention < 0 || options.maxPresented < 0 {
		return nil, errors.New("local API: pending journal retention options are invalid")
	}
	if options.presentedRetention == 0 {
		options.presentedRetention = presentedJournalRetentionGrace
	}
	if options.maxPresented == 0 {
		options.maxPresented = maxPresentedJournalTombstones
	}
	operationsDir, ownerUID, err := ensureOwnerSubdirectory(nodeState, "operations")
	if err != nil {
		return nil, err
	}
	return &PendingJournalStore{
		nodeState:          nodeState,
		operationsDir:      operationsDir,
		ownerUID:           ownerUID,
		random:             options.Random,
		clock:              options.Clock,
		retentionClock:     options.retentionClock,
		presentedRetention: options.presentedRetention,
		maxPresented:       options.maxPresented,
	}, nil
}

// FindOrCreate returns the unique replayable operation for an exact canonical
// request and optional context-file digest. Pending and terminal journals keep
// the same operation key across transport or presentation loss. Presented
// journals are bounded tombstones: they are ignored as replay candidates but
// retained long enough for an already-running client to recover their inode.
func (s *PendingJournalStore) FindOrCreate(requestDigest model.Digest,
	contextFileDigest *model.Digest,
) (journal PendingJournal, reused bool, err error) {
	if s == nil || s.random == nil || s.clock == nil || s.retentionClock == nil ||
		s.presentedRetention <= 0 || s.maxPresented <= 0 {
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
		// Tombstones collected in this critical section still exclude their old
		// operation key and locator from the identity drawn immediately after GC.
		// This turns faulty/repeating entropy into a retry instead of key reuse.
		identityExclusions := append([]PendingJournal(nil), journals...)
		presentedCount := countJournalState(journals, journalStatePresented)
		var createdAt time.Time
		var createdAtWire string
		if presentedCount > s.maxPresented {
			createdAt, createdAtWire, err = canonicalJournalTime(s.clock())
			if err != nil {
				return err
			}
			journals, err = s.collectPresentedLocked(journals, createdAt)
			if err != nil {
				return err
			}
			if countJournalState(journals, journalStatePresented) > s.maxPresented {
				return unsafeClientState("presented journal retention bound is exceeded")
			}
		}

		matchIndex := -1
		for index := range journals {
			candidate := journals[index]
			if candidate.state == journalStatePresented || candidate.requestDigest != requestDigest ||
				!sameContextDigest(candidate, contextDigest, hasContext) {
				continue
			}
			if matchIndex >= 0 {
				return unsafeClientState("duplicate replayable journals exist for one operation input")
			}
			matchIndex = index
		}
		if matchIndex >= 0 {
			journal, reused = journals[matchIndex], true
			return nil
		}

		if createdAt.IsZero() {
			createdAt, createdAtWire, err = canonicalJournalTime(s.clock())
			if err != nil {
				return err
			}
		}
		journals, err = s.collectPresentedLocked(journals, createdAt)
		if err != nil {
			return err
		}
		if countJournalState(journals, journalStatePresented) >= s.maxPresented {
			return unsafeClientState("presented journal retention bound has no safe capacity")
		}
		key, locator, path, err := s.drawIndependentIdentityLocked(identityExclusions)
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
			created.state != journalStatePending ||
			subtle.ConstantTimeCompare(created.locator[:], locator[:]) != 1 ||
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

// MarkTerminal records that mnemond returned a validated terminal accepted or
// rejected receipt. Repeating the transition with the returned terminal
// handle is idempotent. A retry after rename but before the first call returned
// can also recover the exact transitioned inode.
func (s *PendingJournalStore) MarkTerminal(expected PendingJournal) (PendingJournal, error) {
	return s.transitionJournal(expected, journalStatePending, journalStateTerminal)
}

// MarkPresented records that the validated terminal receipt was successfully
// written to stdout. Presented journals are no longer replay handles, but stay
// as bounded tombstones so concurrent holders of older handles can recover the
// exact inode while a new identical operation receives a fresh key.
func (s *PendingJournalStore) MarkPresented(expected PendingJournal) (PendingJournal, error) {
	return s.transitionJournal(expected, journalStateTerminal, journalStatePresented)
}

// RemoveTerminal removes only the exact terminal or presented journal inode
// and operation identity. A pending replay handle is never removable through
// this terminal acknowledgement path.
func (s *PendingJournalStore) RemoveTerminal(expected PendingJournal) error {
	if s == nil {
		return errors.New("local API: pending journal store is unavailable")
	}
	if err := s.validateJournalHandle(expected); err != nil {
		return err
	}
	if expected.state != journalStateTerminal && expected.state != journalStatePresented {
		return unsafeClientState("pending journal is not terminal or presented")
	}
	if err := s.validateLayout(); err != nil {
		return err
	}
	return withOwnerDirectoryLock(s.operationsDir, s.ownerUID, func() error {
		states, err := s.readLocatorStatesLocked(expected.locator)
		if err != nil {
			return err
		}
		if len(states) == 0 {
			return unsafeClientState("terminal journal removal has no inode evidence")
		}
		if len(states) != 1 {
			return unsafeClientState("terminal journal removal has conflicting state paths")
		}
		current := states[0]
		if current.state < expected.state || current.state == journalStatePending ||
			!sameJournalTransition(expected, current, current.state) {
			return unsafeClientState("terminal journal removal identity changed")
		}
		return s.removeJournalLocked(current)
	})
}

func (s *PendingJournalStore) transitionJournal(expected PendingJournal,
	from, to journalState,
) (journal PendingJournal, err error) {
	if s == nil || s.retentionClock == nil {
		return PendingJournal{}, errors.New("local API: pending journal store is unavailable")
	}
	if !from.valid() || !to.valid() || from == to {
		return PendingJournal{}, unsafeClientState("pending journal transition is invalid")
	}
	if err := s.validateJournalHandle(expected); err != nil {
		return PendingJournal{}, err
	}
	if expected.state != from && expected.state != to {
		return PendingJournal{}, unsafeClientState("pending journal is in the wrong transition state")
	}
	if err := s.validateLayout(); err != nil {
		return PendingJournal{}, err
	}
	err = withOwnerDirectoryLock(s.operationsDir, s.ownerUID, func() error {
		states, readErr := s.readLocatorStatesLocked(expected.locator)
		if readErr != nil {
			return readErr
		}
		if len(states) == 0 {
			return unsafeClientState("pending journal transition has no recoverable inode evidence")
		}
		if len(states) != 1 {
			return unsafeClientState("pending journal transition has conflicting state paths")
		}
		current := states[0]
		if !sameJournalTransition(expected, current, current.state) {
			return unsafeClientState("pending journal transition identity changed")
		}

		if current.state != from {
			if current.state < to || current.state < expected.state {
				return unsafeClientState("pending journal transition regressed or has the wrong state")
			}
			if syncErr := syncOwnerDirectory(s.operationsDir); syncErr != nil {
				return fmt.Errorf("persist pending journal transition: %w", syncErr)
			}
			verified, verifyErr := s.confirmOnlyLocatorStateLocked(current)
			if verifyErr != nil {
				return verifyErr
			}
			journal = verified
			return nil
		}

		if expected.state != from {
			return unsafeClientState("pending journal transition regressed behind its handle")
		}
		if to == journalStatePresented {
			presentationTime, _, clockErr := canonicalJournalTime(s.retentionClock())
			if clockErr != nil {
				return clockErr
			}
			current, readErr = s.stampPresentedJournalLocked(current, presentationTime)
			if readErr != nil {
				return readErr
			}
		}

		destination := journalPath(s.operationsDir, expected.locator, to)
		if _, destinationErr := os.Lstat(destination); destinationErr == nil {
			return unsafeClientState("pending journal transition destination already exists")
		} else if !errors.Is(destinationErr, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect pending journal transition destination", ErrUnsafeClientState)
		}
		sourceInfo, statErr := os.Lstat(current.path)
		if statErr != nil || !os.SameFile(current.identity, sourceInfo) ||
			validateOwnerRegularFile(sourceInfo, s.ownerUID) != nil {
			return unsafeClientState("pending journal changed before transition")
		}
		if renameErr := os.Rename(current.path, destination); renameErr != nil {
			return fmt.Errorf("transition pending journal: %w", renameErr)
		}
		transitioned, readErr := readPendingJournalFile(s.operationsDir, destination, s.ownerUID)
		if readErr != nil || !sameJournalTransition(current, transitioned, to) {
			return unsafeClientState("pending journal changed during transition")
		}
		if _, sourceErr := os.Lstat(current.path); !errors.Is(sourceErr, os.ErrNotExist) {
			return unsafeClientState("pending journal source remained after transition")
		}
		if syncErr := syncOwnerDirectory(s.operationsDir); syncErr != nil {
			return fmt.Errorf("persist pending journal transition: %w", syncErr)
		}
		verified, verifyErr := s.confirmOnlyLocatorStateLocked(transitioned)
		if verifyErr != nil {
			return verifyErr
		}
		journal = verified
		return nil
	})
	if err != nil {
		return PendingJournal{}, err
	}
	return journal, nil
}

func (s *PendingJournalStore) readExactJournalLocked(expected PendingJournal) (PendingJournal, error) {
	current, err := readPendingJournalFile(s.operationsDir, expected.path, s.ownerUID)
	if err != nil {
		return PendingJournal{}, err
	}
	if !sameJournalExact(current, expected) {
		return PendingJournal{}, unsafeClientState("pending journal identity changed")
	}
	return current, nil
}

func (s *PendingJournalStore) readLocatorStatesLocked(
	locator [opaqueSecretBytes]byte,
) ([]PendingJournal, error) {
	states := make([]PendingJournal, 0, 1)
	for _, state := range []journalState{journalStatePending, journalStateTerminal, journalStatePresented} {
		path := journalPath(s.operationsDir, locator, state)
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("%w: inspect pending journal state path", ErrUnsafeClientState)
		}
		journal, err := readPendingJournalFile(s.operationsDir, path, s.ownerUID)
		if err != nil {
			return nil, err
		}
		states = append(states, journal)
	}
	return states, nil
}

func (s *PendingJournalStore) confirmOnlyLocatorStateLocked(
	expected PendingJournal,
) (PendingJournal, error) {
	states, err := s.readLocatorStatesLocked(expected.locator)
	if err != nil {
		return PendingJournal{}, err
	}
	if len(states) != 1 || !sameJournalExact(expected, states[0]) {
		return PendingJournal{}, unsafeClientState("pending journal changed while confirming state")
	}
	return states[0], nil
}

func (s *PendingJournalStore) stampPresentedJournalLocked(
	expected PendingJournal, presentedAt time.Time,
) (PendingJournal, error) {
	if expected.state != journalStateTerminal {
		return PendingJournal{}, unsafeClientState("only a terminal journal can be stamped for presentation")
	}
	current, err := s.readExactJournalLocked(expected)
	if err != nil {
		return PendingJournal{}, err
	}
	if err := os.Chtimes(current.path, presentedAt, presentedAt); err != nil {
		return PendingJournal{}, fmt.Errorf("stamp presented journal retention: %w", err)
	}
	file, err := os.Open(current.path)
	if err != nil {
		return PendingJournal{}, fmt.Errorf("%w: open stamped presented journal", ErrUnsafeClientState)
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(current.identity, opened) ||
		validateOwnerRegularFile(opened, s.ownerUID) != nil {
		_ = file.Close()
		return PendingJournal{}, unsafeClientState("presented journal changed while stamping retention")
	}
	if syncErr := file.Sync(); syncErr != nil {
		_ = file.Close()
		return PendingJournal{}, fmt.Errorf("persist presented journal retention stamp: %w", syncErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return PendingJournal{}, fmt.Errorf("close presented journal retention stamp: %w", closeErr)
	}
	stamped, err := readPendingJournalFile(s.operationsDir, current.path, s.ownerUID)
	if err != nil || !sameJournalTransition(current, stamped, journalStateTerminal) {
		return PendingJournal{}, unsafeClientState("presented journal changed after retention stamp")
	}
	return stamped, nil
}

func (s *PendingJournalStore) removeJournalLocked(expected PendingJournal) error {
	if err := s.validateJournalHandle(expected); err != nil {
		return err
	}
	if expected.state != journalStateTerminal && expected.state != journalStatePresented {
		return unsafeClientState("pending journal is not terminal or presented")
	}
	current, err := s.readExactJournalLocked(expected)
	if err != nil {
		return err
	}
	before, err := os.Lstat(expected.path)
	if err != nil || !os.SameFile(current.identity, before) ||
		validateOwnerRegularFile(before, s.ownerUID) != nil {
		return unsafeClientState("pending journal changed before removal")
	}
	if err := os.Remove(expected.path); err != nil {
		return fmt.Errorf("remove terminal pending journal: %w", err)
	}
	if _, err := os.Lstat(expected.path); !errors.Is(err, os.ErrNotExist) {
		return unsafeClientState("pending journal path remained after removal")
	}
	if err := syncOwnerDirectory(s.operationsDir); err != nil {
		return fmt.Errorf("persist terminal pending removal: %w", err)
	}
	return nil
}

func (s *PendingJournalStore) collectPresentedLocked(
	journals []PendingJournal, now time.Time,
) ([]PendingJournal, error) {
	retained := make([]PendingJournal, 0, len(journals))
	for index := range journals {
		journal := journals[index]
		if journal.state != journalStatePresented ||
			!retentionElapsed(now, journal.createdAt, s.presentedRetention) ||
			!retentionElapsed(now, journal.identity.ModTime(), s.presentedRetention) {
			retained = append(retained, journal)
			continue
		}
		if err := s.removeJournalLocked(journal); err != nil {
			return nil, err
		}
	}
	return retained, nil
}

func retentionElapsed(now, since time.Time, grace time.Duration) bool {
	if now.IsZero() || since.IsZero() || grace <= 0 || now.Before(since) {
		return false
	}
	return now.Sub(since) >= grace
}

func countJournalState(journals []PendingJournal, state journalState) int {
	count := 0
	for index := range journals {
		if journals[index].state == state {
			count++
		}
	}
	return count
}

func (s *PendingJournalStore) validateJournalHandle(journal PendingJournal) error {
	if journal.identity == nil || journal.fileDigest.IsZero() || journal.requestDigest.IsZero() ||
		journal.createdAt.IsZero() || !journal.state.valid() {
		return unsafeClientState("pending journal handle lacks a verified identity")
	}
	if s == nil || s.operationsDir == "" {
		return unsafeClientState("pending journal store identity is unavailable")
	}
	locator, state, err := parsePendingJournalStatePath(s.operationsDir, journal.path)
	if err != nil || state != journal.state ||
		subtle.ConstantTimeCompare(locator[:], journal.locator[:]) != 1 {
		return unsafeClientState("pending journal handle path differs from its identity")
	}
	return nil
}

func sameJournalExact(left, right PendingJournal) bool {
	return left.path == right.path && left.state == right.state &&
		sameJournalTransition(left, right, left.state)
}

func sameJournalTransition(expected, actual PendingJournal, state journalState) bool {
	return actual.state == state && expected.identity != nil && actual.identity != nil &&
		os.SameFile(expected.identity, actual.identity) && expected.fileDigest == actual.fileDigest &&
		expected.requestDigest == actual.requestDigest && expected.createdAt == actual.createdAt &&
		sameContextDigest(actual, expected.contextFileDigest, expected.hasContext) &&
		subtle.ConstantTimeCompare(expected.locator[:], actual.locator[:]) == 1 &&
		subtle.ConstantTimeCompare(expected.operationKey[:], actual.operationKey[:]) == 1
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
		_, recognized, classifyErr := classifyJournalFilename(entry.Name())
		if classifyErr != nil {
			return nil, classifyErr
		}
		if !recognized {
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
			if subtle.ConstantTimeCompare(result[index].locator[:], journal.locator[:]) == 1 {
				return nil, unsafeClientState("multiple pending journal states reuse one locator")
			}
		}
		result = append(result, journal)
	}
	return result, nil
}

func readPendingJournalFile(operationsDir, path string, ownerUID uint32) (PendingJournal, error) {
	locator, state, err := parsePendingJournalStatePath(operationsDir, path)
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
		locator:           locator,
		state:             state,
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
		duplicateLocator := false
		for index := range existing {
			if subtle.ConstantTimeCompare(existing[index].locator[:], locator[:]) == 1 {
				duplicateLocator = true
				break
			}
		}
		if duplicateLocator {
			continue
		}
		available := true
		for _, state := range []journalState{journalStatePending, journalStateTerminal, journalStatePresented} {
			candidate := journalPath(s.operationsDir, locator, state)
			if _, statErr := os.Lstat(candidate); statErr == nil {
				available = false
				break
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return key, locator, "", fmt.Errorf("%w: inspect pending journal destination", ErrUnsafeClientState)
			}
		}
		if available {
			path = journalPath(s.operationsDir, locator, journalStatePending)
			return key, locator, path, nil
		}
	}
	return key, locator, "", errors.New("local API: cannot allocate an independent pending operation identity")
}

func parsePendingJournalPath(operationsDir, path string) ([opaqueSecretBytes]byte, error) {
	locator, state, err := parsePendingJournalStatePath(operationsDir, path)
	if err == nil && state == journalStatePresented {
		return [opaqueSecretBytes]byte{},
			unsafeClientState("presented journal is not an operation replay handle")
	}
	return locator, err
}

func parsePendingJournalStatePath(operationsDir, path string) ([opaqueSecretBytes]byte, journalState, error) {
	var locator [opaqueSecretBytes]byte
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != operationsDir {
		return locator, journalStateInvalid,
			unsafeClientState("pending journal path escaped its operations directory")
	}
	base := filepath.Base(path)
	state, recognized, err := classifyJournalFilename(base)
	if err != nil {
		return locator, journalStateInvalid, err
	}
	if !recognized {
		return locator, journalStateInvalid, unsafeClientState("pending journal path has the wrong suffix")
	}
	encoded := strings.TrimSuffix(base, state.suffix())
	raw, err := decodeOpaqueSecret(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != encoded {
		clear(raw)
		return locator, journalStateInvalid,
			unsafeClientState("pending journal locator is not canonical")
	}
	copy(locator[:], raw)
	clear(raw)
	return locator, state, nil
}

func classifyJournalFilename(base string) (journalState, bool, error) {
	states := []journalState{journalStatePending, journalStateTerminal, journalStatePresented}
	for _, state := range states {
		if strings.HasSuffix(base, state.suffix()) {
			return state, true, nil
		}
	}
	for _, state := range states {
		if len(base) >= len(state.suffix()) &&
			strings.EqualFold(base[len(base)-len(state.suffix()):], state.suffix()) {
			return journalStateInvalid, false,
				unsafeClientState("pending journal state suffix is not canonical")
		}
	}
	separator := strings.IndexByte(base, '.')
	if separator != 0 {
		encoded := base
		if separator > 0 {
			encoded = base[:separator]
		}
		raw, err := decodeOpaqueSecret(encoded)
		canonical := err == nil && base64.RawURLEncoding.EncodeToString(raw) == encoded
		clear(raw)
		if canonical {
			return journalStateInvalid, false,
				unsafeClientState("pending journal path has an unknown state suffix")
		}
	}
	return journalStateInvalid, false, nil
}

func journalPath(operationsDir string, locator [opaqueSecretBytes]byte, state journalState) string {
	return filepath.Join(operationsDir,
		base64.RawURLEncoding.EncodeToString(locator[:])+state.suffix())
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
