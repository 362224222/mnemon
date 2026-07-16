package localapi

import (
	"crypto/subtle"
	"encoding/base64"
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
	// RunAttachmentEnv carries only an owner-only file reference inherited by
	// a mnemond-launched Runtime. The capability itself never enters argv or
	// the environment.
	RunAttachmentEnv = "MNEMON_HARNESS_RUN_ATTACHMENT"

	runAttachmentSuffix        = ".attach"
	runAttachmentStagePrefix   = ".mnemon-run-attachment-"
	runAttachmentStageSuffix   = ".tmp"
	runAttachmentFileBytes     = 43 + 1
	runAttachmentStageIDBytes  = 16
	maxRunAttachmentStageAge   = 10 * time.Minute
	maxRunAttachmentCandidates = 65 // one active claim plus one bounded reap batch
)

// StagedRunAttachment is the pre-transaction filesystem half of a wake
// preclaim. It has no Run identity until SQLite commits the selected Handling
// and AgentRun atomically.
type StagedRunAttachment struct {
	nodeState string
	path      string
	token     [opaqueSecretBytes]byte
	digest    model.Digest
	identity  os.FileInfo
}

// RunAttachment is a verified final <run-id>.attach capability. HeaderValue
// is deliberately the only secret accessor and must be used only for the
// Mnemon-Run-Attachment transport header or the post-consume context file.
type RunAttachment struct {
	path     string
	runID    model.RunID
	token    [opaqueSecretBytes]byte
	digest   model.Digest
	identity os.FileInfo
}

// RunAttachmentCandidate exposes only the filesystem identity needed for
// Store authorization. Raw capability bytes never cross this boundary.
type RunAttachmentCandidate struct {
	runID     model.RunID
	tokenHash model.Digest
}

type RunAttachmentCandidatePage struct {
	candidates []RunAttachmentCandidate
	more       bool
}

func (c RunAttachmentCandidate) RunID() model.RunID      { return c.runID }
func (c RunAttachmentCandidate) TokenHash() model.Digest { return c.tokenHash }
func (p RunAttachmentCandidatePage) More() bool          { return p.more }
func (p RunAttachmentCandidatePage) Candidates() []RunAttachmentCandidate {
	return append([]RunAttachmentCandidate(nil), p.candidates...)
}

func (f StagedRunAttachment) TokenHash() model.Digest { return model.Sum(f.token[:]) }
func (f RunAttachment) Path() string                  { return f.path }
func (f RunAttachment) RunID() model.RunID            { return f.runID }
func (f RunAttachment) Digest() model.Digest          { return f.digest }

func (f RunAttachment) HeaderValue() string {
	return base64.RawURLEncoding.EncodeToString(f.token[:])
}

// StageRunAttachment creates and fsyncs one owner-only temporary capability.
// The caller must do this before the Store preclaim transaction, then call
// Publish only after that transaction returns its server-owned Run ID.
func StageRunAttachment(nodeState string, random io.Reader) (StagedRunAttachment, error) {
	if random == nil {
		return StagedRunAttachment{}, unsafeClientState("Run attachment entropy is unavailable")
	}
	runsDir, ownerUID, err := ensureOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return StagedRunAttachment{}, err
	}
	token := make([]byte, opaqueSecretBytes)
	stageID := make([]byte, runAttachmentStageIDBytes)
	if _, err := io.ReadFull(random, token); err != nil {
		clear(token)
		return StagedRunAttachment{}, fmt.Errorf("generate Run attachment capability: %w", err)
	}
	defer clear(token)
	if _, err := io.ReadFull(random, stageID); err != nil {
		clear(stageID)
		return StagedRunAttachment{}, fmt.Errorf("generate Run attachment stage identity: %w", err)
	}
	stageName := runAttachmentStagePrefix + base64.RawURLEncoding.EncodeToString(stageID) +
		runAttachmentStageSuffix
	clear(stageID)
	path := filepath.Join(runsDir, stageName)
	encoded := base64.RawURLEncoding.EncodeToString(token)
	payload := []byte(encoded + "\n")
	if err := atomicWriteOwnerFile(runsDir, path, payload, ownerUID); err != nil {
		return StagedRunAttachment{}, err
	}
	created, err := readStagedRunAttachment(runsDir, path, ownerUID)
	if err != nil {
		return StagedRunAttachment{}, err
	}
	if subtle.ConstantTimeCompare(created.token[:], token) != 1 {
		return StagedRunAttachment{}, unsafeClientState("published Run attachment stage differs")
	}
	created.nodeState = nodeState
	return created, nil
}

// Publish atomically renames an exact staged capability to the canonical Run
// path. An existing destination is never replaced, even with equal bytes.
func (f StagedRunAttachment) Publish(runID model.RunID) (RunAttachment, error) {
	runsDir, ownerUID, err := requireOwnerSubdirectory(f.nodeState, "runs")
	if err != nil {
		return RunAttachment{}, err
	}
	finalPath, err := runAttachmentPathForRun(runsDir, runID)
	if err != nil {
		return RunAttachment{}, err
	}
	var result RunAttachment
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		current, err := readStagedRunAttachment(runsDir, f.path, ownerUID)
		if err != nil {
			return err
		}
		if f.identity == nil || !os.SameFile(current.identity, f.identity) || current.digest != f.digest ||
			subtle.ConstantTimeCompare(current.token[:], f.token[:]) != 1 {
			return unsafeClientState("Run attachment stage identity changed")
		}
		if _, err := os.Lstat(finalPath); err == nil {
			return unsafeClientState("Run attachment destination already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect Run attachment destination", ErrUnsafeClientState)
		}
		if err := os.Rename(f.path, finalPath); err != nil {
			return fmt.Errorf("publish Run attachment: %w", err)
		}
		if err := syncOwnerDirectory(runsDir); err != nil {
			return fmt.Errorf("persist Run attachment publication: %w", err)
		}
		published, err := readRunAttachmentFromDir(runsDir, finalPath, ownerUID)
		if err != nil {
			return err
		}
		if published.runID != runID || published.digest != f.digest ||
			subtle.ConstantTimeCompare(published.token[:], f.token[:]) != 1 {
			return unsafeClientState("published Run attachment differs from its stage")
		}
		result = published
		return nil
	})
	if err != nil {
		return RunAttachment{}, err
	}
	return result, nil
}

// Discard removes only the exact staged inode represented by f.
func (f StagedRunAttachment) Discard() error {
	runsDir, ownerUID, err := requireOwnerSubdirectory(f.nodeState, "runs")
	if err != nil {
		return err
	}
	return withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		if _, err := os.Lstat(f.path); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return fmt.Errorf("%w: inspect Run attachment stage", ErrUnsafeClientState)
		}
		current, err := readStagedRunAttachment(runsDir, f.path, ownerUID)
		if err != nil {
			return err
		}
		if f.identity == nil || !os.SameFile(current.identity, f.identity) || current.digest != f.digest ||
			subtle.ConstantTimeCompare(current.token[:], f.token[:]) != 1 {
			return unsafeClientState("Run attachment stage identity changed before removal")
		}
		if err := os.Remove(f.path); err != nil {
			return fmt.Errorf("remove Run attachment stage: %w", err)
		}
		return syncOwnerDirectory(runsDir)
	})
}

func ReadRunAttachment(nodeState, path string) (RunAttachment, error) {
	runsDir, ownerUID, err := requireOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return RunAttachment{}, err
	}
	if _, err := parseRunAttachmentPath(runsDir, path); err != nil {
		return RunAttachment{}, err
	}
	var result RunAttachment
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		var readErr error
		result, readErr = readRunAttachmentFromDir(runsDir, path, ownerUID)
		return readErr
	})
	return result, err
}

// RemoveRunAttachment removes a consumed or expired capability only if its
// path, inode and bytes still match the previously verified value.
func RemoveRunAttachment(nodeState string, expected RunAttachment) error {
	runsDir, ownerUID, err := requireOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return err
	}
	if expected.identity == nil || expected.digest.IsZero() || expected.runID.IsZero() {
		return unsafeClientState("Run attachment removal lacks an expected identity")
	}
	if _, err := parseRunAttachmentPath(runsDir, expected.path); err != nil {
		return err
	}
	return withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		current, err := readRunAttachmentFromDir(runsDir, expected.path, ownerUID)
		if err != nil {
			return err
		}
		if !os.SameFile(current.identity, expected.identity) || current.runID != expected.runID ||
			current.digest != expected.digest ||
			subtle.ConstantTimeCompare(current.token[:], expected.token[:]) != 1 {
			return unsafeClientState("Run attachment identity changed before removal")
		}
		if err := os.Remove(expected.path); err != nil {
			return fmt.Errorf("remove Run attachment: %w", err)
		}
		return syncOwnerDirectory(runsDir)
	})
}

// RemoveReapableRunAttachment removes the canonical file for a Store-approved
// terminal/expired Run only when its secret still hashes to the durable
// attachment identity. It never removes an unknown replacement.
func RemoveReapableRunAttachment(nodeState string, runID model.RunID,
	expectedHash model.Digest,
) (bool, error) {
	if runID.IsZero() || expectedHash.IsZero() {
		return false, unsafeClientState("reapable Run attachment authority is incomplete")
	}
	runsDir, ownerUID, err := requireOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return false, err
	}
	path, err := runAttachmentPathForRun(runsDir, runID)
	if err != nil {
		return false, err
	}
	removed := false
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return fmt.Errorf("%w: inspect reapable Run attachment", ErrUnsafeClientState)
		}
		current, err := readRunAttachmentFromDir(runsDir, path, ownerUID)
		if err != nil {
			return err
		}
		if current.runID != runID || !sameRunAttachmentHash(current.token[:], expectedHash) {
			return unsafeClientState("reapable Run attachment differs from durable authority")
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove reapable Run attachment: %w", err)
		}
		removed = true
		return syncOwnerDirectory(runsDir)
	})
	return removed, err
}

// ListRunAttachmentCandidates scans only files that actually exist. It keeps
// memory and the Store authorization request bounded; after authorized files
// are removed, a later scan naturally advances to the next page without a
// durable DB cursor or deleted historical rows occupying the batch.
func ListRunAttachmentCandidates(nodeState string) (RunAttachmentCandidatePage, error) {
	runsDir, ownerUID, err := ensureOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return RunAttachmentCandidatePage{}, err
	}
	page := RunAttachmentCandidatePage{candidates: make([]RunAttachmentCandidate, 0,
		maxRunAttachmentCandidates)}
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		directory, err := os.Open(runsDir)
		if err != nil {
			return fmt.Errorf("open Run attachment directory: %w", err)
		}
		defer directory.Close()
		for {
			entries, readErr := directory.ReadDir(32)
			for _, entry := range entries {
				if !strings.HasSuffix(entry.Name(), runAttachmentSuffix) {
					continue
				}
				path := filepath.Join(runsDir, entry.Name())
				attachment, err := readRunAttachmentFromDir(runsDir, path, ownerUID)
				if err != nil {
					return err
				}
				tokenHash := model.Sum(attachment.token[:])
				clear(attachment.token[:])
				if len(page.candidates) == maxRunAttachmentCandidates {
					page.more = true
					return nil
				}
				page.candidates = append(page.candidates, RunAttachmentCandidate{
					runID: attachment.RunID(), tokenHash: tokenHash,
				})
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			if readErr != nil {
				return fmt.Errorf("scan Run attachment directory: %w", readErr)
			}
		}
	})
	if err != nil {
		return RunAttachmentCandidatePage{}, err
	}
	return page, nil
}

// CleanupRunAttachmentStages removes only recognizable, owner-only temporary
// capabilities older than the bounded synchronous preclaim staging window.
// A stage has no Run identity and is never launchable; final Run attachments
// are removed by exact consume/expiry handling, never by mtime.
func CleanupRunAttachmentStages(nodeState string, at time.Time) (int, error) {
	canonical := at.Round(0).UTC()
	if canonical.IsZero() || canonical.UnixNano() <= 0 || !time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return 0, unsafeClientState("Run attachment cleanup time is invalid")
	}
	runsDir, ownerUID, err := ensureOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return 0, err
	}
	removed := 0
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		entries, err := os.ReadDir(runsDir)
		if err != nil {
			return fmt.Errorf("scan Run attachment stages: %w", err)
		}
		cutoff := canonical.Add(-maxRunAttachmentStageAge)
		for _, entry := range entries {
			if !validRunAttachmentStageName(entry.Name()) {
				continue
			}
			path := filepath.Join(runsDir, entry.Name())
			stage, err := readStagedRunAttachment(runsDir, path, ownerUID)
			if err != nil {
				return err
			}
			if stage.identity.ModTime().After(cutoff) {
				continue
			}
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove expired Run attachment stage: %w", err)
			}
			removed++
		}
		if removed != 0 {
			return syncOwnerDirectory(runsDir)
		}
		return nil
	})
	return removed, err
}

func runAttachmentPathForRun(runsDir string, runID model.RunID) (string, error) {
	if runID.IsZero() || !safeManagedFilename(runID.String()) {
		return "", unsafeClientState("Run ID is not safe for an attachment path")
	}
	return filepath.Join(runsDir, runID.String()+runAttachmentSuffix), nil
}

func parseRunAttachmentPath(runsDir, path string) (model.RunID, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != runsDir {
		return model.RunID{}, unsafeClientState("Run attachment path is outside the expected directory")
	}
	base := filepath.Base(path)
	if !strings.HasSuffix(base, runAttachmentSuffix) {
		return model.RunID{}, unsafeClientState("Run attachment path has the wrong suffix")
	}
	name := strings.TrimSuffix(base, runAttachmentSuffix)
	if !safeManagedFilename(name) {
		return model.RunID{}, unsafeClientState("Run attachment path has an invalid Run name")
	}
	runID, err := model.ParseRunID(name)
	if err != nil {
		return model.RunID{}, unsafeClientState("Run attachment path has an invalid Run ID")
	}
	expected, err := runAttachmentPathForRun(runsDir, runID)
	if err != nil || expected != path {
		return model.RunID{}, unsafeClientState("Run attachment path is not canonical")
	}
	return runID, nil
}

func readRunAttachmentFromDir(runsDir, path string, ownerUID uint32) (RunAttachment, error) {
	runID, err := parseRunAttachmentPath(runsDir, path)
	if err != nil {
		return RunAttachment{}, err
	}
	raw, identity, token, err := readRunAttachmentCapability(path, ownerUID)
	if err != nil {
		return RunAttachment{}, err
	}
	defer clear(raw)
	result := RunAttachment{path: path, runID: runID, digest: model.Sum(raw), identity: identity}
	copy(result.token[:], token)
	clear(token)
	return result, nil
}

func readStagedRunAttachment(runsDir, path string, ownerUID uint32) (StagedRunAttachment, error) {
	if filepath.Dir(path) != runsDir || !validRunAttachmentStageName(filepath.Base(path)) {
		return StagedRunAttachment{}, unsafeClientState("Run attachment stage path is not canonical")
	}
	raw, identity, token, err := readRunAttachmentCapability(path, ownerUID)
	if err != nil {
		return StagedRunAttachment{}, err
	}
	defer clear(raw)
	result := StagedRunAttachment{path: path, digest: model.Sum(raw), identity: identity}
	copy(result.token[:], token)
	clear(token)
	return result, nil
}

func readRunAttachmentCapability(path string, ownerUID uint32) ([]byte, os.FileInfo, []byte, error) {
	raw, identity, err := readOwnerRegularFile(path, runAttachmentFileBytes, ownerUID)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(raw) != runAttachmentFileBytes || raw[len(raw)-1] != '\n' {
		clear(raw)
		return nil, nil, nil, unsafeClientState("Run attachment has noncanonical bytes")
	}
	encoded := string(raw[:len(raw)-1])
	token, err := decodeOpaqueSecret(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(token) != encoded {
		clear(token)
		clear(raw)
		return nil, nil, nil, unsafeClientState("Run attachment has noncanonical bytes")
	}
	return raw, identity, token, nil
}

func validRunAttachmentStageName(name string) bool {
	if !strings.HasPrefix(name, runAttachmentStagePrefix) || !strings.HasSuffix(name, runAttachmentStageSuffix) {
		return false
	}
	encoded := strings.TrimSuffix(strings.TrimPrefix(name, runAttachmentStagePrefix), runAttachmentStageSuffix)
	if strings.ContainsAny(encoded, "= /\\\t\r\n") {
		return false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	return err == nil && len(raw) == runAttachmentStageIDBytes &&
		base64.RawURLEncoding.EncodeToString(raw) == encoded
}

func sameRunAttachmentHash(token []byte, expected model.Digest) bool {
	actual := model.Sum(token)
	actualBytes, expectedBytes := actual.Bytes(), expected.Bytes()
	equal := subtle.ConstantTimeCompare(actualBytes, expectedBytes) == 1
	clear(actualBytes)
	clear(expectedBytes)
	return equal
}
