package node

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	testRunAttachmentSuffix    = ".attach"
	testRunAttachmentStageAge  = 10 * time.Minute
	testRunAttachmentBatchSize = 65
)

func ensureTestRunsDirectory(nodeState string) (string, error) {
	return ensureTestOwnerDirectory(filepath.Join(nodeState, "runs"))
}

type testRunAttachmentFilesystem struct {
	nodeState string
}

func (filesystem testRunAttachmentFilesystem) ListCandidates() ([]RunAttachmentCandidate, error) {
	page, err := ListRunAttachmentCandidates(filesystem.nodeState)
	if err != nil {
		return nil, err
	}
	candidates := page.Candidates()
	result := make([]RunAttachmentCandidate, len(candidates))
	for index, candidate := range candidates {
		result[index] = RunAttachmentCandidate{RunID: candidate.RunID(),
			TokenHash: candidate.TokenHash()}
	}
	return result, nil
}

func (filesystem testRunAttachmentFilesystem) RemoveReapable(runID model.RunID,
	tokenHash model.Digest,
) (bool, error) {
	if runID.IsZero() || tokenHash.IsZero() {
		return false, ErrUnsafeClientState
	}
	path := filepath.Join(filesystem.nodeState, "runs", runID.String()+testRunAttachmentSuffix)
	attachment, err := ReadRunAttachment(filesystem.nodeState, path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if attachment.runID != runID || model.Sum(attachment.token[:]) != tokenHash {
		return false, ErrUnsafeClientState
	}
	return true, os.Remove(path)
}

func (filesystem testRunAttachmentFilesystem) CleanupStages(at time.Time) (int, error) {
	runs, err := ensureTestRunsDirectory(filesystem.nodeState)
	if err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(runs)
	if err != nil {
		return 0, err
	}
	cutoff := at.Add(-testRunAttachmentStageAge)
	removed := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".mnemon-run-attachment-") ||
			!strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(runs, entry.Name())
		info, err := os.Stat(path)
		if err != nil {
			return 0, err
		}
		if info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(path); err != nil {
			return 0, err
		}
		removed++
	}
	return removed, nil
}

func (filesystem testRunAttachmentFilesystem) Stage(random io.Reader) (StagedRunAttachment, error) {
	runs, err := ensureTestRunsDirectory(filesystem.nodeState)
	if err != nil {
		return nil, err
	}
	entropy := make([]byte, 48)
	if _, err := io.ReadFull(random, entropy); err != nil {
		return nil, err
	}
	var token [testOpaqueSecretBytes]byte
	copy(token[:], entropy[:testOpaqueSecretBytes])
	stageID := base64.RawURLEncoding.EncodeToString(entropy[testOpaqueSecretBytes:])
	path := filepath.Join(runs, ".mnemon-run-attachment-"+stageID+".tmp")
	raw := append([]byte(base64.RawURLEncoding.EncodeToString(token[:])), '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &testStagedRunAttachment{nodeState: filesystem.nodeState, path: path,
		token: token, identity: info}, nil
}

func (filesystem testRunAttachmentFilesystem) Remove(attachment RunAttachment) error {
	value, ok := attachment.(*testRunAttachment)
	if !ok || value == nil {
		return ErrUnsafeClientState
	}
	current, err := ReadRunAttachment(filesystem.nodeState, value.path)
	if err != nil {
		return err
	}
	if current.runID != value.runID || current.digest != value.digest ||
		subtle.ConstantTimeCompare(current.token[:], value.token[:]) != 1 ||
		!os.SameFile(current.identity, value.identity) {
		return ErrUnsafeClientState
	}
	return os.Remove(value.path)
}

type testStagedRunAttachment struct {
	nodeState string
	path      string
	token     [testOpaqueSecretBytes]byte
	identity  os.FileInfo
}

func (attachment *testStagedRunAttachment) TokenHash() model.Digest {
	return model.Sum(attachment.token[:])
}

func (attachment *testStagedRunAttachment) Publish(runID model.RunID) (RunAttachment, error) {
	path := filepath.Join(attachment.nodeState, "runs", runID.String()+testRunAttachmentSuffix)
	if err := os.Rename(attachment.path, path); err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &testRunAttachment{path: path, runID: runID, token: attachment.token,
		digest: model.Sum(attachment.token[:]), identity: info}, nil
}

func (attachment *testStagedRunAttachment) Discard() error {
	if err := os.Remove(attachment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

type testRunAttachment struct {
	path     string
	runID    model.RunID
	token    [testOpaqueSecretBytes]byte
	digest   model.Digest
	identity os.FileInfo
}

func (attachment *testRunAttachment) Path() string         { return attachment.path }
func (attachment *testRunAttachment) RunID() model.RunID   { return attachment.runID }
func (attachment *testRunAttachment) Digest() model.Digest { return attachment.digest }
func (attachment *testRunAttachment) HeaderValue() string {
	return base64.RawURLEncoding.EncodeToString(attachment.token[:])
}

func ReadRunAttachment(nodeState, path string) (*testRunAttachment, error) {
	runs := filepath.Join(nodeState, "runs")
	if !strings.HasPrefix(path, runs+string(os.PathSeparator)) ||
		!strings.HasSuffix(path, testRunAttachmentSuffix) {
		return nil, ErrUnsafeClientState
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err != nil || len(raw) != testProfileTokenBytes || raw[len(raw)-1] != '\n' {
		return nil, ErrUnsafeClientState
	}
	token, err := decodeTestSecret(string(raw[:len(raw)-1]))
	if err != nil {
		return nil, err
	}
	runIDText := strings.TrimSuffix(filepath.Base(path), testRunAttachmentSuffix)
	runID, err := model.ParseRunID(runIDText)
	if err != nil {
		return nil, ErrUnsafeClientState
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	var fixed [testOpaqueSecretBytes]byte
	copy(fixed[:], token)
	return &testRunAttachment{path: path, runID: runID, token: fixed,
		digest: model.Sum(fixed[:]), identity: info}, nil
}

type testRunAttachmentCandidate struct {
	runID     model.RunID
	tokenHash model.Digest
}

func (candidate testRunAttachmentCandidate) RunID() model.RunID { return candidate.runID }
func (candidate testRunAttachmentCandidate) TokenHash() model.Digest {
	return candidate.tokenHash
}

type testRunAttachmentCandidatePage struct {
	candidates []testRunAttachmentCandidate
	more       bool
}

func (page testRunAttachmentCandidatePage) More() bool { return page.more }
func (page testRunAttachmentCandidatePage) Candidates() []testRunAttachmentCandidate {
	return append([]testRunAttachmentCandidate(nil), page.candidates...)
}

func ListRunAttachmentCandidates(nodeState string) (testRunAttachmentCandidatePage, error) {
	runs, err := ensureTestRunsDirectory(nodeState)
	if err != nil {
		return testRunAttachmentCandidatePage{}, err
	}
	entries, err := os.ReadDir(runs)
	if err != nil {
		return testRunAttachmentCandidatePage{}, err
	}
	page := testRunAttachmentCandidatePage{}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), testRunAttachmentSuffix) {
			continue
		}
		attachment, err := ReadRunAttachment(nodeState, filepath.Join(runs, entry.Name()))
		if err != nil {
			return testRunAttachmentCandidatePage{}, err
		}
		if len(page.candidates) == testRunAttachmentBatchSize {
			page.more = true
			return page, nil
		}
		page.candidates = append(page.candidates, testRunAttachmentCandidate{
			runID: attachment.runID, tokenHash: model.Sum(attachment.token[:]),
		})
	}
	return page, nil
}

var _ RunAttachmentFilesystem = testRunAttachmentFilesystem{}
