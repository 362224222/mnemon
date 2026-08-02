package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

const (
	r7PiProjectionSchema = 1
	r7PiRuntime          = "pi"
	r7PiInstalling       = "installing"
	r7PiApplied          = "applied"
)

// R7PiProjectionReceipt names the two project-local mnemond files and private
// ownership journal, all distinct from release-path Mnemon Memory assets.
type R7PiProjectionReceipt struct {
	ExtensionPath string
	GuidePath     string
	OwnershipPath string
	Replayed      bool
	Revision      string
}

type r7PiProjectionRecord struct {
	Digest string `json:"digest"`
	Mode   string `json:"mode"`
	Path   string `json:"path"`
	Source string `json:"source"`
}

type r7PiProjectionRevision struct {
	Files   []r7PiProjectionRecord `json:"files"`
	Runtime string                 `json:"runtime"`
	Schema  int                    `json:"schema"`
}

type r7PiProjectionOwnership struct {
	Files    []r7PiProjectionRecord `json:"files"`
	Revision string                 `json:"revision"`
	Runtime  string                 `json:"runtime"`
	Schema   int                    `json:"schema"`
	State    string                 `json:"state"`
}

type r7PiProjectedFile struct {
	content []byte
	mode    os.FileMode
	path    string
	record  r7PiProjectionRecord
}

type r7PiProjectionPlan struct {
	applied         r7PiProjectionOwnership
	appliedBytes    []byte
	files           []r7PiProjectedFile
	installing      r7PiProjectionOwnership
	installingBytes []byte
	ownershipPath   string
	workspace       string
}

type r7PiProjectionBoundary func(string) error

// InstallR7PiProjection installs Pi's bounded mnemond Skill and fixed-cue
// extension without calling or sharing state with release-path `mnemon setup`.
func InstallR7PiProjection(workspace string,
	projection assets.R7Projection,
) (R7PiProjectionReceipt, error) {
	return installR7PiProjection(workspace, projection, nil)
}

func installR7PiProjection(workspace string, projection assets.R7Projection,
	boundary r7PiProjectionBoundary,
) (R7PiProjectionReceipt, error) {
	plan, err := prepareR7PiProjection(workspace, projection)
	if err != nil {
		return R7PiProjectionReceipt{}, err
	}
	receipt := r7PiReceipt(plan)
	_, statErr := os.Lstat(plan.ownershipPath)
	switch {
	case statErr == nil:
		receipt.Replayed, err = resumeR7PiProjection(plan, boundary)
	case errors.Is(statErr, os.ErrNotExist):
		err = installFreshR7PiProjection(plan, boundary)
	default:
		err = projectionConflict("inspect R7 Pi ownership journal", statErr)
	}
	if err != nil {
		return R7PiProjectionReceipt{}, err
	}
	return receipt, nil
}

func resumeR7PiProjection(plan r7PiProjectionPlan,
	boundary r7PiProjectionBoundary,
) (bool, error) {
	ownership, raw, err := readR7PiOwnership(plan)
	if err != nil {
		return false, err
	}
	if ownership.State != r7PiApplied {
		return false, convergeR7PiProjection(plan, raw, boundary)
	}
	if err := verifyR7PiProjectedState(plan); err == nil {
		return true, nil
	}
	return false, convergeR7PiFiles(plan, nil)
}

func installFreshR7PiProjection(plan r7PiProjectionPlan,
	boundary r7PiProjectionBoundary,
) error {
	if err := inspectFreshR7PiProjection(plan); err != nil {
		return err
	}
	if err := createR7PiOwnershipDirectories(plan); err != nil {
		return err
	}
	if err := publishExclusiveFile(filepath.Dir(plan.ownershipPath), plan.ownershipPath,
		plan.installingBytes, 0o600); err != nil {
		return joinConcurrentR7PiProjection(plan, boundary, err)
	}
	if err := runR7PiBoundary(boundary, "after_journal"); err != nil {
		return err
	}
	return convergeR7PiProjection(plan, plan.installingBytes, boundary)
}

func joinConcurrentR7PiProjection(plan r7PiProjectionPlan,
	boundary r7PiProjectionBoundary, publishErr error,
) error {
	// A concurrent identical installer may have published the journal after
	// preflight. Join it only after exact closed-schema validation.
	ownership, raw, err := readR7PiOwnership(plan)
	if err != nil || ownership.State != r7PiInstalling {
		return projectionConflict("publish R7 Pi ownership journal", publishErr)
	}
	return convergeR7PiProjection(plan, raw, boundary)
}

// VerifyR7PiProjection proves exact ownership, bytes, modes and fixed paths.
func VerifyR7PiProjection(workspace string, projection assets.R7Projection) error {
	plan, err := prepareR7PiProjection(workspace, projection)
	if err != nil {
		return err
	}
	ownership, _, err := readR7PiOwnership(plan)
	if err != nil {
		return err
	}
	if ownership.State != r7PiApplied {
		return projectionConflict("R7 Pi ownership journal is not applied", nil)
	}
	return verifyR7PiProjectedState(plan)
}

func prepareR7PiProjection(workspace string,
	projection assets.R7Projection,
) (r7PiProjectionPlan, error) {
	if workspace == "" || !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace {
		return r7PiProjectionPlan{}, fmt.Errorf("%w: workspace must be an absolute clean path",
			ErrUnsafeProjection)
	}
	physical, err := filepath.EvalSymlinks(workspace)
	if err != nil || physical != workspace {
		return r7PiProjectionPlan{}, fmt.Errorf("%w: workspace must be a physical path",
			ErrUnsafeProjection)
	}
	if err := requireOwnedRealDirectory(workspace); err != nil {
		return r7PiProjectionPlan{}, err
	}
	guide, extension := projection.Guide(), projection.PiExtension()
	if len(guide) == 0 || len(guide) > assets.MaxR7GuideBytes || len(extension) == 0 ||
		len(extension) > assets.MaxR7PiExtensionBytes || projection.HookCue() == "" {
		return r7PiProjectionPlan{}, fmt.Errorf("%w: R7 Pi assets are incomplete",
			ErrUnsafeProjection)
	}

	sources := []struct {
		content     []byte
		destination string
		source      string
	}{
		{guide, filepath.Join(".pi", "skills", "mnemond", "SKILL.md"), "r7/mnemond.md"},
		{extension, filepath.Join(".pi", "extensions", "mnemond.ts"), "r7/pi/mnemond.ts"},
	}
	files := make([]r7PiProjectedFile, 0, len(sources))
	records := make([]r7PiProjectionRecord, 0, len(sources))
	for _, source := range sources {
		record := r7PiProjectionRecord{Digest: digest(source.content), Mode: "0644",
			Path: filepath.ToSlash(source.destination), Source: source.source}
		files = append(files, r7PiProjectedFile{content: append([]byte(nil), source.content...),
			mode: 0o644, path: filepath.Join(workspace, source.destination), record: record})
		records = append(records, record)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].record.Path < files[j].record.Path })
	sort.Slice(records, func(i, j int) bool { return records[i].Path < records[j].Path })

	revisionBytes, err := json.Marshal(r7PiProjectionRevision{
		Files: records, Runtime: r7PiRuntime, Schema: r7PiProjectionSchema,
	})
	if err != nil {
		return r7PiProjectionPlan{}, fmt.Errorf("encode R7 Pi projection revision: %w", err)
	}
	revision := digest(revisionBytes)
	applied := r7PiProjectionOwnership{Files: records, Revision: revision,
		Runtime: r7PiRuntime, Schema: r7PiProjectionSchema, State: r7PiApplied}
	installing := applied
	installing.State = r7PiInstalling
	appliedBytes, err := canonicalR7PiOwnership(applied)
	if err != nil {
		return r7PiProjectionPlan{}, err
	}
	installingBytes, err := canonicalR7PiOwnership(installing)
	if err != nil {
		return r7PiProjectionPlan{}, err
	}
	return r7PiProjectionPlan{
		applied: applied, appliedBytes: appliedBytes, files: files,
		installing: installing, installingBytes: installingBytes,
		ownershipPath: filepath.Join(workspace, ".mnemon", "harness", "integrations",
			"pi-mnemond", "ownership.json"),
		workspace: workspace,
	}, nil
}

func canonicalR7PiOwnership(value r7PiProjectionOwnership) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode R7 Pi ownership journal: %w", err)
	}
	return append(raw, '\n'), nil
}

func r7PiReceipt(plan r7PiProjectionPlan) R7PiProjectionReceipt {
	receipt := R7PiProjectionReceipt{OwnershipPath: plan.ownershipPath,
		Revision: plan.applied.Revision}
	for _, file := range plan.files {
		switch file.record.Source {
		case "r7/mnemond.md":
			receipt.GuidePath = file.path
		case "r7/pi/mnemond.ts":
			receipt.ExtensionPath = file.path
		}
	}
	return receipt
}

func inspectFreshR7PiProjection(plan r7PiProjectionPlan) error {
	if err := validateExistingR7PiParents(plan); err != nil {
		return err
	}
	for _, file := range plan.files {
		if _, err := os.Lstat(file.path); err == nil {
			return projectionConflict("R7 Pi file exists without ownership", nil)
		} else if !errors.Is(err, os.ErrNotExist) {
			return projectionConflict("inspect R7 Pi file", err)
		}
	}
	return nil
}

func createR7PiOwnershipDirectories(plan r7PiProjectionPlan) error {
	relative, err := filepath.Rel(plan.workspace, filepath.Dir(plan.ownershipPath))
	if err != nil {
		return projectionConflict("resolve R7 Pi ownership directory", err)
	}
	return ensureOwnedDirectoryChain(plan.workspace, relative, 0o700)
}

func readR7PiOwnership(plan r7PiProjectionPlan) (r7PiProjectionOwnership, []byte, error) {
	relative, err := filepath.Rel(plan.workspace, filepath.Dir(plan.ownershipPath))
	if err != nil {
		return r7PiProjectionOwnership{}, nil,
			projectionConflict("resolve R7 Pi ownership directory", err)
	}
	if err := requireOwnedDirectoryChain(plan.workspace, relative); err != nil {
		return r7PiProjectionOwnership{}, nil, err
	}
	raw, _, err := readSafeRegular(plan.ownershipPath, 0o600, 64<<10)
	if err != nil {
		return r7PiProjectionOwnership{}, nil,
			projectionConflict("read R7 Pi ownership journal", err)
	}
	var ownership r7PiProjectionOwnership
	if err := decodeClosedCanonical(raw, &ownership); err != nil {
		return r7PiProjectionOwnership{}, nil,
			projectionConflict("decode R7 Pi ownership journal", err)
	}
	switch {
	case reflect.DeepEqual(ownership, plan.installing) && bytes.Equal(raw, plan.installingBytes):
	case reflect.DeepEqual(ownership, plan.applied) && bytes.Equal(raw, plan.appliedBytes):
	default:
		return r7PiProjectionOwnership{}, nil,
			projectionConflict("R7 Pi ownership differs from exact embedded assets", nil)
	}
	return ownership, raw, nil
}

func convergeR7PiProjection(plan r7PiProjectionPlan, journal []byte,
	boundary r7PiProjectionBoundary,
) error {
	if !bytes.Equal(journal, plan.installingBytes) {
		return projectionConflict("R7 Pi convergence requires an installing journal", nil)
	}
	if err := convergeR7PiFiles(plan, boundary); err != nil {
		return err
	}
	if err := runR7PiBoundary(boundary, "before_applied"); err != nil {
		return err
	}
	if err := replaceExactFile(filepath.Dir(plan.ownershipPath), plan.ownershipPath,
		journal, plan.appliedBytes, 0o600); err != nil {
		ownership, _, readErr := readR7PiOwnership(plan)
		if readErr != nil || ownership.State != r7PiApplied {
			return projectionConflict("finalize R7 Pi ownership journal", err)
		}
	}
	ownership, _, err := readR7PiOwnership(plan)
	if err != nil {
		return err
	}
	if ownership.State != r7PiApplied {
		return projectionConflict("R7 Pi ownership journal did not become applied", nil)
	}
	return verifyR7PiProjectedState(plan)
}

func runR7PiBoundary(boundary r7PiProjectionBoundary, stage string) error {
	if boundary == nil {
		return nil
	}
	return boundary(stage)
}
