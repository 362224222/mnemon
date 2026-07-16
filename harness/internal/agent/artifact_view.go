package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	artifactViewControlMode = 0o700
	artifactViewDirMode     = 0o500
	artifactViewFileMode    = 0o400
)

var ErrArtifactViewValidation = errors.New("readonly Artifact view validation failed")

// ReadonlyArtifactViewValidator checks the receipt-bound local projection at
// action time without reading its bytes as authority. The accepted Event
// transaction separately revalidates the original CAS root, pin, and
// provenance before a referenced role can commit.
type ReadonlyArtifactViewValidator struct {
	nodeState string
	nodeInfo  os.FileInfo
	ownerUID  uint32
}

func NewReadonlyArtifactViewValidator(nodeState string) (*ReadonlyArtifactViewValidator, error) {
	if nodeState == "" || !filepath.IsAbs(nodeState) || filepath.Clean(nodeState) != nodeState {
		return nil, fmt.Errorf("%w: Node state path is not absolute and canonical", ErrArtifactViewValidation)
	}
	info, err := os.Lstat(nodeState)
	uid, ownerOK := artifactViewOwner(info)
	if err != nil || !ownerOK || uid != uint32(os.Geteuid()) ||
		!artifactViewDirectory(info, artifactViewControlMode) {
		return nil, fmt.Errorf("%w: Node state is not an owner-only real directory", ErrArtifactViewValidation)
	}
	root, err := os.OpenRoot(nodeState)
	if err != nil {
		return nil, fmt.Errorf("%w: open Node state: %v", ErrArtifactViewValidation, err)
	}
	opened, openErr := root.Stat(".")
	closeErr := root.Close()
	if openErr != nil || closeErr != nil || !sameArtifactViewDirectory(info, opened, uid,
		artifactViewControlMode) {
		return nil, fmt.Errorf("%w: Node state changed while opening", ErrArtifactViewValidation)
	}
	return &ReadonlyArtifactViewValidator{nodeState: nodeState, nodeInfo: info, ownerUID: uid}, nil
}

func (validator *ReadonlyArtifactViewValidator) Validate(ctx context.Context,
	current model.CurrentReadReceipt, ref model.CurrentArtifactRef,
) error {
	if validator == nil || ctx == nil || current.RunID().IsZero() || ref.RootDigest().IsZero() {
		return fmt.Errorf("%w: incomplete current view authority", ErrArtifactViewValidation)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	viewPath, ok := ref.ViewPath()
	if !ok || !receiptContainsExactArtifactView(current, ref) {
		return fmt.Errorf("%w: path is not in the exact current receipt", ErrArtifactViewValidation)
	}
	components := strings.Split(viewPath, "/")
	if len(components) < 6 || components[4] != current.RunID().String() {
		return fmt.Errorf("%w: path belongs to another managed Run", ErrArtifactViewValidation)
	}
	nodeRoot, err := validator.openNodeRoot()
	if err != nil {
		return err
	}
	defer nodeRoot.Close()
	viewsRoot, err := openArtifactViewDirectory(nodeRoot, "views", validator.ownerUID,
		artifactViewControlMode)
	if err != nil {
		return err
	}
	defer viewsRoot.Close()
	runRoot, err := openArtifactViewDirectory(viewsRoot, components[4], validator.ownerUID,
		artifactViewControlMode)
	if err != nil {
		return err
	}
	defer runRoot.Close()
	ordinalRoot, err := openArtifactViewDirectory(runRoot, components[5], validator.ownerUID,
		artifactViewDirMode)
	if err != nil {
		return err
	}
	defer ordinalRoot.Close()
	if len(components) == 6 {
		return nil
	}

	parent := ordinalRoot
	opened := make([]*os.Root, 0, len(components)-7)
	defer func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}()
	for index, component := range components[6:] {
		if err := ctx.Err(); err != nil {
			return err
		}
		last := index == len(components[6:])-1
		if last {
			return validateArtifactViewLeaf(parent, component, validator.ownerUID)
		}
		next, err := openArtifactViewDirectory(parent, component, validator.ownerUID,
			artifactViewDirMode)
		if err != nil {
			return err
		}
		opened = append(opened, next)
		parent = next
	}
	return fmt.Errorf("%w: view path has no leaf", ErrArtifactViewValidation)
}

func (validator *ReadonlyArtifactViewValidator) openNodeRoot() (*os.Root, error) {
	before, err := os.Lstat(validator.nodeState)
	if err != nil || !sameArtifactViewDirectory(validator.nodeInfo, before, validator.ownerUID,
		artifactViewControlMode) {
		return nil, fmt.Errorf("%w: Node state identity or mode changed", ErrArtifactViewValidation)
	}
	root, err := os.OpenRoot(validator.nodeState)
	if err != nil {
		return nil, fmt.Errorf("%w: open Node state: %v", ErrArtifactViewValidation, err)
	}
	opened, openErr := root.Stat(".")
	after, pathErr := os.Lstat(validator.nodeState)
	if openErr != nil || pathErr != nil ||
		!sameArtifactViewDirectory(before, opened, validator.ownerUID, artifactViewControlMode) ||
		!sameArtifactViewDirectory(before, after, validator.ownerUID, artifactViewControlMode) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: Node state changed while opening", ErrArtifactViewValidation)
	}
	return root, nil
}

func receiptContainsExactArtifactView(current model.CurrentReadReceipt,
	want model.CurrentArtifactRef,
) bool {
	wantPath, _ := want.ViewPath()
	count := 0
	for _, candidate := range current.ArtifactRefs() {
		path, ok := candidate.ViewPath()
		if ok && candidate.RootDigest() == want.RootDigest() && path == wantPath {
			count++
		}
	}
	return count == 1
}

func openArtifactViewDirectory(parent *os.Root, name string, uid uint32,
	mode os.FileMode,
) (*os.Root, error) {
	before, err := parent.Lstat(name)
	if err != nil || !artifactViewDirectory(before, mode) || !artifactViewOwnedBy(before, uid) {
		return nil, fmt.Errorf("%w: directory %q has invalid owner, type, or mode",
			ErrArtifactViewValidation, name)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("%w: open directory %q: %v", ErrArtifactViewValidation, name, err)
	}
	opened, openErr := root.Stat(".")
	after, pathErr := parent.Lstat(name)
	if openErr != nil || pathErr != nil || !sameArtifactViewDirectory(before, opened, uid, mode) ||
		!sameArtifactViewDirectory(before, after, uid, mode) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: directory %q changed while opening", ErrArtifactViewValidation, name)
	}
	return root, nil
}

func validateArtifactViewLeaf(parent *os.Root, name string, uid uint32) error {
	before, err := parent.Lstat(name)
	if err != nil || !artifactViewOwnedBy(before, uid) || before.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: leaf %q has invalid owner or type", ErrArtifactViewValidation, name)
	}
	if before.IsDir() {
		root, err := openArtifactViewDirectory(parent, name, uid, artifactViewDirMode)
		if err != nil {
			return err
		}
		return root.Close()
	}
	if !before.Mode().IsRegular() || before.Mode().Perm() != artifactViewFileMode {
		return fmt.Errorf("%w: leaf %q is not a protected file", ErrArtifactViewValidation, name)
	}
	file, err := parent.Open(name)
	if err != nil {
		return fmt.Errorf("%w: open leaf %q: %v", ErrArtifactViewValidation, name, err)
	}
	opened, openErr := file.Stat()
	closeErr := file.Close()
	after, pathErr := parent.Lstat(name)
	if openErr != nil || closeErr != nil || pathErr != nil ||
		!sameArtifactViewFile(before, opened, uid) || !sameArtifactViewFile(before, after, uid) {
		return fmt.Errorf("%w: leaf %q changed while opening", ErrArtifactViewValidation, name)
	}
	return nil
}

func artifactViewOwner(info os.FileInfo) (uint32, bool) {
	if info == nil {
		return 0, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Uid, true
}

func artifactViewOwnedBy(info os.FileInfo, uid uint32) bool {
	owner, ok := artifactViewOwner(info)
	return ok && owner == uid
}

func artifactViewDirectory(info os.FileInfo, mode os.FileMode) bool {
	return info != nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == mode
}

func sameArtifactViewDirectory(left, right os.FileInfo, uid uint32, mode os.FileMode) bool {
	return left != nil && right != nil && os.SameFile(left, right) &&
		artifactViewDirectory(left, mode) && artifactViewDirectory(right, mode) &&
		artifactViewOwnedBy(left, uid) && artifactViewOwnedBy(right, uid)
}

func sameArtifactViewFile(left, right os.FileInfo, uid uint32) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.Mode() == right.Mode() &&
		left.Mode().IsRegular() && left.Mode()&os.ModeSymlink == 0 &&
		left.Mode().Perm() == artifactViewFileMode && left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime()) && artifactViewOwnedBy(left, uid) && artifactViewOwnedBy(right, uid)
}

var _ ArtifactViewValidator = (*ReadonlyArtifactViewValidator)(nil)
