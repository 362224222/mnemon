package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrArtifactChanged = errors.New("Artifact path changed during capture")
	ErrArtifactType    = errors.New("unsupported Artifact filesystem object")
	ErrClosureMismatch = errors.New("Artifact closure does not match immutable CAS bytes")
)

type Clock func() time.Time

type CapturedRoot struct {
	RootDigest     model.Digest
	Manifest       model.JSON
	ManifestDigest model.Digest
	TotalBytes     uint64
	CreatedAt      time.Time
	VerifiedAt     time.Time
}

type CapturedBlock struct {
	Digest    model.Digest
	SizeBytes uint64
	CreatedAt time.Time
}

type RootBlock struct {
	RootDigest  model.Digest
	Ordinal     uint64
	LogicalPath string
	OffsetBytes uint64
	LengthBytes uint64
	BlockDigest model.Digest
	Mode        uint32
}

type Closure struct {
	roots      []CapturedRoot
	blocks     []CapturedBlock
	blockMap   []RootBlock
	checkpoint model.JSON
	capturedAt time.Time
}

func (closure Closure) Roots() []CapturedRoot   { return append([]CapturedRoot{}, closure.roots...) }
func (closure Closure) Blocks() []CapturedBlock { return append([]CapturedBlock{}, closure.blocks...) }
func (closure Closure) BlockMap() []RootBlock   { return append([]RootBlock{}, closure.blockMap...) }
func (closure Closure) Checkpoint() model.JSON  { return closure.checkpoint }
func (closure Closure) CapturedAt() time.Time   { return closure.capturedAt }
func (closure Closure) IsZero() bool            { return closure.checkpoint.IsZero() }
func (closure Closure) SameContent(other Closure) bool {
	if closure.checkpoint.IsZero() || other.checkpoint.IsZero() ||
		closure.checkpoint.String() != other.checkpoint.String() || len(closure.roots) != len(other.roots) ||
		len(closure.blocks) != len(other.blocks) || len(closure.blockMap) != len(other.blockMap) {
		return false
	}
	for index := range closure.roots {
		left, right := closure.roots[index], other.roots[index]
		if left.RootDigest != right.RootDigest || left.Manifest.String() != right.Manifest.String() ||
			left.ManifestDigest != right.ManifestDigest || left.TotalBytes != right.TotalBytes {
			return false
		}
	}
	for index := range closure.blocks {
		if closure.blocks[index].Digest != other.blocks[index].Digest ||
			closure.blocks[index].SizeBytes != other.blocks[index].SizeBytes {
			return false
		}
	}
	for index := range closure.blockMap {
		if closure.blockMap[index] != other.blockMap[index] {
			return false
		}
	}
	return true
}

type captureLimits struct {
	maxEntries int
	maxTotal   uint64
}

// ObjectSink is the complete byte-store authority needed by Capturer.
type ObjectSink interface {
	Put(model.Digest, []byte) (PutResult, error)
}

type Capturer struct {
	workspace     string
	workspaceInfo os.FileInfo
	clock         Clock
	limits        captureLimits
	afterOpen     func(string)
}

func NewCapturer(workspace string, clock Clock) (*Capturer, error) {
	if workspace == "" || !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace {
		return nil, fmt.Errorf("%w: workspace must be an absolute canonical directory", ErrCASInput)
	}
	info, err := os.Lstat(workspace)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: workspace is not a real directory", ErrArtifactPath)
	}
	opened, err := os.Open(workspace)
	if err != nil {
		return nil, fmt.Errorf("open Artifact workspace: %w", err)
	}
	openedInfo, statErr := opened.Stat()
	closeErr := opened.Close()
	if statErr != nil || !os.SameFile(info, openedInfo) || closeErr != nil {
		return nil, fmt.Errorf("%w: workspace changed while opening", ErrArtifactChanged)
	}
	if clock == nil {
		clock = time.Now
	}
	return &Capturer{workspace: workspace, workspaceInfo: info, clock: clock,
		limits: captureLimits{maxEntries: MaxEntries, maxTotal: MaxTotalBytes}}, nil
}

func (capturer *Capturer) Capture(ctx context.Context, requested []string,
	sink ObjectSink,
) (Closure, error) {
	if capturer == nil || sink == nil || ctx == nil || len(requested) == 0 ||
		len(requested) > MaxRoots {
		return Closure{}, fmt.Errorf("%w: capture requires 1..%d roots", ErrArtifactLimit, MaxRoots)
	}
	if err := capturer.verifyWorkspace(); err != nil {
		return Closure{}, err
	}
	workspaceRoot, err := capturer.openWorkspaceRoot()
	if err != nil {
		return Closure{}, err
	}
	defer workspaceRoot.Close()
	capturedAt, err := canonicalCaptureTime(capturer.clock())
	if err != nil {
		return Closure{}, err
	}
	logicalRoots := make([]string, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for index, raw := range requested {
		logical, err := normalizeRequestedRoot(raw)
		if err != nil {
			return Closure{}, err
		}
		if _, duplicate := seen[logical]; duplicate {
			return Closure{}, fmt.Errorf("%w: duplicate root %q", ErrArtifactPath, logical)
		}
		seen[logical] = struct{}{}
		logicalRoots[index] = logical
	}

	state := captureState{capturer: capturer, sink: sink, capturedAt: capturedAt,
		blocks: make(map[model.Digest]CapturedBlock)}
	for _, logical := range logicalRoots {
		if err := ctx.Err(); err != nil {
			return Closure{}, err
		}
		root := rootCapture{}
		if err := state.captureRequestedRoot(ctx, workspaceRoot, logical, &root); err != nil {
			return Closure{}, err
		}
		if err := ctx.Err(); err != nil {
			return Closure{}, err
		}
		manifest, err := NewManifest(ManifestSpec{RootKind: root.kind,
			RootPath: logical, Entries: root.entries})
		if err != nil {
			return Closure{}, err
		}
		if _, err := sink.Put(manifest.ManifestDigest(), manifest.CanonicalJSON().Bytes()); err != nil {
			return Closure{}, err
		}
		state.roots = append(state.roots, CapturedRoot{RootDigest: manifest.RootDigest(),
			Manifest: manifest.CanonicalJSON(), ManifestDigest: manifest.ManifestDigest(),
			TotalBytes: manifest.TotalBytes(), CreatedAt: capturedAt, VerifiedAt: capturedAt})
		ordinal := uint64(0)
		for _, entry := range manifest.Entries() {
			for _, block := range entry.Blocks {
				state.blockMap = append(state.blockMap, RootBlock{RootDigest: manifest.RootDigest(),
					Ordinal: ordinal, LogicalPath: entry.LogicalPath, OffsetBytes: block.OffsetBytes,
					LengthBytes: block.LengthBytes, BlockDigest: block.Digest, Mode: entry.Mode})
				ordinal++
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return Closure{}, err
	}
	if err := capturer.verifyOpenWorkspaceRoot(workspaceRoot); err != nil {
		return Closure{}, err
	}
	if err := capturer.verifyWorkspace(); err != nil {
		return Closure{}, err
	}
	return state.closure()
}

type rootCapture struct {
	kind    EntryKind
	entries []ManifestEntry
}

type captureState struct {
	capturer   *Capturer
	sink       ObjectSink
	capturedAt time.Time
	entryCount int
	totalBytes uint64
	roots      []CapturedRoot
	blocks     map[model.Digest]CapturedBlock
	blockMap   []RootBlock
}

func (state *captureState) captureRequestedRoot(ctx context.Context, workspaceRoot *os.Root,
	logical string, root *rootCapture,
) error {
	components := strings.Split(logical, "/")
	return state.captureThroughComponents(ctx, workspaceRoot, components, logical, root)
}

func (state *captureState) captureThroughComponents(ctx context.Context, parent *os.Root,
	components []string, logical string, root *rootCapture,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(components) == 1 {
		return state.captureNode(ctx, parent, components[0], logical, root)
	}
	name := components[0]
	before, err := parent.Lstat(name)
	if err != nil {
		return fmt.Errorf("capture Artifact root component %q: %w", name, err)
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: symlink root component %q", ErrArtifactType, name)
	}
	if !before.IsDir() {
		return fmt.Errorf("%w: non-directory root component %q", ErrArtifactType, name)
	}
	directoryRoot, err := parent.OpenRoot(name)
	if err != nil {
		return fmt.Errorf("%w: open root component %q: %v", ErrArtifactChanged, name, err)
	}
	defer directoryRoot.Close()
	opened, statErr := directoryRoot.Stat(".")
	after, pathErr := parent.Lstat(name)
	if statErr != nil || pathErr != nil || !sameFileSnapshot(before, opened) ||
		!sameFileSnapshot(before, after) {
		return fmt.Errorf("%w: root component %q changed while opening", ErrArtifactChanged, name)
	}
	return state.captureThroughComponents(ctx, directoryRoot, components[1:], logical, root)
}

func (state *captureState) captureNode(ctx context.Context, parent *os.Root, name, logical string,
	root *rootCapture,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	logical, err := validateLogicalPath(logical)
	if err != nil {
		return err
	}
	before, err := parent.Lstat(name)
	if err != nil {
		return fmt.Errorf("capture Artifact %q: %w", logical, err)
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: symlink %q", ErrArtifactType, logical)
	}
	if root.kind == "" {
		if before.IsDir() {
			root.kind = EntryDirectory
		} else if before.Mode().IsRegular() {
			root.kind = EntryFile
		}
	}
	switch {
	case before.IsDir():
		return state.captureDirectory(ctx, parent, name, logical, before, root)
	case before.Mode().IsRegular():
		return state.captureFile(ctx, parent, name, logical, before, root)
	default:
		return fmt.Errorf("%w: %q has mode %s", ErrArtifactType, logical, before.Mode().String())
	}
}

func (state *captureState) captureDirectory(ctx context.Context, parent *os.Root, name, logical string,
	before os.FileInfo, root *rootCapture,
) error {
	if err := state.addEntry(); err != nil {
		return err
	}
	directoryRoot, err := parent.OpenRoot(name)
	if err != nil {
		return fmt.Errorf("%w: open Artifact directory %q: %v", ErrArtifactChanged, logical, err)
	}
	defer directoryRoot.Close()
	openedRoot, rootStatErr := directoryRoot.Stat(".")
	afterOpenPath, pathErr := parent.Lstat(name)
	if rootStatErr != nil || pathErr != nil || !openedRoot.IsDir() ||
		!sameFileSnapshot(before, openedRoot) || !sameFileSnapshot(before, afterOpenPath) {
		return fmt.Errorf("%w: directory %q changed while opening", ErrArtifactChanged, logical)
	}
	// Open the directory stream from its parent Root. This produces an
	// independently readable stream on Darwin while retaining openat
	// confinement to the same parent handle.
	directory, err := parent.Open(name)
	if err != nil {
		return fmt.Errorf("open Artifact directory handle %q: %w", logical, err)
	}
	defer directory.Close()
	openedFile, err := directory.Stat()
	if err != nil || !sameFileSnapshot(before, openedFile) {
		return fmt.Errorf("%w: directory %q handle changed while opening", ErrArtifactChanged, logical)
	}
	state.callAfterOpen(logical)
	remainingEntries := state.capturer.limits.maxEntries - state.entryCount
	children := make([]os.FileInfo, 0, remainingEntries)
	for {
		batch, readErr := directory.Readdir(remainingEntries - len(children) + 1)
		children = append(children, batch...)
		if len(children) > remainingEntries {
			return fmt.Errorf("%w: entry count exceeds %d", ErrArtifactLimit,
				state.capturer.limits.maxEntries)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read Artifact directory %q: %w", logical, readErr)
		}
		if len(batch) == 0 {
			return fmt.Errorf("read Artifact directory %q: %w", logical, io.ErrNoProgress)
		}
	}
	sort.Slice(children, func(left, right int) bool { return children[left].Name() < children[right].Name() })
	root.entries = append(root.entries, ManifestEntry{Kind: EntryDirectory,
		LogicalPath: logical, Mode: uint32(before.Mode().Perm()), Blocks: []ManifestBlock{}})
	for _, child := range children {
		name := child.Name()
		if name == "" || name == "." || name == ".." || !utf8.ValidString(name) ||
			strings.ContainsRune(name, 0) || strings.Contains(name, "/") {
			return fmt.Errorf("%w: directory %q contains a non-UTF-8 or invalid name", ErrArtifactPath, logical)
		}
		childLogical := path.Join(logical, name)
		if err := state.captureNode(ctx, directoryRoot, name, childLogical, root); err != nil {
			return err
		}
	}
	afterFD, fdErr := directory.Stat()
	afterRoot, rootErr := directoryRoot.Stat(".")
	afterPath, pathErr := parent.Lstat(name)
	if fdErr != nil || pathErr != nil || !sameFileSnapshot(before, afterFD) ||
		rootErr != nil || !sameFileSnapshot(before, afterRoot) || !sameFileSnapshot(before, afterPath) {
		return fmt.Errorf("%w: directory %q changed during traversal", ErrArtifactChanged, logical)
	}
	return nil
}

func (state *captureState) captureFile(ctx context.Context, parent *os.Root, name, logical string,
	before os.FileInfo, root *rootCapture,
) error {
	if before.Size() < 0 || state.totalBytes > state.capturer.limits.maxTotal ||
		uint64(before.Size()) > state.capturer.limits.maxTotal-state.totalBytes {
		return fmt.Errorf("%w: file %q exceeds remaining byte budget", ErrArtifactLimit, logical)
	}
	if err := state.addEntry(); err != nil {
		return err
	}
	file, err := parent.Open(name)
	if err != nil {
		return fmt.Errorf("%w: open Artifact file %q: %v", ErrArtifactChanged, logical, err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !sameFileSnapshot(before, opened) {
		return fmt.Errorf("%w: file %q changed while opening", ErrArtifactChanged, logical)
	}
	state.callAfterOpen(logical)
	remaining := uint64(opened.Size())
	offset := uint64(0)
	blocks := make([]ManifestBlock, 0, (remaining+BlockSize-1)/BlockSize)
	buffer := make([]byte, BlockSize)
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := uint64(BlockSize)
		if remaining < length {
			length = remaining
		}
		if _, err := io.ReadFull(file, buffer[:length]); err != nil {
			return fmt.Errorf("%w: short read for file %q: %v", ErrArtifactChanged, logical, err)
		}
		content := buffer[:length]
		digest := model.Sum(content)
		if _, err := state.sink.Put(digest, content); err != nil {
			return err
		}
		blocks = append(blocks, ManifestBlock{Digest: digest, OffsetBytes: offset, LengthBytes: length})
		if _, exists := state.blocks[digest]; !exists {
			state.blocks[digest] = CapturedBlock{Digest: digest, SizeBytes: length, CreatedAt: state.capturedAt}
		}
		offset += length
		remaining -= length
	}
	var extra [1]byte
	if count, err := file.Read(extra[:]); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read Artifact file %q tail: %w", logical, err)
	} else if count != 0 {
		return fmt.Errorf("%w: file %q grew during capture", ErrArtifactChanged, logical)
	}
	afterFD, fdErr := file.Stat()
	afterPath, pathErr := parent.Lstat(name)
	if fdErr != nil || pathErr != nil || !sameFileSnapshot(before, afterFD) ||
		!sameFileSnapshot(before, afterPath) {
		return fmt.Errorf("%w: file %q changed during capture", ErrArtifactChanged, logical)
	}
	state.totalBytes += uint64(opened.Size())
	root.entries = append(root.entries, ManifestEntry{Kind: EntryFile, LogicalPath: logical,
		Mode: uint32(before.Mode().Perm()), SizeBytes: uint64(opened.Size()), Blocks: blocks})
	return nil
}

func (state *captureState) addEntry() error {
	if state.entryCount >= state.capturer.limits.maxEntries {
		return fmt.Errorf("%w: entry count exceeds %d", ErrArtifactLimit, state.capturer.limits.maxEntries)
	}
	state.entryCount++
	return nil
}

func (state *captureState) callAfterOpen(logical string) {
	if state.capturer.afterOpen != nil {
		state.capturer.afterOpen(filepath.Join(state.capturer.workspace, filepath.FromSlash(logical)))
	}
}

func (state *captureState) closure() (Closure, error) {
	sort.Slice(state.roots, func(left, right int) bool {
		return state.roots[left].RootDigest.String() < state.roots[right].RootDigest.String()
	})
	blocks := make([]CapturedBlock, 0, len(state.blocks))
	for _, block := range state.blocks {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(left, right int) bool {
		return blocks[left].Digest.String() < blocks[right].Digest.String()
	})
	sort.Slice(state.blockMap, func(left, right int) bool {
		if state.blockMap[left].RootDigest == state.blockMap[right].RootDigest {
			return state.blockMap[left].Ordinal < state.blockMap[right].Ordinal
		}
		return state.blockMap[left].RootDigest.String() < state.blockMap[right].RootDigest.String()
	})
	type checkpointRoot struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}
	checkpointRows := make([]checkpointRoot, len(state.roots))
	for index, root := range state.roots {
		checkpointRows[index] = checkpointRoot{root.ManifestDigest, root.RootDigest}
	}
	checkpoint, err := model.JSONFrom(struct {
		Roots []checkpointRoot `json:"roots"`
	}{checkpointRows})
	if err != nil {
		return Closure{}, fmt.Errorf("build Artifact capture checkpoint: %w", err)
	}
	return Closure{roots: append([]CapturedRoot{}, state.roots...), blocks: blocks,
		blockMap: append([]RootBlock{}, state.blockMap...), checkpoint: checkpoint,
		capturedAt: state.capturedAt}, nil
}

func (capturer *Capturer) openWorkspaceRoot() (*os.Root, error) {
	workspaceRoot, err := os.OpenRoot(capturer.workspace)
	if err != nil {
		return nil, fmt.Errorf("%w: open workspace Root: %v", ErrArtifactChanged, err)
	}
	if err := capturer.verifyOpenWorkspaceRoot(workspaceRoot); err != nil {
		_ = workspaceRoot.Close()
		return nil, err
	}
	return workspaceRoot, nil
}

func (capturer *Capturer) verifyOpenWorkspaceRoot(workspaceRoot *os.Root) error {
	if capturer == nil || capturer.workspaceInfo == nil || workspaceRoot == nil {
		return fmt.Errorf("%w: missing open workspace identity", ErrArtifactChanged)
	}
	opened, rootErr := workspaceRoot.Stat(".")
	pathInfo, pathErr := os.Lstat(capturer.workspace)
	if rootErr != nil || pathErr != nil || !opened.IsDir() || !pathInfo.IsDir() ||
		pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(capturer.workspaceInfo, opened) ||
		!os.SameFile(capturer.workspaceInfo, pathInfo) {
		return fmt.Errorf("%w: open workspace Root differs from configured directory", ErrArtifactChanged)
	}
	return nil
}

// verifyWorkspace pins the configured path to the directory identity observed
// by NewCapturer while still allowing ordinary workspace content changes.
// A workspace path replaced by a symlink or another directory is never used as
// a new capture authority.
func (capturer *Capturer) verifyWorkspace() error {
	if capturer == nil || capturer.workspaceInfo == nil {
		return fmt.Errorf("%w: missing workspace identity", ErrArtifactChanged)
	}
	before, err := os.Lstat(capturer.workspace)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(capturer.workspaceInfo, before) {
		return fmt.Errorf("%w: workspace path identity changed", ErrArtifactChanged)
	}
	directory, err := os.Open(capturer.workspace)
	if err != nil {
		return fmt.Errorf("%w: open workspace identity: %v", ErrArtifactChanged, err)
	}
	opened, statErr := directory.Stat()
	after, pathErr := os.Lstat(capturer.workspace)
	closeErr := directory.Close()
	if statErr != nil || pathErr != nil || closeErr != nil || !opened.IsDir() ||
		!os.SameFile(before, opened) || !os.SameFile(before, after) ||
		before.Mode()&os.ModeSymlink != 0 || after.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: workspace changed while verifying", ErrArtifactChanged)
	}
	return nil
}

func normalizeRequestedRoot(raw string) (string, error) {
	if raw == "" || !utf8.ValidString(raw) || strings.ContainsRune(raw, 0) || filepath.IsAbs(raw) {
		return "", fmt.Errorf("%w: root must be nonempty relative UTF-8", ErrArtifactPath)
	}
	for _, component := range strings.Split(filepath.ToSlash(raw), "/") {
		if component == ".." || component == "" {
			return "", fmt.Errorf("%w: root contains traversal or an empty component", ErrArtifactPath)
		}
	}
	clean := filepath.ToSlash(filepath.Clean(raw))
	for strings.HasPrefix(clean, "./") {
		clean = strings.TrimPrefix(clean, "./")
	}
	return validateLogicalPath(clean)
}

func canonicalCaptureTime(value time.Time) (time.Time, error) {
	value = value.Round(0).UTC()
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 ||
		!time.Unix(0, value.UnixNano()).UTC().Equal(value) {
		return time.Time{}, errors.New("Artifact capture clock returned an unsupported time")
	}
	return value, nil
}

func sameFileSnapshot(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.Mode() == right.Mode() &&
		left.Size() == right.Size() && left.ModTime().Equal(right.ModTime())
}

// VerifyClosure re-parses every manifest and re-hashes every reachable CAS
// object. Orphan temps and unrelated immutable blocks are intentionally ignored.
func (cas *CAS) VerifyClosure(ctx context.Context, closure Closure) error {
	if cas == nil {
		return fmt.Errorf("%w: unavailable CAS", ErrClosureMismatch)
	}
	return verifyClosureWithReader(ctx, closure, cas.Read)
}

type closureObjectReader func(model.Digest, int) ([]byte, error)

func verifyClosureWithReader(ctx context.Context, closure Closure, read closureObjectReader) error {
	if read == nil {
		return fmt.Errorf("%w: unavailable object reader", ErrClosureMismatch)
	}
	if err := validateClosureMetadata(ctx, closure); err != nil {
		return err
	}
	for _, root := range closure.roots {
		if err := ctx.Err(); err != nil {
			return err
		}
		storedManifest, err := read(root.ManifestDigest, MaxManifestBytes)
		if err != nil || !bytes.Equal(storedManifest, root.Manifest.Bytes()) {
			return fmt.Errorf("%w: manifest object is missing or changed", ErrClosureMismatch)
		}
	}
	for _, block := range closure.blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		content, err := read(block.Digest, BlockSize)
		if err != nil || uint64(len(content)) != block.SizeBytes {
			return fmt.Errorf("%w: block object is missing or changed", ErrClosureMismatch)
		}
	}
	return nil
}
