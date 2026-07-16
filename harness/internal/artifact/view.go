package artifact

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	viewControlDirectoryMode = 0o700
	viewDirectoryMode        = 0o500
	viewFileMode             = 0o400
	viewReceiptLimit         = 4096
)

var (
	ErrViewInput      = errors.New("invalid readonly Artifact view input")
	ErrViewConflict   = errors.New("readonly Artifact view conflicts with existing root")
	ErrViewCorruption = errors.New("readonly Artifact view or closure is corrupt")
	viewFilesystemMu  sync.Mutex
)

// ViewSpec contains only the server-resolved identity of one verified root.
// Pin and Run/claim authority remain Store responsibilities above this
// filesystem materialization boundary.
type ViewSpec struct {
	RunID          model.RunID
	Ordinal        uint32
	RootDigest     model.Digest
	ManifestDigest model.Digest
}

type MaterializedView struct {
	Directory      string
	Path           string
	RelativePath   string
	LogicalPath    string
	RootDigest     model.Digest
	ManifestDigest model.Digest
	Replayed       bool
}

// ViewMaterializer owns only <node-state>/views. It never treats materialized
// bytes as authority: every create and replay reads and hashes the manifest
// and every reachable block through CAS again.
type ViewMaterializer struct {
	nodeState     string
	nodeInfo      os.FileInfo
	cas           *CAS
	beforePublish func()
	afterPending  func()
}

func NewViewMaterializer(nodeState string, cas *CAS) (*ViewMaterializer, error) {
	if cas == nil || nodeState == "" || !filepath.IsAbs(nodeState) || filepath.Clean(nodeState) != nodeState {
		return nil, fmt.Errorf("%w: Node state must be an absolute canonical path", ErrViewInput)
	}
	info, err := os.Lstat(nodeState)
	if err != nil || !realViewDirectory(info, viewControlDirectoryMode) {
		return nil, fmt.Errorf("%w: Node state must be an owner-only real directory", ErrViewInput)
	}
	opened, err := os.Open(nodeState)
	if err != nil {
		return nil, fmt.Errorf("open readonly Artifact Node state: %w", err)
	}
	openedInfo, statErr := opened.Stat()
	closeErr := opened.Close()
	if statErr != nil || closeErr != nil || !sameViewDirectoryIdentity(info, openedInfo) {
		return nil, fmt.Errorf("%w: Node state changed while opening", ErrViewCorruption)
	}
	return &ViewMaterializer{nodeState: nodeState, nodeInfo: info, cas: cas}, nil
}

func (materializer *ViewMaterializer) Materialize(ctx context.Context,
	spec ViewSpec,
) (MaterializedView, error) {
	if materializer == nil || materializer.cas == nil || ctx == nil || spec.RunID.IsZero() ||
		spec.RootDigest.IsZero() || spec.ManifestDigest.IsZero() || !safeViewRunComponent(spec.RunID.String()) {
		return MaterializedView{}, fmt.Errorf("%w: complete Run and root authority are required", ErrViewInput)
	}
	if err := ctx.Err(); err != nil {
		return MaterializedView{}, err
	}
	manifest, err := materializer.readManifest(ctx, spec)
	if err != nil {
		return MaterializedView{}, err
	}
	tree, err := newMaterializedTree(manifest)
	if err != nil {
		return MaterializedView{}, err
	}

	viewFilesystemMu.Lock()
	defer viewFilesystemMu.Unlock()
	if err := ctx.Err(); err != nil {
		return MaterializedView{}, err
	}
	nodeRoot, err := materializer.openNodeRoot()
	if err != nil {
		return MaterializedView{}, err
	}
	defer nodeRoot.Close()
	viewsRoot, _, err := ensureViewDirectory(nodeRoot, "views", viewControlDirectoryMode)
	if err != nil {
		return MaterializedView{}, err
	}
	defer viewsRoot.Close()
	runName := spec.RunID.String()
	runRoot, _, err := ensureViewDirectory(viewsRoot, runName, viewControlDirectoryMode)
	if err != nil {
		return MaterializedView{}, err
	}
	defer runRoot.Close()
	if err := cleanViewStages(ctx, runRoot); err != nil {
		return MaterializedView{}, err
	}

	ordinalName := strconv.FormatUint(uint64(spec.Ordinal), 10)
	receiptName := viewReceiptName(ordinalName)
	pendingName := viewPendingReceiptName(ordinalName)
	finalExists, err := rootEntryExists(runRoot, ordinalName)
	if err != nil {
		return MaterializedView{}, err
	}
	receiptExists, err := rootEntryExists(runRoot, receiptName)
	if err != nil {
		return MaterializedView{}, err
	}
	pendingExists, err := rootEntryExists(runRoot, pendingName)
	if err != nil {
		return MaterializedView{}, err
	}
	if pendingExists {
		pending, err := readViewReceipt(runRoot, pendingName)
		if err != nil {
			return MaterializedView{}, err
		}
		if !pending.matches(spec, manifest) {
			return MaterializedView{}, fmt.Errorf("%w: incomplete ordinal belongs to another root",
				ErrViewConflict)
		}
		if receiptExists {
			ready, err := readViewReceipt(runRoot, receiptName)
			if err != nil || !ready.matches(spec, manifest) {
				return MaterializedView{}, fmt.Errorf("%w: ready and pending view receipts differ",
					ErrViewCorruption)
			}
		} else if finalExists {
			if err := removeRootEntry(ctx, runRoot, ordinalName); err != nil {
				return MaterializedView{}, err
			}
			finalExists = false
		}
		if err := runRoot.Remove(pendingName); err != nil {
			return MaterializedView{}, fmt.Errorf("remove incomplete readonly view receipt: %w", err)
		}
		if err := syncViewRoot(runRoot); err != nil {
			return MaterializedView{}, err
		}
	}
	if finalExists != receiptExists {
		return MaterializedView{}, fmt.Errorf("%w: ordinal directory lacks its root receipt", ErrViewCorruption)
	}
	if finalExists {
		receipt, err := readViewReceipt(runRoot, receiptName)
		if err != nil {
			return MaterializedView{}, err
		}
		if !receipt.matches(spec, manifest) {
			return MaterializedView{}, fmt.Errorf("%w: Run ordinal already belongs to another root",
				ErrViewConflict)
		}
		if err := materializer.verifyView(ctx, runRoot, ordinalName, tree); err != nil {
			return MaterializedView{}, err
		}
		return materializer.viewResult(spec, manifest, ordinalName, true), nil
	}

	stageName, stageRoot, err := newViewStage(runRoot, ordinalName)
	if err != nil {
		return MaterializedView{}, err
	}
	stageOpen := true
	stageOwned := true
	defer func() {
		if stageOpen {
			_ = stageRoot.Close()
		}
		if stageOwned {
			_ = removeRootEntry(context.Background(), runRoot, stageName)
			_ = syncViewRoot(runRoot)
		}
	}()
	if err := materializer.buildView(ctx, stageRoot, tree); err != nil {
		return MaterializedView{}, err
	}
	if materializer.beforePublish != nil {
		materializer.beforePublish()
	}
	if err := ctx.Err(); err != nil {
		return MaterializedView{}, err
	}
	if err := stageRoot.Close(); err != nil {
		return MaterializedView{}, fmt.Errorf("close readonly Artifact staging Root: %w", err)
	}
	stageOpen = false

	receipt, err := newViewReceipt(spec, manifest)
	if err != nil {
		return MaterializedView{}, err
	}
	receiptTemp, err := writeViewReceiptTemp(runRoot, receipt, ordinalName)
	if err != nil {
		return MaterializedView{}, err
	}
	receiptTempOwned := true
	defer func() {
		if receiptTempOwned {
			_ = runRoot.Remove(receiptTemp)
		}
	}()
	runPath := filepath.Join(materializer.nodeState, "views", runName)
	pendingPath := filepath.Join(runPath, pendingName)
	receiptPath := filepath.Join(runPath, receiptName)
	if err := os.Link(filepath.Join(runPath, receiptTemp), pendingPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return MaterializedView{}, fmt.Errorf("%w: ordinal publication raced another writer", ErrViewConflict)
		}
		return MaterializedView{}, fmt.Errorf("publish pending readonly Artifact view receipt: %w", err)
	}
	pendingOwned := true
	defer func() {
		if pendingOwned {
			_ = runRoot.Remove(pendingName)
			_ = syncViewRoot(runRoot)
		}
	}()
	if err := syncViewRoot(runRoot); err != nil {
		return MaterializedView{}, err
	}
	if err := runRoot.Remove(receiptTemp); err != nil {
		return MaterializedView{}, fmt.Errorf("remove readonly Artifact receipt temp: %w", err)
	}
	receiptTempOwned = false
	if err := syncViewRoot(runRoot); err != nil {
		return MaterializedView{}, err
	}
	if materializer.afterPending != nil {
		materializer.afterPending()
	}
	if err := ctx.Err(); err != nil {
		return MaterializedView{}, err
	}
	finalOwned := false
	defer func() {
		if finalOwned {
			_ = removeRootEntry(context.Background(), runRoot, ordinalName)
			_ = syncViewRoot(runRoot)
		}
	}()
	if err := os.Rename(filepath.Join(runPath, stageName), filepath.Join(runPath, ordinalName)); err != nil {
		return MaterializedView{}, fmt.Errorf("atomically publish readonly Artifact view: %w", err)
	}
	stageOwned = false
	finalOwned = true
	publishedRoot, err := openViewDirectory(runRoot, ordinalName, viewControlDirectoryMode)
	if err != nil {
		return MaterializedView{}, err
	}
	publishedDirectory, err := publishedRoot.Open(".")
	if err != nil {
		publishedRoot.Close()
		return MaterializedView{}, fmt.Errorf("open published readonly Artifact directory: %w", err)
	}
	if err := publishedDirectory.Chmod(viewDirectoryMode); err != nil {
		publishedDirectory.Close()
		publishedRoot.Close()
		return MaterializedView{}, fmt.Errorf("protect published readonly Artifact directory: %w", err)
	}
	if err := publishedDirectory.Sync(); err != nil {
		publishedDirectory.Close()
		publishedRoot.Close()
		return MaterializedView{}, fmt.Errorf("fsync published readonly Artifact directory: %w", err)
	}
	if err := publishedDirectory.Close(); err != nil {
		publishedRoot.Close()
		return MaterializedView{}, fmt.Errorf("close published readonly Artifact directory: %w", err)
	}
	if err := publishedRoot.Close(); err != nil {
		return MaterializedView{}, fmt.Errorf("close published readonly Artifact Root: %w", err)
	}
	if err := syncViewRoot(runRoot); err != nil {
		return MaterializedView{}, err
	}
	if err := materializer.verifyView(ctx, runRoot, ordinalName, tree); err != nil {
		return MaterializedView{}, err
	}
	if err := os.Link(pendingPath, receiptPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return MaterializedView{}, fmt.Errorf("%w: ready receipt raced another writer", ErrViewConflict)
		}
		return MaterializedView{}, fmt.Errorf("publish ready readonly Artifact view receipt: %w", err)
	}
	receiptOwned := true
	defer func() {
		if receiptOwned {
			_ = runRoot.Remove(receiptName)
			_ = syncViewRoot(runRoot)
		}
	}()
	if err := syncViewRoot(runRoot); err != nil {
		return MaterializedView{}, err
	}
	if err := runRoot.Remove(pendingName); err != nil {
		return MaterializedView{}, fmt.Errorf("remove completed readonly view pending receipt: %w", err)
	}
	pendingOwned = false
	if err := syncViewRoot(runRoot); err != nil {
		return MaterializedView{}, err
	}
	receiptOwned = false
	finalOwned = false
	return materializer.viewResult(spec, manifest, ordinalName, false), nil
}

// CleanupRun removes exactly one validated Run component beneath views. It
// never follows a symlink and never touches CAS, pins, or provenance.
func (materializer *ViewMaterializer) CleanupRun(ctx context.Context, runID model.RunID) error {
	if materializer == nil || materializer.cas == nil || ctx == nil || runID.IsZero() ||
		!safeViewRunComponent(runID.String()) {
		return fmt.Errorf("%w: valid Run identity is required for cleanup", ErrViewInput)
	}
	viewFilesystemMu.Lock()
	defer viewFilesystemMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	nodeRoot, err := materializer.openNodeRoot()
	if err != nil {
		return err
	}
	defer nodeRoot.Close()
	viewsInfo, err := nodeRoot.Lstat("views")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !realViewDirectory(viewsInfo, viewControlDirectoryMode) {
		return fmt.Errorf("%w: views control directory is invalid", ErrViewCorruption)
	}
	viewsRoot, err := nodeRoot.OpenRoot("views")
	if err != nil {
		return fmt.Errorf("open readonly Artifact views: %w", err)
	}
	defer viewsRoot.Close()
	exists, err := rootEntryExists(viewsRoot, runID.String())
	if err != nil || !exists {
		return err
	}
	if err := removeRootEntry(ctx, viewsRoot, runID.String()); err != nil {
		return err
	}
	return syncViewRoot(viewsRoot)
}

func (materializer *ViewMaterializer) readManifest(ctx context.Context,
	spec ViewSpec,
) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	raw, err := materializer.cas.Read(spec.ManifestDigest, MaxManifestBytes)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: manifest CAS read failed: %v", ErrViewCorruption, err)
	}
	manifest, err := ParseManifest(raw)
	if err != nil || manifest.ManifestDigest() != spec.ManifestDigest ||
		manifest.RootDigest() != spec.RootDigest {
		return Manifest{}, fmt.Errorf("%w: manifest does not match verified root authority", ErrViewCorruption)
	}
	return manifest, nil
}

func (materializer *ViewMaterializer) openNodeRoot() (*os.Root, error) {
	before, err := os.Lstat(materializer.nodeState)
	if err != nil || !sameViewDirectoryIdentity(materializer.nodeInfo, before) ||
		!realViewDirectory(before, viewControlDirectoryMode) {
		return nil, fmt.Errorf("%w: Node state identity or mode changed", ErrViewCorruption)
	}
	root, err := os.OpenRoot(materializer.nodeState)
	if err != nil {
		return nil, fmt.Errorf("open readonly Artifact Node Root: %w", err)
	}
	opened, rootErr := root.Stat(".")
	after, pathErr := os.Lstat(materializer.nodeState)
	if rootErr != nil || pathErr != nil || !sameViewDirectoryIdentity(before, opened) ||
		!sameViewDirectoryIdentity(before, after) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: Node state changed while opening", ErrViewCorruption)
	}
	return root, nil
}

func (materializer *ViewMaterializer) buildView(ctx context.Context, root *os.Root,
	tree materializedTree,
) error {
	directories := append([]string(nil), tree.directories...)
	sort.Slice(directories, func(left, right int) bool {
		return viewPathDepth(directories[left]) < viewPathDepth(directories[right]) ||
			(viewPathDepth(directories[left]) == viewPathDepth(directories[right]) &&
				directories[left] < directories[right])
	})
	for _, directory := range directories {
		if directory == "." {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := root.Mkdir(directory, viewControlDirectoryMode); err != nil {
			return fmt.Errorf("create readonly Artifact directory %q: %w", directory, err)
		}
	}
	for _, entry := range tree.files {
		if err := materializer.writeViewFile(ctx, root, entry); err != nil {
			return err
		}
	}
	sort.Slice(directories, func(left, right int) bool {
		return viewPathDepth(directories[left]) > viewPathDepth(directories[right]) ||
			(viewPathDepth(directories[left]) == viewPathDepth(directories[right]) &&
				directories[left] > directories[right])
	})
	for _, directory := range directories {
		// Darwin refuses to rename a directory after its own write bit is
		// removed. Keep only the staging wrapper at 0700; every descendant is
		// already readonly and the wrapper is changed to 0500 immediately after
		// the atomic rename, before fsync or return.
		if directory == "." {
			continue
		}
		file, err := root.Open(directory)
		if err != nil {
			return fmt.Errorf("open readonly Artifact directory %q: %w", directory, err)
		}
		if err := file.Chmod(viewDirectoryMode); err != nil {
			file.Close()
			return fmt.Errorf("protect readonly Artifact directory %q: %w", directory, err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return fmt.Errorf("fsync readonly Artifact directory %q: %w", directory, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close readonly Artifact directory %q: %w", directory, err)
		}
	}
	return syncViewRoot(root)
}

func (materializer *ViewMaterializer) writeViewFile(ctx context.Context, root *os.Root,
	entry materializedFile,
) error {
	file, err := root.OpenFile(entry.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create readonly Artifact file %q: %w", entry.path, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	var written uint64
	for _, block := range entry.entry.Blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		content, err := materializer.cas.Read(block.Digest, int(block.LengthBytes))
		if err != nil || uint64(len(content)) != block.LengthBytes || block.OffsetBytes != written {
			return fmt.Errorf("%w: block for %q is missing, changed, or misplaced",
				ErrViewCorruption, entry.path)
		}
		if err := writeFull(file, content); err != nil {
			return fmt.Errorf("write readonly Artifact file %q: %w", entry.path, err)
		}
		written += uint64(len(content))
	}
	if written != entry.entry.SizeBytes {
		return fmt.Errorf("%w: reconstructed file %q has wrong size", ErrViewCorruption, entry.path)
	}
	if err := file.Chmod(viewFileMode); err != nil {
		return fmt.Errorf("protect readonly Artifact file %q: %w", entry.path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("fsync readonly Artifact file %q: %w", entry.path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close readonly Artifact file %q: %w", entry.path, err)
	}
	closed = true
	return nil
}

func (materializer *ViewMaterializer) verifyView(ctx context.Context, runRoot *os.Root,
	ordinal string, tree materializedTree,
) error {
	viewRoot, err := openExistingView(runRoot, ordinal)
	if err != nil {
		return err
	}
	defer viewRoot.Close()
	actual := make(map[string]EntryKind, len(tree.kinds))
	if err := walkMaterializedView(ctx, viewRoot, ".", actual); err != nil {
		return err
	}
	if len(actual) != len(tree.kinds) {
		return fmt.Errorf("%w: readonly view contains missing or extra paths", ErrViewCorruption)
	}
	for path, kind := range tree.kinds {
		if actual[path] != kind {
			return fmt.Errorf("%w: readonly view path %q changed kind", ErrViewCorruption, path)
		}
	}
	for _, entry := range tree.files {
		if err := materializer.verifyViewFile(ctx, viewRoot, entry); err != nil {
			return err
		}
	}
	return nil
}

func (materializer *ViewMaterializer) verifyViewFile(ctx context.Context, root *os.Root,
	entry materializedFile,
) error {
	before, err := root.Lstat(entry.path)
	if err != nil || !regularViewFile(before) || uint64(before.Size()) != entry.entry.SizeBytes {
		return fmt.Errorf("%w: readonly file %q identity or mode changed", ErrViewCorruption, entry.path)
	}
	file, err := root.Open(entry.path)
	if err != nil {
		return fmt.Errorf("%w: open readonly file %q: %v", ErrViewCorruption, entry.path, err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !sameViewFileSnapshot(before, opened) {
		return fmt.Errorf("%w: readonly file %q changed while opening", ErrViewCorruption, entry.path)
	}
	for _, block := range entry.entry.Blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		expected, err := materializer.cas.Read(block.Digest, int(block.LengthBytes))
		if err != nil || uint64(len(expected)) != block.LengthBytes {
			return fmt.Errorf("%w: CAS block for %q changed", ErrViewCorruption, entry.path)
		}
		actual := make([]byte, len(expected))
		if _, err := io.ReadFull(file, actual); err != nil || !bytes.Equal(actual, expected) {
			return fmt.Errorf("%w: readonly file %q bytes changed", ErrViewCorruption, entry.path)
		}
	}
	var tail [1]byte
	if count, err := file.Read(tail[:]); (err != nil && !errors.Is(err, io.EOF)) || count != 0 {
		return fmt.Errorf("%w: readonly file %q has trailing bytes", ErrViewCorruption, entry.path)
	}
	afterFD, fdErr := file.Stat()
	afterPath, pathErr := root.Lstat(entry.path)
	if fdErr != nil || pathErr != nil || !sameViewFileSnapshot(before, afterFD) ||
		!sameViewFileSnapshot(before, afterPath) {
		return fmt.Errorf("%w: readonly file %q changed during verification", ErrViewCorruption, entry.path)
	}
	return nil
}

func (materializer *ViewMaterializer) viewResult(spec ViewSpec, manifest Manifest,
	ordinal string, replayed bool,
) MaterializedView {
	directory := filepath.Join(materializer.nodeState, "views", spec.RunID.String(), ordinal)
	rootPath := directory
	if manifest.RootPath() != "." {
		rootPath = filepath.Join(directory, filepath.FromSlash(manifest.RootPath()))
	}
	relative := filepath.ToSlash(filepath.Join("views", spec.RunID.String(), ordinal))
	if manifest.RootPath() != "." {
		relative += "/" + manifest.RootPath()
	}
	return MaterializedView{Directory: directory, Path: rootPath, RelativePath: relative,
		LogicalPath: manifest.RootPath(), RootDigest: spec.RootDigest,
		ManifestDigest: spec.ManifestDigest, Replayed: replayed}
}

type materializedFile struct {
	path  string
	entry ManifestEntry
}

type materializedTree struct {
	directories []string
	files       []materializedFile
	kinds       map[string]EntryKind
}

func newMaterializedTree(manifest Manifest) (materializedTree, error) {
	if manifest.IsZero() || (manifest.RootKind() == EntryFile && manifest.RootPath() == ".") {
		return materializedTree{}, fmt.Errorf("%w: manifest root cannot be materialized", ErrViewCorruption)
	}
	kinds := map[string]EntryKind{".": EntryDirectory}
	files := make([]materializedFile, 0)
	for _, entry := range manifest.Entries() {
		relative, err := viewRelativePath(entry.LogicalPath)
		if err != nil {
			return materializedTree{}, err
		}
		if entry.Kind == EntryDirectory {
			kinds[relative] = EntryDirectory
		} else {
			kinds[relative] = EntryFile
			files = append(files, materializedFile{path: relative, entry: entry})
		}
		for parent := filepath.Dir(relative); parent != "."; parent = filepath.Dir(parent) {
			if kind, exists := kinds[parent]; exists && kind != EntryDirectory {
				return materializedTree{}, fmt.Errorf("%w: file blocks a materialized parent", ErrViewCorruption)
			}
			kinds[parent] = EntryDirectory
		}
	}
	directories := make([]string, 0)
	for relative, kind := range kinds {
		if kind == EntryDirectory {
			directories = append(directories, relative)
		}
	}
	sort.Strings(directories)
	sort.Slice(files, func(left, right int) bool { return files[left].path < files[right].path })
	return materializedTree{directories: directories, files: files, kinds: kinds}, nil
}

func viewRelativePath(logical string) (string, error) {
	validated, err := validateLogicalPath(logical)
	if err != nil || validated != logical {
		return "", fmt.Errorf("%w: manifest path is not canonical", ErrViewCorruption)
	}
	relative := filepath.Clean(filepath.FromSlash(logical))
	if relative == "" || filepath.IsAbs(relative) || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.ToSlash(relative) != logical {
		return "", fmt.Errorf("%w: manifest path escapes materialized root", ErrViewCorruption)
	}
	return relative, nil
}

type viewReceipt struct {
	runID          model.RunID
	ordinal        uint32
	rootDigest     model.Digest
	manifestDigest model.Digest
	rootKind       EntryKind
	rootPath       string
	canonical      model.JSON
}

type viewReceiptWire struct {
	ManifestDigest string    `json:"manifest_digest"`
	Ordinal        uint32    `json:"ordinal"`
	RootDigest     string    `json:"root_digest"`
	RootKind       EntryKind `json:"root_kind"`
	RootPath       string    `json:"root_path"`
	RunID          string    `json:"run_id"`
	SchemaVersion  int       `json:"schema_version"`
}

func newViewReceipt(spec ViewSpec, manifest Manifest) (viewReceipt, error) {
	wire := viewReceiptWire{ManifestDigest: spec.ManifestDigest.String(), Ordinal: spec.Ordinal,
		RootDigest: spec.RootDigest.String(), RootKind: manifest.RootKind(), RootPath: manifest.RootPath(),
		RunID: spec.RunID.String(), SchemaVersion: SchemaVersion}
	canonical, err := model.JSONFrom(wire)
	if err != nil || len(canonical.Bytes()) > viewReceiptLimit {
		return viewReceipt{}, fmt.Errorf("%w: view receipt cannot be encoded", ErrViewCorruption)
	}
	return viewReceipt{runID: spec.RunID, ordinal: spec.Ordinal, rootDigest: spec.RootDigest,
		manifestDigest: spec.ManifestDigest, rootKind: manifest.RootKind(), rootPath: manifest.RootPath(),
		canonical: canonical}, nil
}

func (receipt viewReceipt) matches(spec ViewSpec, manifest Manifest) bool {
	return receipt.runID == spec.RunID && receipt.ordinal == spec.Ordinal &&
		receipt.rootDigest == spec.RootDigest && receipt.manifestDigest == spec.ManifestDigest &&
		receipt.rootKind == manifest.RootKind() && receipt.rootPath == manifest.RootPath()
}

func readViewReceipt(root *os.Root, name string) (viewReceipt, error) {
	before, err := root.Lstat(name)
	if err != nil || !regularViewFile(before) || before.Size() <= 0 || before.Size() > viewReceiptLimit {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt identity is invalid", ErrViewCorruption)
	}
	file, err := root.Open(name)
	if err != nil {
		return viewReceipt{}, fmt.Errorf("%w: open readonly view receipt: %v", ErrViewCorruption, err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, viewReceiptLimit+1))
	opened, statErr := file.Stat()
	closeErr := file.Close()
	after, pathErr := root.Lstat(name)
	if readErr != nil || statErr != nil || closeErr != nil || pathErr != nil || len(raw) > viewReceiptLimit ||
		!sameViewFileSnapshot(before, opened) || !sameViewFileSnapshot(before, after) {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt changed while reading", ErrViewCorruption)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt is not canonical", ErrViewCorruption)
	}
	var wire viewReceiptWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || wire.SchemaVersion != SchemaVersion {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt schema is invalid", ErrViewCorruption)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt has trailing data", ErrViewCorruption)
	}
	runID, runErr := model.ParseRunID(wire.RunID)
	rootDigest, rootErr := model.ParseDigest(wire.RootDigest)
	manifestDigest, manifestErr := model.ParseDigest(wire.ManifestDigest)
	if runErr != nil || rootErr != nil || manifestErr != nil || !safeViewRunComponent(wire.RunID) ||
		!wire.RootKind.Valid() {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt authority is invalid", ErrViewCorruption)
	}
	rebuilt, err := model.JSONFrom(wire)
	if err != nil || rebuilt.String() != canonical.String() {
		return viewReceipt{}, fmt.Errorf("%w: readonly view receipt projection changed", ErrViewCorruption)
	}
	return viewReceipt{runID: runID, ordinal: wire.Ordinal, rootDigest: rootDigest,
		manifestDigest: manifestDigest, rootKind: wire.RootKind, rootPath: wire.RootPath,
		canonical: canonical}, nil
}

func writeViewReceiptTemp(root *os.Root, receipt viewReceipt, ordinal string) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		name, err := randomViewName(".meta-stage-" + ordinal + "-")
		if err != nil {
			return "", err
		}
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("create readonly Artifact receipt temp: %w", err)
		}
		if err := writeFull(file, receipt.canonical.Bytes()); err != nil {
			file.Close()
			root.Remove(name)
			return "", fmt.Errorf("write readonly Artifact receipt temp: %w", err)
		}
		if err := file.Chmod(viewFileMode); err != nil {
			file.Close()
			root.Remove(name)
			return "", fmt.Errorf("protect readonly Artifact receipt temp: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			root.Remove(name)
			return "", fmt.Errorf("fsync readonly Artifact receipt temp: %w", err)
		}
		if err := file.Close(); err != nil {
			root.Remove(name)
			return "", fmt.Errorf("close readonly Artifact receipt temp: %w", err)
		}
		return name, nil
	}
	return "", errors.New("allocate readonly Artifact receipt temp: collision budget exhausted")
}

func newViewStage(runRoot *os.Root, ordinal string) (string, *os.Root, error) {
	for attempt := 0; attempt < 8; attempt++ {
		name, err := randomViewName(".stage-" + ordinal + "-")
		if err != nil {
			return "", nil, err
		}
		if err := runRoot.Mkdir(name, viewControlDirectoryMode); errors.Is(err, os.ErrExist) {
			continue
		} else if err != nil {
			return "", nil, fmt.Errorf("create readonly Artifact staging directory: %w", err)
		}
		root, err := runRoot.OpenRoot(name)
		if err != nil {
			runRoot.Remove(name)
			return "", nil, fmt.Errorf("open readonly Artifact staging Root: %w", err)
		}
		if err := syncViewRoot(runRoot); err != nil {
			root.Close()
			removeRootEntry(context.Background(), runRoot, name)
			return "", nil, err
		}
		return name, root, nil
	}
	return "", nil, errors.New("allocate readonly Artifact staging directory: collision budget exhausted")
}

func cleanViewStages(ctx context.Context, root *os.Root) error {
	names, err := viewRootNames(root)
	if err != nil {
		return err
	}
	for _, name := range names {
		if strings.HasPrefix(name, ".stage-") || strings.HasPrefix(name, ".meta-stage-") {
			if err := removeRootEntry(ctx, root, name); err != nil {
				return err
			}
		}
	}
	return syncViewRoot(root)
}

func ensureViewDirectory(parent *os.Root, name string, mode os.FileMode) (*os.Root, bool, error) {
	created := false
	info, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		if err := parent.Mkdir(name, mode); err != nil {
			return nil, false, fmt.Errorf("create readonly Artifact control directory %q: %w", name, err)
		}
		created = true
		if err := syncViewRoot(parent); err != nil {
			return nil, false, err
		}
		info, err = parent.Lstat(name)
	}
	if err != nil || !realViewDirectory(info, mode) {
		return nil, false, fmt.Errorf("%w: control path %q is not a real mode %04o directory",
			ErrViewCorruption, name, mode)
	}
	root, err := openViewDirectory(parent, name, mode)
	return root, created, err
}

func openViewDirectory(parent *os.Root, name string, mode os.FileMode) (*os.Root, error) {
	info, err := parent.Lstat(name)
	if err != nil || !realViewDirectory(info, mode) {
		return nil, fmt.Errorf("%w: path %q is not a real mode %04o directory",
			ErrViewCorruption, name, mode)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("open readonly Artifact directory %q: %w", name, err)
	}
	opened, rootErr := root.Stat(".")
	after, pathErr := parent.Lstat(name)
	if rootErr != nil || pathErr != nil || !sameViewDirectoryIdentity(info, opened) ||
		!sameViewDirectoryIdentity(info, after) {
		root.Close()
		return nil, fmt.Errorf("%w: directory %q changed while opening",
			ErrViewCorruption, name)
	}
	return root, nil
}

func openExistingView(parent *os.Root, name string) (*os.Root, error) {
	info, err := parent.Lstat(name)
	if err != nil || !realViewDirectory(info, viewDirectoryMode) {
		return nil, fmt.Errorf("%w: ordinal view is not a real readonly directory", ErrViewCorruption)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("open readonly Artifact ordinal: %w", err)
	}
	opened, rootErr := root.Stat(".")
	after, pathErr := parent.Lstat(name)
	if rootErr != nil || pathErr != nil || !sameViewDirectoryIdentity(info, opened) ||
		!sameViewDirectoryIdentity(info, after) {
		root.Close()
		return nil, fmt.Errorf("%w: ordinal view changed while opening", ErrViewCorruption)
	}
	return root, nil
}

func walkMaterializedView(ctx context.Context, root *os.Root, relative string,
	actual map[string]EntryKind,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := root.Lstat(relative)
	if err != nil || !realViewDirectory(info, viewDirectoryMode) {
		return fmt.Errorf("%w: readonly directory %q identity or mode changed",
			ErrViewCorruption, relative)
	}
	actual[relative] = EntryDirectory
	names, err := viewDirectoryNames(root, relative)
	if err != nil {
		return err
	}
	for _, name := range names {
		child := name
		if relative != "." {
			child = filepath.Join(relative, name)
		}
		childInfo, err := root.Lstat(child)
		if err != nil || childInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: readonly path %q is missing or a symlink", ErrViewCorruption, child)
		}
		switch {
		case childInfo.IsDir():
			if err := walkMaterializedView(ctx, root, child, actual); err != nil {
				return err
			}
		case childInfo.Mode().IsRegular() && childInfo.Mode().Perm() == viewFileMode:
			actual[child] = EntryFile
		default:
			return fmt.Errorf("%w: readonly path %q is not a protected file or directory",
				ErrViewCorruption, child)
		}
	}
	return nil
}

func viewDirectoryNames(root *os.Root, relative string) ([]string, error) {
	file, err := root.Open(relative)
	if err != nil {
		return nil, fmt.Errorf("open readonly Artifact directory %q: %w", relative, err)
	}
	names, readErr := file.Readdirnames(-1)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, fmt.Errorf("read readonly Artifact directory %q: %v", relative,
			errors.Join(readErr, closeErr))
	}
	sort.Strings(names)
	return names, nil
}

func viewRootNames(root *os.Root) ([]string, error) {
	return viewDirectoryNames(root, ".")
}

func removeRootEntry(ctx context.Context, parent *os.Root, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect readonly Artifact cleanup entry %q: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		if err := parent.Remove(name); err != nil {
			return fmt.Errorf("remove readonly Artifact cleanup entry %q: %w", name, err)
		}
		return nil
	}
	child, err := parent.OpenRoot(name)
	if err != nil {
		return fmt.Errorf("open readonly Artifact cleanup directory %q: %w", name, err)
	}
	opened, rootErr := child.Stat(".")
	after, pathErr := parent.Lstat(name)
	if rootErr != nil || pathErr != nil || !sameViewDirectoryIdentity(info, opened) ||
		!sameViewDirectoryIdentity(info, after) {
		child.Close()
		return fmt.Errorf("%w: cleanup directory %q changed while opening", ErrViewCorruption, name)
	}
	if err := clearViewRoot(ctx, child); err != nil {
		child.Close()
		return err
	}
	if err := child.Close(); err != nil {
		return fmt.Errorf("close readonly Artifact cleanup directory %q: %w", name, err)
	}
	if err := parent.Remove(name); err != nil {
		return fmt.Errorf("remove readonly Artifact cleanup directory %q: %w", name, err)
	}
	return nil
}

func clearViewRoot(ctx context.Context, root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open readonly Artifact cleanup Root: %w", err)
	}
	if err := directory.Chmod(viewControlDirectoryMode); err != nil {
		directory.Close()
		return fmt.Errorf("unlock readonly Artifact cleanup Root: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close readonly Artifact cleanup Root: %w", err)
	}
	names, err := viewRootNames(root)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := removeRootEntry(ctx, root, name); err != nil {
			return err
		}
	}
	return syncViewRoot(root)
}

func syncViewRoot(root *os.Root) error {
	file, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open readonly Artifact directory for fsync: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("fsync readonly Artifact directory: %w", err)
	}
	return nil
}

func rootEntryExists(root *os.Root, name string) (bool, error) {
	_, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect readonly Artifact path %q: %w", name, err)
	}
	return true, nil
}

func safeViewRunComponent(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value &&
		!strings.ContainsAny(value, `/\`) && !strings.HasPrefix(value, ".")
}

func randomViewName(prefix string) (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("allocate readonly Artifact staging identity: %w", err)
	}
	return prefix + hex.EncodeToString(random), nil
}

func viewReceiptName(ordinal string) string { return ".view-" + ordinal + ".json" }

func viewPendingReceiptName(ordinal string) string { return ".view-" + ordinal + ".pending" }

func viewPathDepth(value string) int {
	if value == "." {
		return 0
	}
	return strings.Count(filepath.Clean(value), string(filepath.Separator)) + 1
}

func realViewDirectory(info os.FileInfo, mode os.FileMode) bool {
	return info != nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == mode
}

func regularViewFile(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 &&
		info.Mode().Perm() == viewFileMode
}

func sameViewDirectoryIdentity(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.IsDir() && right.IsDir() &&
		left.Mode()&os.ModeSymlink == 0 && right.Mode()&os.ModeSymlink == 0
}

func sameViewFileSnapshot(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.Mode() == right.Mode() &&
		left.Mode().IsRegular() && left.Mode()&os.ModeSymlink == 0 && left.Mode().Perm() == viewFileMode &&
		left.Size() == right.Size() && left.ModTime().Equal(right.ModTime())
}
