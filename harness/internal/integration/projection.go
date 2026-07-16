package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

const (
	projectionSchemaVersion = 1
	maxHostConfigBytes      = 1 << 20
	projectionInstalling    = "installing"
	projectionApplied       = "applied"
	projectionUpgrading     = "upgrading"
	projectionPrevious      = "previous_applied"
)

type HostProjectionReceipt struct {
	ConfigPath    string
	Host          assets.Host
	OwnershipPath string
	Replayed      bool
	Revision      string
}

type ownershipManifest struct {
	Schema        int                     `json:"schema"`
	State         string                  `json:"state"`
	Host          assets.Host             `json:"host"`
	AssetRevision string                  `json:"asset_revision"`
	Files         []ownershipFile         `json:"files"`
	Registrations []ownershipRegistration `json:"registrations"`
	Previous      *ownershipPrevious      `json:"previous,omitempty"`
}

type ownershipPrevious struct {
	AssetRevision string                  `json:"asset_revision"`
	Files         []ownershipFile         `json:"files"`
	Registrations []ownershipRegistration `json:"registrations"`
}

type ownershipFile struct {
	Path            string `json:"path"`
	Source          string `json:"source"`
	InstalledDigest string `json:"installed_digest"`
	Mode            string `json:"mode"`
}

type ownershipRegistration struct {
	Path            string `json:"path"`
	ManagedKey      string `json:"managed_key"`
	InstalledDigest string `json:"installed_digest"`
}

type projectedFile struct {
	content []byte
	mode    os.FileMode
	path    string
	record  ownershipFile
}

type hostRegistrationHook struct {
	Command       string `json:"command"`
	StatusMessage string `json:"statusMessage"`
	Timeout       int    `json:"timeout"`
	Type          string `json:"type"`
}

type hostRegistrationEntry struct {
	Hooks []hostRegistrationHook `json:"hooks"`
}

type projectionPlan struct {
	configPath      string
	entry           hostRegistrationEntry
	entryDigest     string
	files           []projectedFile
	applied         ownershipManifest
	appliedBytes    []byte
	installing      ownershipManifest
	installingBytes []byte
	ownershipPath   string
	registration    assets.Registration
	workspace       string
}

type projectionOwnership struct {
	journalBytes []byte
	previous     *ownershipPrevious
	state        string
}

type projectionBoundary func(string) error

// InstallHostProjection installs the explicit Teamwork Skill, Guide and Hook
// in one Host's frozen project-local paths. The canonical Node bundle must
// already be installed and exact. Existing unowned files are never adopted,
// while a complete exact ownership manifest makes the operation a replay.
func InstallHostProjection(workspace, nodeState string, host assets.Host, bundle assets.Bundle) (HostProjectionReceipt, error) {
	return installHostProjection(workspace, nodeState, host, bundle, nil)
}

func installHostProjection(workspace, nodeState string, host assets.Host, bundle assets.Bundle, boundary projectionBoundary) (HostProjectionReceipt, error) {
	plan, err := prepareProjection(workspace, nodeState, host, bundle)
	if err != nil {
		return HostProjectionReceipt{}, err
	}
	receipt := HostProjectionReceipt{ConfigPath: plan.configPath, Host: host,
		OwnershipPath: plan.ownershipPath, Revision: plan.applied.AssetRevision}

	if _, err := os.Lstat(plan.ownershipPath); err == nil {
		ownership, stateErr := readProjectionOwnership(plan)
		if stateErr != nil {
			return HostProjectionReceipt{}, stateErr
		}
		if ownership.state == projectionApplied {
			if err := verifyProjection(plan); err == nil {
				receipt.Replayed = true
				return receipt, nil
			}
		}
		if ownership.state == projectionPrevious {
			ownership, err = beginProjectionUpgrade(plan, ownership, boundary)
			if err != nil {
				return HostProjectionReceipt{}, err
			}
		}
		if _, err := convergeProjection(plan, ownership, boundary); err != nil {
			return HostProjectionReceipt{}, err
		}
		return receipt, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return HostProjectionReceipt{}, projectionConflict("inspect ownership manifest", err)
	}

	if _, _, err := inspectFreshProjection(plan); err != nil {
		return HostProjectionReceipt{}, err
	}
	if err := createOwnershipDirectories(workspace, plan); err != nil {
		return HostProjectionReceipt{}, err
	}
	if err := publishExclusiveFile(filepath.Dir(plan.ownershipPath), plan.ownershipPath, plan.installingBytes, 0o600); err != nil {
		// A concurrent identical installer may have committed the desired
		// journal after the preflight. It is safe to join only after validating
		// that journal as the exact canonical plan.
		ownership, stateErr := readProjectionOwnership(plan)
		if stateErr != nil {
			return HostProjectionReceipt{}, projectionConflict("publish installing ownership journal", err)
		}
		if ownership.state == projectionPrevious {
			ownership, err = beginProjectionUpgrade(plan, ownership, boundary)
			if err != nil {
				return HostProjectionReceipt{}, err
			}
		}
		if _, err := convergeProjection(plan, ownership, boundary); err != nil {
			return HostProjectionReceipt{}, err
		}
		return receipt, nil
	}
	if err := runProjectionBoundary(boundary, "after_journal"); err != nil {
		return HostProjectionReceipt{}, err
	}
	ownership := projectionOwnership{journalBytes: plan.installingBytes, state: projectionInstalling}
	if _, err := convergeProjection(plan, ownership, boundary); err != nil {
		return HostProjectionReceipt{}, err
	}
	return receipt, nil
}

// VerifyHostProjection checks the closed ownership manifest, exact projected
// bytes and modes, and only the Mnemon-owned normalized registration subentry.
// Unknown adjacent Host settings are parsed and preserved but never treated as
// Mnemon authority.
func VerifyHostProjection(workspace, nodeState string, host assets.Host, bundle assets.Bundle) error {
	plan, err := prepareProjection(workspace, nodeState, host, bundle)
	if err != nil {
		return err
	}
	return verifyProjection(plan)
}

func prepareProjection(workspace, nodeState string, host assets.Host, bundle assets.Bundle) (projectionPlan, error) {
	if !host.Valid() {
		return projectionPlan{}, fmt.Errorf("%w: unknown Host", ErrUnsafeProjection)
	}
	if workspace == "" || !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace {
		return projectionPlan{}, fmt.Errorf("%w: workspace must be an absolute clean path", ErrUnsafeProjection)
	}
	if err := requireOwnedRealDirectory(workspace); err != nil {
		return projectionPlan{}, err
	}
	wantNodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if nodeState != wantNodeState {
		return projectionPlan{}, fmt.Errorf("%w: Node state must use the frozen workspace path", ErrUnsafeProjection)
	}
	if err := VerifyNodeBundle(nodeState, bundle); err != nil {
		return projectionPlan{}, fmt.Errorf("verify canonical Node bundle before Host projection: %w", err)
	}

	registration, ok := bundle.Registration(host)
	if !ok {
		return projectionPlan{}, fmt.Errorf("%w: Host registration is absent", ErrUnsafeProjection)
	}
	hostDir := map[assets.Host]string{assets.HostCodex: ".codex", assets.HostClaudeCode: ".claude"}[host]
	hostRoot := filepath.Join(workspace, hostDir)
	hookRelative := filepath.Join("hooks", "mnemon-harness", "hook.sh")
	hookPath := filepath.Join(hostRoot, hookRelative)
	if !filepath.IsAbs(hookPath) || filepath.Clean(hookPath) != hookPath {
		return projectionPlan{}, fmt.Errorf("%w: projected Hook path is not absolute and clean", ErrUnsafeProjection)
	}
	entry := hostRegistrationEntry{Hooks: []hostRegistrationHook{{
		Command: hookPath, StatusMessage: registration.Value.Hook.StatusMessage,
		Timeout: registration.Value.Hook.Timeout, Type: registration.Value.Hook.Type,
	}}}
	entryCanonical, err := json.Marshal(entry)
	if err != nil {
		return projectionPlan{}, fmt.Errorf("encode Host registration: %w", err)
	}

	sources := []struct {
		destination string
		source      string
	}{
		{destination: filepath.Join("skills", "mnemon-harness", "SKILL.md"), source: "SKILL.md"},
		{destination: filepath.Join("skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md"), source: "guides/teamwork/GUIDE.md"},
		{destination: hookRelative, source: "hosts/" + string(host) + "/hook.sh"},
	}
	files := make([]projectedFile, 0, len(sources))
	manifestFiles := make([]ownershipFile, 0, len(sources))
	for _, source := range sources {
		content, err := bundle.Read(filepath.ToSlash(source.source))
		if err != nil {
			return projectionPlan{}, fmt.Errorf("read projected source %s: %w", source.source, err)
		}
		mode := os.FileMode(0o644)
		modeText := "0644"
		if strings.HasSuffix(source.source, "/hook.sh") {
			mode, modeText = 0o755, "0755"
		}
		record := ownershipFile{Path: filepath.ToSlash(source.destination), Source: filepath.ToSlash(source.source),
			InstalledDigest: digest(content), Mode: modeText}
		files = append(files, projectedFile{content: content, mode: mode,
			path: filepath.Join(hostRoot, source.destination), record: record})
		manifestFiles = append(manifestFiles, record)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].record.Path < files[j].record.Path })
	sort.Slice(manifestFiles, func(i, j int) bool { return manifestFiles[i].Path < manifestFiles[j].Path })
	configPath := filepath.Join(hostRoot, registration.Target)
	registrationRecord := ownershipRegistration{Path: registration.Target, ManagedKey: registration.ManagedKey,
		InstalledDigest: digest(entryCanonical)}
	applied := ownershipManifest{Schema: projectionSchemaVersion, State: projectionApplied, Host: host,
		AssetRevision: bundle.Manifest().AssetRevision, Files: manifestFiles,
		Registrations: []ownershipRegistration{registrationRecord}}
	installing := applied
	installing.State = projectionInstalling
	appliedCanonical, err := json.Marshal(applied)
	if err != nil {
		return projectionPlan{}, fmt.Errorf("encode ownership manifest: %w", err)
	}
	installingCanonical, err := json.Marshal(installing)
	if err != nil {
		return projectionPlan{}, fmt.Errorf("encode installing ownership journal: %w", err)
	}
	appliedBytes := append(appliedCanonical, '\n')
	installingBytes := append(installingCanonical, '\n')
	ownershipPath := filepath.Join(workspace, ".mnemon", "harness", "integrations", string(host), "ownership.json")
	return projectionPlan{configPath: configPath, entry: entry,
		entryDigest: registrationRecord.InstalledDigest, files: files,
		applied: applied, appliedBytes: appliedBytes, installing: installing,
		installingBytes: installingBytes, ownershipPath: ownershipPath,
		registration: registration, workspace: workspace}, nil
}

type fileSnapshot struct {
	exists bool
	mode   os.FileMode
	raw    []byte
	stat   os.FileInfo
}

func inspectFreshProjection(plan projectionPlan) (map[string]any, fileSnapshot, error) {
	for _, file := range plan.files {
		if _, err := os.Lstat(file.path); err == nil {
			return nil, fileSnapshot{}, projectionConflict("managed Host file exists without ownership", nil)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fileSnapshot{}, projectionConflict("inspect managed Host file", err)
		}
	}
	config, snapshot, err := readSharedJSON(plan.configPath)
	if err != nil {
		return nil, fileSnapshot{}, err
	}
	entries, err := registrationEntries(config, plan.registration.Value.Event, false)
	if err != nil {
		return nil, fileSnapshot{}, err
	}
	for _, candidate := range entries {
		canonical, err := json.Marshal(candidate)
		if err != nil {
			return nil, fileSnapshot{}, projectionConflict("normalize Host registration", err)
		}
		if digest(canonical) == plan.entryDigest || entryUsesExactCommand(candidate, plan.entry.Hooks[0].Command) ||
			entryUsesManagedStatus(candidate, plan.entry.Hooks[0].StatusMessage) {
			return nil, fileSnapshot{}, projectionConflict("managed Host registration exists without ownership", nil)
		}
	}
	return config, snapshot, nil
}

func readProjectionOwnership(plan projectionPlan) (projectionOwnership, error) {
	relative, err := filepath.Rel(plan.workspace, filepath.Dir(plan.ownershipPath))
	if err != nil {
		return projectionOwnership{}, projectionConflict("resolve ownership journal directory", err)
	}
	if err := requireOwnedDirectoryChain(plan.workspace, relative); err != nil {
		return projectionOwnership{}, err
	}
	raw, _, err := readSafeRegular(plan.ownershipPath, 0o600, 64<<10)
	if err != nil {
		return projectionOwnership{}, projectionConflict("read ownership journal", err)
	}
	var manifest ownershipManifest
	if err := decodeClosedCanonical(raw, &manifest); err != nil {
		return projectionOwnership{}, projectionConflict("decode ownership journal", err)
	}
	switch {
	case reflect.DeepEqual(manifest, plan.installing) && bytes.Equal(raw, plan.installingBytes):
		return projectionOwnership{journalBytes: raw, state: projectionInstalling}, nil
	case reflect.DeepEqual(manifest, plan.applied) && bytes.Equal(raw, plan.appliedBytes):
		return projectionOwnership{journalBytes: raw, state: projectionApplied}, nil
	case manifest.State == projectionUpgrading:
		previous, err := validateUpgradeJournal(plan, manifest)
		if err != nil {
			return projectionOwnership{}, err
		}
		return projectionOwnership{journalBytes: raw, previous: previous, state: projectionUpgrading}, nil
	case manifest.State == projectionApplied:
		previous, err := validatePreviousApplied(plan, manifest)
		if err != nil {
			return projectionOwnership{}, err
		}
		return projectionOwnership{journalBytes: raw, previous: previous, state: projectionPrevious}, nil
	default:
		return projectionOwnership{}, projectionConflict("ownership journal differs from the exact projection", nil)
	}
}

func validatePreviousApplied(plan projectionPlan, manifest ownershipManifest) (*ownershipPrevious, error) {
	if manifest.Schema != projectionSchemaVersion || manifest.State != projectionApplied ||
		manifest.Host != plan.applied.Host || manifest.Previous != nil ||
		manifest.AssetRevision == plan.applied.AssetRevision || !validProjectionDigest(manifest.AssetRevision) {
		return nil, projectionConflict("previous applied ownership has unsafe identity or revision", nil)
	}
	previous := &ownershipPrevious{AssetRevision: manifest.AssetRevision,
		Files:         append([]ownershipFile(nil), manifest.Files...),
		Registrations: append([]ownershipRegistration(nil), manifest.Registrations...)}
	if err := validatePreviousShape(plan, previous); err != nil {
		return nil, err
	}
	return previous, nil
}

func validateUpgradeJournal(plan projectionPlan, manifest ownershipManifest) (*ownershipPrevious, error) {
	if manifest.Schema != projectionSchemaVersion || manifest.State != projectionUpgrading ||
		manifest.Host != plan.applied.Host || manifest.AssetRevision != plan.applied.AssetRevision ||
		manifest.Previous == nil {
		return nil, projectionConflict("upgrade journal has unsafe identity or desired revision", nil)
	}
	desired := manifest
	desired.State = projectionApplied
	desired.Previous = nil
	if !reflect.DeepEqual(desired, plan.applied) {
		return nil, projectionConflict("upgrade journal desired projection differs from current canonical assets", nil)
	}
	previous := &ownershipPrevious{AssetRevision: manifest.Previous.AssetRevision,
		Files:         append([]ownershipFile(nil), manifest.Previous.Files...),
		Registrations: append([]ownershipRegistration(nil), manifest.Previous.Registrations...)}
	if previous.AssetRevision == plan.applied.AssetRevision || !validProjectionDigest(previous.AssetRevision) {
		return nil, projectionConflict("upgrade journal previous revision is invalid", nil)
	}
	if err := validatePreviousShape(plan, previous); err != nil {
		return nil, err
	}
	return previous, nil
}

func validatePreviousShape(plan projectionPlan, previous *ownershipPrevious) error {
	if previous == nil || len(previous.Files) != len(plan.applied.Files) ||
		len(previous.Registrations) != len(plan.applied.Registrations) {
		return projectionConflict("previous ownership has the wrong closed file or registration count", nil)
	}
	for index, record := range previous.Files {
		desired := plan.applied.Files[index]
		if record.Path != desired.Path || record.Source != desired.Source || record.Mode != desired.Mode ||
			!validProjectionDigest(record.InstalledDigest) {
			return projectionConflict("previous ownership escapes frozen managed file paths", nil)
		}
	}
	for index, record := range previous.Registrations {
		desired := plan.applied.Registrations[index]
		if record.Path != desired.Path || record.ManagedKey != desired.ManagedKey ||
			!validProjectionDigest(record.InstalledDigest) {
			return projectionConflict("previous ownership escapes the frozen managed registration", nil)
		}
	}
	return nil
}

func beginProjectionUpgrade(plan projectionPlan, ownership projectionOwnership, boundary projectionBoundary) (projectionOwnership, error) {
	if ownership.state != projectionPrevious || ownership.previous == nil {
		return projectionOwnership{}, projectionConflict("upgrade requires one validated previous applied projection", nil)
	}
	if err := preflightPreviousProjection(plan, *ownership.previous); err != nil {
		return projectionOwnership{}, err
	}
	manifest := plan.applied
	manifest.State = projectionUpgrading
	manifest.Previous = &ownershipPrevious{AssetRevision: ownership.previous.AssetRevision,
		Files:         append([]ownershipFile(nil), ownership.previous.Files...),
		Registrations: append([]ownershipRegistration(nil), ownership.previous.Registrations...)}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return projectionOwnership{}, fmt.Errorf("encode upgrade ownership journal: %w", err)
	}
	journalBytes := append(canonical, '\n')
	if err := replaceExactFile(filepath.Dir(plan.ownershipPath), plan.ownershipPath, ownership.journalBytes, journalBytes, 0o600); err != nil {
		current, readErr := readProjectionOwnership(plan)
		if readErr != nil || current.state != projectionUpgrading ||
			!reflect.DeepEqual(current.previous, ownership.previous) {
			return projectionOwnership{}, projectionConflict("publish upgrade ownership journal", err)
		}
		return current, nil
	}
	if err := runProjectionBoundary(boundary, "after_upgrade_journal"); err != nil {
		return projectionOwnership{}, err
	}
	return projectionOwnership{journalBytes: journalBytes, previous: manifest.Previous, state: projectionUpgrading}, nil
}

func convergeProjection(plan projectionPlan, ownership projectionOwnership, boundary projectionBoundary) (bool, error) {
	if ownership.state != projectionInstalling && ownership.state != projectionApplied && ownership.state != projectionUpgrading {
		return false, projectionConflict("ownership journal has an unknown state", nil)
	}
	if err := createProjectionDirectories(plan.workspace, plan); err != nil {
		return false, err
	}
	changed := false
	for _, file := range plan.files {
		installed, err := convergeProjectedFile(plan, file, previousFileRecord(ownership.previous, file.record.Path))
		if err != nil {
			return false, err
		}
		changed = changed || installed
		if err := runProjectionBoundary(boundary, "after_file:"+file.record.Path); err != nil {
			return false, err
		}
	}
	registered, err := convergeRegistration(plan, ownership.previous)
	if err != nil {
		return false, err
	}
	changed = changed || registered
	if err := runProjectionBoundary(boundary, "after_config"); err != nil {
		return false, err
	}
	if err := verifyProjectedState(plan); err != nil {
		return false, err
	}
	if ownership.state == projectionInstalling || ownership.state == projectionUpgrading {
		if err := runProjectionBoundary(boundary, "before_applied"); err != nil {
			return false, err
		}
		if err := replaceExactFile(filepath.Dir(plan.ownershipPath), plan.ownershipPath, ownership.journalBytes, plan.appliedBytes, 0o600); err != nil {
			// A concurrent exact installer may have finalized first.
			current, stateErr := readProjectionOwnership(plan)
			if stateErr != nil || current.state != projectionApplied {
				return false, projectionConflict("finalize ownership journal", err)
			}
		}
		changed = true
	}
	if err := verifyProjection(plan); err != nil {
		return false, err
	}
	return changed, nil
}

func preflightPreviousProjection(plan projectionPlan, previous ownershipPrevious) error {
	if err := validateExistingProjectionParents(plan); err != nil {
		return err
	}
	for _, file := range plan.files {
		record := previousFileRecord(&previous, file.record.Path)
		if record == nil {
			return projectionConflict("previous ownership omits a frozen managed file", nil)
		}
		content, info, err := readSafeRegular(file.path, 0, maxHostConfigBytes)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || info.Mode().Perm() != file.mode || digest(content) != record.InstalledDigest {
			return projectionConflict("existing Host file does not match previous applied ownership", err)
		}
	}
	document, _, err := readSharedJSON(plan.configPath)
	if err != nil {
		return err
	}
	entries, err := registrationEntries(document, plan.registration.Value.Event, false)
	if err != nil {
		return err
	}
	previousDigest := previous.Registrations[0].InstalledDigest
	previousMatches, commandMatches, previousIndex, err := registrationDigestAndCommandCounts(entries, previousDigest, plan.entry.Hooks[0].Command)
	if err != nil {
		return err
	}
	if previousMatches == 0 && commandMatches == 0 {
		return nil
	}
	if previousMatches == 1 && commandMatches == 1 &&
		entryUsesExactCommand(entries[previousIndex], plan.entry.Hooks[0].Command) {
		return nil
	}
	return projectionConflict(fmt.Sprintf("previous registration digest/command counts are %d/%d, want 0/0 or 1/1", previousMatches, commandMatches), nil)
}

func validateExistingProjectionParents(plan projectionPlan) error {
	paths := make([]string, 0, len(plan.files)+1)
	for _, file := range plan.files {
		paths = append(paths, filepath.Dir(file.path))
	}
	paths = append(paths, filepath.Dir(plan.configPath))
	for _, path := range paths {
		relative, err := filepath.Rel(plan.workspace, path)
		if err != nil {
			return projectionConflict("resolve previous projection directory", err)
		}
		if err := requireExistingOwnedDirectoryChain(plan.workspace, relative); err != nil {
			return err
		}
	}
	return nil
}

func previousFileRecord(previous *ownershipPrevious, path string) *ownershipFile {
	if previous == nil {
		return nil
	}
	for index := range previous.Files {
		if previous.Files[index].Path == path {
			return &previous.Files[index]
		}
	}
	return nil
}

func convergeProjectedFile(plan projectionPlan, file projectedFile, previous *ownershipFile) (bool, error) {
	content, info, err := readSafeRegular(file.path, 0, maxHostConfigBytes)
	if errors.Is(err, os.ErrNotExist) {
		if err := publishExclusiveFile(filepath.Dir(plan.ownershipPath), file.path, file.content, file.mode); err != nil {
			// Accept only an exact concurrent publication made after the missing
			// check; any other occupant is drift under the desired journal.
			content, _, readErr := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
			if readErr != nil || !bytes.Equal(content, file.content) {
				return false, projectionConflict("publish desired Host file", err)
			}
			return false, nil
		}
		return true, nil
	}
	if err != nil {
		return false, projectionConflict("managed Host file drift", err)
	}
	if info.Mode().Perm() == file.mode && bytes.Equal(content, file.content) {
		return false, nil
	}
	if previous != nil && info.Mode().Perm() == file.mode && digest(content) == previous.InstalledDigest {
		if err := replaceExactFile(filepath.Dir(plan.ownershipPath), file.path, content, file.content, file.mode); err != nil {
			// Accept a concurrent replacement only if it published exact desired
			// bytes and mode under the same upgrade journal.
			current, currentInfo, readErr := readSafeRegular(file.path, 0, maxHostConfigBytes)
			if readErr != nil || currentInfo.Mode().Perm() != file.mode || !bytes.Equal(current, file.content) {
				return false, projectionConflict("replace previous managed Host file", err)
			}
			return false, nil
		}
		return true, nil
	}
	return false, projectionConflict("managed Host file matches neither previous nor desired ownership", nil)
}

func convergeRegistration(plan projectionPlan, previous *ownershipPrevious) (bool, error) {
	document, snapshot, err := readSharedJSON(plan.configPath)
	if err != nil {
		return false, err
	}
	entries, err := registrationEntries(document, plan.registration.Value.Event, false)
	if err != nil {
		return false, err
	}
	if previous != nil {
		return convergeUpgradeRegistration(plan, previous, document, snapshot, entries)
	}
	exact, logical, err := registrationCounts(entries, plan)
	if err != nil {
		return false, err
	}
	if exact == 1 && logical == 1 {
		return false, nil
	}
	if exact != 0 || logical != 0 {
		return false, projectionConflict(fmt.Sprintf("managed Host registration exact/logical counts are %d/%d during convergence", exact, logical), nil)
	}
	if managedStatusCount(entries, plan.entry.Hooks[0].StatusMessage) != 0 {
		return false, projectionConflict("managed Host registration status remains but its command drifted", nil)
	}
	return appendDesiredRegistration(plan, document, snapshot)
}

func convergeUpgradeRegistration(plan projectionPlan, previous *ownershipPrevious, document map[string]any, snapshot fileSnapshot, entries []any) (bool, error) {
	previousDigest := previous.Registrations[0].InstalledDigest
	desiredMatches, commandMatches, desiredIndex, err := registrationDigestAndCommandCounts(entries, plan.entryDigest, plan.entry.Hooks[0].Command)
	if err != nil {
		return false, err
	}
	previousMatches, previousCommandMatches, previousIndex, err := registrationDigestAndCommandCounts(entries, previousDigest, plan.entry.Hooks[0].Command)
	if err != nil {
		return false, err
	}
	if commandMatches != previousCommandMatches {
		return false, projectionConflict("registration command accounting is inconsistent", nil)
	}
	if desiredMatches == 1 && commandMatches == 1 {
		if entryUsesExactCommand(entries[desiredIndex], plan.entry.Hooks[0].Command) {
			return false, nil
		}
		return false, projectionConflict("desired registration digest is detached from its logical command", nil)
	}
	if previousMatches == 1 && commandMatches == 1 &&
		entryUsesExactCommand(entries[previousIndex], plan.entry.Hooks[0].Command) {
		hooks := document["hooks"].(map[string]any)
		updated := append([]any(nil), entries...)
		updated[previousIndex] = plan.entry
		hooks[plan.registration.Value.Event] = updated
		if err := replaceSharedJSON(filepath.Dir(plan.ownershipPath), plan.configPath, snapshot, document); err != nil {
			if registrationIsDesired(plan) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if desiredMatches == 0 && previousMatches == 0 && commandMatches == 0 &&
		managedStatusCount(entries, plan.entry.Hooks[0].StatusMessage) == 0 {
		return appendDesiredRegistration(plan, document, snapshot)
	}
	return false, projectionConflict(fmt.Sprintf("upgrade registration desired/previous/command counts are %d/%d/%d", desiredMatches, previousMatches, commandMatches), nil)
}

func appendDesiredRegistration(plan projectionPlan, document map[string]any, snapshot fileSnapshot) (bool, error) {
	updated, err := appendRegistration(document, plan.registration.Value.Event, plan.entry)
	if err != nil {
		return false, err
	}
	if err := replaceSharedJSON(filepath.Dir(plan.ownershipPath), plan.configPath, snapshot, updated); err != nil {
		// Preserve a concurrent adjacent update. If another exact installer
		// added the desired entry, that result already satisfies convergence.
		if registrationIsDesired(plan) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func registrationIsDesired(plan projectionPlan) bool {
	current, _, err := readSharedJSON(plan.configPath)
	if err != nil {
		return false
	}
	entries, err := registrationEntries(current, plan.registration.Value.Event, false)
	if err != nil {
		return false
	}
	exact, command, _, err := registrationDigestAndCommandCounts(entries, plan.entryDigest, plan.entry.Hooks[0].Command)
	return err == nil && exact == 1 && command == 1
}

func registrationDigestAndCommandCounts(entries []any, wantedDigest, command string) (int, int, int, error) {
	digestMatches := 0
	commandMatches := 0
	digestIndex := -1
	for index, candidate := range entries {
		canonical, err := json.Marshal(candidate)
		if err != nil {
			return 0, 0, -1, projectionConflict("normalize managed Host registration", err)
		}
		if digest(canonical) == wantedDigest {
			digestMatches++
			digestIndex = index
		}
		if entryUsesExactCommand(candidate, command) {
			commandMatches++
		}
	}
	return digestMatches, commandMatches, digestIndex, nil
}

func verifyProjection(plan projectionPlan) error {
	ownership, err := readProjectionOwnership(plan)
	if err != nil {
		return err
	}
	if ownership.state != projectionApplied {
		return projectionConflict("ownership journal is not applied", nil)
	}
	return verifyProjectedState(plan)
}

func verifyProjectedState(plan projectionPlan) error {
	if err := verifyProjectionDirectories(plan); err != nil {
		return err
	}
	for _, file := range plan.files {
		content, fileInfo, err := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
		if err != nil || !ownedByCurrentUser(fileInfo) || !bytes.Equal(content, file.content) {
			if err == nil {
				err = errors.New("content or owner differs")
			}
			return projectionConflict("managed Host file drift", err)
		}
	}
	config, _, err := readSharedJSON(plan.configPath)
	if err != nil {
		return err
	}
	entries, err := registrationEntries(config, plan.registration.Value.Event, true)
	if err != nil {
		return err
	}
	matches, logicalMatches, err := registrationCounts(entries, plan)
	if err != nil {
		return err
	}
	if matches != 1 || logicalMatches != 1 {
		return projectionConflict(fmt.Sprintf("managed Host registration exact/logical counts are %d/%d, want 1/1", matches, logicalMatches), nil)
	}
	return nil
}

func registrationCounts(entries []any, plan projectionPlan) (int, int, error) {
	exact := 0
	logical := 0
	for _, candidate := range entries {
		canonical, err := json.Marshal(candidate)
		if err != nil {
			return 0, 0, projectionConflict("normalize managed Host registration", err)
		}
		if digest(canonical) == plan.entryDigest {
			exact++
		}
		if entryUsesExactCommand(candidate, plan.entry.Hooks[0].Command) {
			logical++
		}
	}
	return exact, logical, nil
}

func verifyProjectionDirectories(plan projectionPlan) error {
	directories := make(map[string]struct{})
	for _, file := range plan.files {
		directories[filepath.Dir(file.path)] = struct{}{}
	}
	directories[filepath.Dir(plan.configPath)] = struct{}{}
	directories[filepath.Dir(plan.ownershipPath)] = struct{}{}
	ordered := make([]string, 0, len(directories))
	for directory := range directories {
		ordered = append(ordered, directory)
	}
	sort.Strings(ordered)
	for _, directory := range ordered {
		relative, err := filepath.Rel(plan.workspace, directory)
		if err != nil {
			return projectionConflict("resolve projection directory", err)
		}
		if err := requireOwnedDirectoryChain(plan.workspace, relative); err != nil {
			return err
		}
	}
	return nil
}

func createProjectionDirectories(workspace string, plan projectionPlan) error {
	for _, file := range plan.files {
		relative, err := filepath.Rel(workspace, filepath.Dir(file.path))
		if err != nil {
			return fmt.Errorf("resolve Host directory: %w", err)
		}
		if err := ensureOwnedDirectoryChain(workspace, relative, 0o755); err != nil {
			return err
		}
	}
	configRelative, err := filepath.Rel(workspace, filepath.Dir(plan.configPath))
	if err != nil {
		return err
	}
	if err := ensureOwnedDirectoryChain(workspace, configRelative, 0o755); err != nil {
		return err
	}
	ownershipRelative, err := filepath.Rel(workspace, filepath.Dir(plan.ownershipPath))
	if err != nil {
		return err
	}
	return ensureOwnedDirectoryChain(workspace, ownershipRelative, 0o700)
}

func createOwnershipDirectories(workspace string, plan projectionPlan) error {
	relative, err := filepath.Rel(workspace, filepath.Dir(plan.ownershipPath))
	if err != nil {
		return projectionConflict("resolve ownership journal directory", err)
	}
	return ensureOwnedDirectoryChain(workspace, relative, 0o700)
}

func ensureOwnedDirectoryChain(root, relative string, createMode os.FileMode) error {
	if relative == "." {
		return nil
	}
	if filepath.IsAbs(relative) || relative == "" || filepath.Clean(relative) != relative ||
		relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: directory escapes workspace", ErrUnsafeProjection)
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		if err := os.Mkdir(current, createMode); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create projection directory: %w", err)
		}
		if err := requireOwnedRealDirectory(current); err != nil {
			return err
		}
	}
	return nil
}

func requireOwnedDirectoryChain(root, relative string) error {
	if relative == "." {
		return nil
	}
	if filepath.IsAbs(relative) || relative == "" || filepath.Clean(relative) != relative ||
		relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: directory escapes workspace", ErrUnsafeProjection)
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		if err := requireOwnedRealDirectory(current); err != nil {
			return err
		}
	}
	return nil
}

func requireExistingOwnedDirectoryChain(root, relative string) error {
	if relative == "." {
		return nil
	}
	if filepath.IsAbs(relative) || relative == "" || filepath.Clean(relative) != relative ||
		relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: directory escapes workspace", ErrUnsafeProjection)
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		if _, err := os.Lstat(current); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return projectionConflict("inspect previous projection directory", err)
		}
		if err := requireOwnedRealDirectory(current); err != nil {
			return err
		}
	}
	return nil
}

func requireOwnedRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ownedByCurrentUser(info) {
		return fmt.Errorf("%w: %s must be a real current-owner directory", ErrUnsafeProjection, path)
	}
	return nil
}

func publishExclusiveFile(stagingDirectory, path string, content []byte, mode os.FileMode) error {
	targetDirectory := filepath.Dir(path)
	temporary, err := os.CreateTemp(stagingDirectory, ".mnemon-harness-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	fail := func(cause error) error {
		_ = temporary.Close()
		return cause
	}
	if _, err := temporary.Write(content); err != nil {
		return fail(err)
	}
	if err := temporary.Chmod(mode); err != nil {
		return fail(err)
	}
	if err := temporary.Sync(); err != nil {
		return fail(err)
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	// Linking a fully synced temporary file publishes without the overwrite
	// semantics of rename. It therefore preserves no-clobber under races.
	if err := os.Link(temporaryPath, path); err != nil {
		return err
	}
	if err := syncDirectory(targetDirectory); err != nil {
		return err
	}
	return nil
}

func replaceExactFile(stagingDirectory, path string, expected, replacement []byte, mode os.FileMode) error {
	content, info, err := readSafeRegular(path, mode, maxHostConfigBytes)
	if err != nil || !bytes.Equal(content, expected) {
		return projectionConflict("file changed before exact replacement", err)
	}
	targetDirectory := filepath.Dir(path)
	temporary, err := os.CreateTemp(stagingDirectory, ".mnemon-harness-replace-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	fail := func(cause error) error {
		_ = temporary.Close()
		return cause
	}
	if _, err := temporary.Write(replacement); err != nil {
		return fail(err)
	}
	if err := temporary.Chmod(mode); err != nil {
		return fail(err)
	}
	if err := temporary.Sync(); err != nil {
		return fail(err)
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	current, currentInfo, err := readSafeRegular(path, mode, maxHostConfigBytes)
	if err != nil || !os.SameFile(info, currentInfo) || !bytes.Equal(current, expected) {
		return projectionConflict("file changed during exact replacement", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(targetDirectory)
}

func readSharedJSON(path string) (map[string]any, fileSnapshot, error) {
	content, info, err := readSafeRegular(path, 0, maxHostConfigBytes)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[string]any), fileSnapshot{}, nil
	}
	if err != nil {
		return nil, fileSnapshot{}, projectionConflict("read shared Host config", err)
	}
	if !ownedByCurrentUser(info) || info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 != 0 {
		return nil, fileSnapshot{}, projectionConflict("shared Host config is not owner-safe", nil)
	}
	if len(bytes.TrimSpace(content)) == 0 {
		return make(map[string]any), fileSnapshot{exists: true, mode: info.Mode().Perm(), raw: content, stat: info}, nil
	}
	cleaned := stripHostJSON5(string(content))
	var document map[string]any
	decoder := json.NewDecoder(strings.NewReader(cleaned))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil || document == nil {
		return nil, fileSnapshot{}, projectionConflict("parse shared Host config", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fileSnapshot{}, projectionConflict("shared Host config has trailing JSON", err)
	}
	return document, fileSnapshot{exists: true, mode: info.Mode().Perm(), raw: content, stat: info}, nil
}

// stripHostJSON5 matches the deliberately narrow compatibility accepted by
// the established root setup surface: // line comments and trailing commas.
// Shared Host configuration is still emitted as ordinary JSON after update.
func stripHostJSON5(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	inString := false
	escaped := false
	for index := 0; index < len(value); {
		character := value[index]
		if escaped {
			result.WriteByte(character)
			escaped = false
			index++
			continue
		}
		if inString {
			if character == '\\' {
				escaped = true
			} else if character == '"' {
				inString = false
			}
			result.WriteByte(character)
			index++
			continue
		}
		if character == '"' {
			inString = true
			result.WriteByte(character)
			index++
			continue
		}
		if character == '/' && index+1 < len(value) && value[index+1] == '/' {
			for index < len(value) && value[index] != '\n' {
				index++
			}
			continue
		}
		if character == ',' {
			lookahead := index + 1
			for lookahead < len(value) && strings.ContainsRune(" \t\n\r", rune(value[lookahead])) {
				lookahead++
			}
			if lookahead < len(value) && (value[lookahead] == ']' || value[lookahead] == '}') {
				index++
				continue
			}
		}
		result.WriteByte(character)
		index++
	}
	return result.String()
}

func registrationEntries(document map[string]any, event string, required bool) ([]any, error) {
	hooksValue, ok := document["hooks"]
	if !ok {
		if required {
			return nil, projectionConflict("shared Host config is missing hooks", nil)
		}
		return nil, nil
	}
	hooks, ok := hooksValue.(map[string]any)
	if !ok {
		return nil, projectionConflict("shared Host hooks must be an object", nil)
	}
	eventValue, ok := hooks[event]
	if !ok {
		if required {
			return nil, projectionConflict("shared Host config is missing the managed event", nil)
		}
		return nil, nil
	}
	entries, ok := eventValue.([]any)
	if !ok {
		return nil, projectionConflict("shared Host event must be an array", nil)
	}
	return entries, nil
}

func appendRegistration(document map[string]any, event string, entry hostRegistrationEntry) (map[string]any, error) {
	hooksValue, ok := document["hooks"]
	if !ok {
		hooksValue = make(map[string]any)
		document["hooks"] = hooksValue
	}
	hooks, ok := hooksValue.(map[string]any)
	if !ok {
		return nil, projectionConflict("shared Host hooks must be an object", nil)
	}
	eventValue, ok := hooks[event]
	if !ok {
		eventValue = []any{}
	}
	entries, ok := eventValue.([]any)
	if !ok {
		return nil, projectionConflict("shared Host event must be an array", nil)
	}
	hooks[event] = append(entries, entry)
	return document, nil
}

func replaceSharedJSON(stagingDirectory, path string, snapshot fileSnapshot, document map[string]any) error {
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return projectionConflict("encode shared Host config", err)
	}
	encoded = append(encoded, '\n')
	mode := os.FileMode(0o644)
	if snapshot.exists {
		mode = snapshot.mode
	}
	targetDirectory := filepath.Dir(path)
	temporary, err := os.CreateTemp(stagingDirectory, ".mnemon-harness-config-")
	if err != nil {
		return projectionConflict("stage shared Host config", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	fail := func(cause error) error {
		_ = temporary.Close()
		return projectionConflict("stage shared Host config", cause)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fail(err)
	}
	if err := temporary.Chmod(mode); err != nil {
		return fail(err)
	}
	if err := temporary.Sync(); err != nil {
		return fail(err)
	}
	if err := temporary.Close(); err != nil {
		return projectionConflict("stage shared Host config", err)
	}
	if err := verifySnapshot(path, snapshot); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return projectionConflict("publish shared Host config", err)
	}
	if err := syncDirectory(targetDirectory); err != nil {
		return projectionConflict("sync shared Host config", err)
	}
	return nil
}

func verifySnapshot(path string, snapshot fileSnapshot) error {
	if !snapshot.exists {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return projectionConflict("recheck shared Host config", err)
		}
		return projectionConflict("shared Host config appeared during installation", nil)
	}
	content, info, err := readSafeRegular(path, snapshot.mode, maxHostConfigBytes)
	if err != nil || !os.SameFile(snapshot.stat, info) || !bytes.Equal(content, snapshot.raw) {
		return projectionConflict("shared Host config changed during installation", err)
	}
	return nil
}

func readSafeRegular(path string, expectedMode os.FileMode, maximum int64) ([]byte, os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !ownedByCurrentUser(info) ||
		(expectedMode != 0 && info.Mode().Perm() != expectedMode) || info.Size() < 0 || info.Size() > maximum {
		return nil, info, errors.New("file has unsafe type, owner, mode, or size")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, info, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, info, errors.New("file changed while opening")
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(content)) > maximum {
		if err == nil {
			err = errors.New("file exceeds size limit")
		}
		return nil, info, err
	}
	return content, opened, nil
}

func decodeClosedCanonical(raw []byte, target any) error {
	canonical := bytes.TrimSuffix(raw, []byte("\n"))
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains a trailing value")
	}
	reencoded, err := json.Marshal(target)
	if err != nil || !bytes.Equal(reencoded, canonical) || len(raw) != len(canonical)+1 || raw[len(raw)-1] != '\n' {
		return errors.New("JSON is not exact canonical closed-schema bytes")
	}
	return nil
}

func entryUsesExactCommand(candidate any, command string) bool {
	entry, ok := candidate.(map[string]any)
	if !ok {
		return false
	}
	hooks, ok := entry["hooks"].([]any)
	if !ok {
		return false
	}
	for _, value := range hooks {
		hook, ok := value.(map[string]any)
		if ok && hook["command"] == command {
			return true
		}
	}
	return false
}

func entryUsesManagedStatus(candidate any, status string) bool {
	entry, ok := candidate.(map[string]any)
	if !ok {
		return false
	}
	hooks, ok := entry["hooks"].([]any)
	if !ok {
		return false
	}
	for _, value := range hooks {
		hook, ok := value.(map[string]any)
		if ok && hook["statusMessage"] == status {
			return true
		}
	}
	return false
}

func managedStatusCount(entries []any, status string) int {
	count := 0
	for _, entry := range entries {
		if entryUsesManagedStatus(entry, status) {
			count++
		}
	}
	return count
}

func runProjectionBoundary(boundary projectionBoundary, stage string) error {
	if boundary == nil {
		return nil
	}
	return boundary(stage)
}

func projectionConflict(message string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrProjectionConflict, message)
	}
	return fmt.Errorf("%w: %s: %v", ErrProjectionConflict, message, cause)
}

func digest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validProjectionDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	encoded := strings.TrimPrefix(value, "sha256:")
	if encoded != strings.ToLower(encoded) {
		return false
	}
	_, err := hex.DecodeString(encoded)
	return err == nil
}
