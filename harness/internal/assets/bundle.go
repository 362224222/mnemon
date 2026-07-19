package assets

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
)

const (
	managedRoot            = "managed"
	manifestPath           = managedRoot + "/manifest.json"
	teamworkActionPathRoot = "actions/teamwork/"
	hookTimeoutSeconds     = 10
	hookBody               = `#!/bin/sh
set -eu

if cue=$(mnemon-harness hook check); then
	if [ -n "$cue" ]; then
		printf '%s\n' "$cue"
	fi
	exit 0
fi

printf '%s\n' 'mnemon-harness hook check failed; managed Agent execution is blocked' >&2
exit 2
`
)

//go:embed managed
var managedFS embed.FS

type Host string

const (
	HostCodex      Host = "codex"
	HostClaudeCode Host = "claude-code"
)

func (host Host) Valid() bool { return host == HostCodex || host == HostClaudeCode }

type FileRecord struct {
	Digest string `json:"digest"`
	Hosts  []Host `json:"hosts"`
	Mode   string `json:"mode"`
	Path   string `json:"path"`
}

type Manifest struct {
	AssetRevision string       `json:"asset_revision"`
	Files         []FileRecord `json:"files"`
	SchemaVersion int          `json:"schema_version"`
}

type Registration struct {
	Host          Host              `json:"host"`
	ManagedKey    string            `json:"managed_key"`
	SchemaVersion int               `json:"schema_version"`
	SkillTarget   string            `json:"skill_target"`
	Target        string            `json:"target"`
	Value         RegistrationValue `json:"value"`
}

type RegistrationValue struct {
	Event string           `json:"event"`
	Hook  RegistrationHook `json:"hook"`
}

type RegistrationHook struct {
	Command       string `json:"command"`
	StatusMessage string `json:"statusMessage"`
	Timeout       int    `json:"timeout"`
	Type          string `json:"type"`
}

// Bundle is a validated immutable view of the canonical managed asset tree.
// It serves source bytes only; projection and Host registration live in the
// integration package.
type Bundle struct {
	manifest            Manifest
	manifestRaw         []byte
	teamworkActionPaths []string
	registration        map[Host]Registration
}

func Load() (Bundle, error) {
	raw, err := managedFS.ReadFile(manifestPath)
	if err != nil {
		return Bundle{}, fmt.Errorf("read managed asset manifest: %w", err)
	}
	var manifest Manifest
	if err := decodeCanonicalObject(raw, &manifest); err != nil {
		return Bundle{}, fmt.Errorf("managed asset manifest: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		return Bundle{}, err
	}
	bundle := Bundle{manifest: cloneManifest(manifest), manifestRaw: append([]byte(nil), raw...),
		registration: make(map[Host]Registration)}
	listed := make(map[string]struct{}, len(manifest.Files))
	for _, record := range manifest.Files {
		content, err := managedFS.ReadFile(managedRoot + "/" + record.Path)
		if err != nil {
			return Bundle{}, fmt.Errorf("read managed asset %s: %w", record.Path, err)
		}
		if digestBytes(content) != record.Digest {
			return Bundle{}, fmt.Errorf("managed asset %s differs from manifest digest", record.Path)
		}
		listed[record.Path] = struct{}{}
		if strings.HasPrefix(record.Path, teamworkActionPathRoot) {
			if !validTeamworkActionPath(record.Path) {
				return Bundle{}, fmt.Errorf("managed Teamwork action path %s is not lexical", record.Path)
			}
			bundle.teamworkActionPaths = append(bundle.teamworkActionPaths, record.Path)
		}
		if strings.HasSuffix(record.Path, "/registration.json") {
			var registration Registration
			if err := decodeCanonicalObject(content, &registration); err != nil {
				return Bundle{}, fmt.Errorf("managed registration %s: %w", record.Path, err)
			}
			if err := validateRegistration(registration); err != nil {
				return Bundle{}, fmt.Errorf("managed registration %s: %w", record.Path, err)
			}
			if _, exists := bundle.registration[registration.Host]; exists {
				return Bundle{}, fmt.Errorf("duplicate managed registration for %s", registration.Host)
			}
			bundle.registration[registration.Host] = registration
		}
		if strings.HasSuffix(record.Path, "/hook.sh") {
			if err := validateHook(content); err != nil {
				return Bundle{}, fmt.Errorf("managed hook %s: %w", record.Path, err)
			}
		}
	}
	if err := requireExactFileSet(listed); err != nil {
		return Bundle{}, err
	}
	if len(bundle.teamworkActionPaths) == 0 || len(bundle.registration) != 2 {
		return Bundle{}, errors.New("managed assets do not contain Teamwork action sources and the closed Host set")
	}
	return bundle, nil
}

func (bundle Bundle) Manifest() Manifest { return cloneManifest(bundle.manifest) }

// Revision returns the whole-manifest revision that binds every validated
// source file in this immutable bundle.
func (bundle Bundle) Revision() string { return bundle.manifest.AssetRevision }

// ManifestBytes returns the exact validated source bytes. Node bundle
// installation must not re-encode the manifest and accidentally create a
// second representation of the canonical asset revision.
func (bundle Bundle) ManifestBytes() []byte { return append([]byte(nil), bundle.manifestRaw...) }

func (bundle Bundle) Read(path string) ([]byte, error) {
	if _, ok := bundle.record(path); !ok {
		return nil, errors.New("asset path is absent from the validated manifest")
	}
	content, err := managedFS.ReadFile(managedRoot + "/" + path)
	return append([]byte(nil), content...), err
}

func (bundle Bundle) FilesFor(host Host) ([]FileRecord, error) {
	if !host.Valid() {
		return nil, errors.New("unknown managed asset Host")
	}
	result := make([]FileRecord, 0)
	for _, record := range bundle.manifest.Files {
		for _, candidate := range record.Hosts {
			if candidate == host {
				result = append(result, cloneFileRecord(record))
				break
			}
		}
	}
	return result, nil
}

// TeamworkActionPaths returns the manifest-filtered lexical paths of the raw
// Teamwork action sources. Semantic parsing belongs to the Teamwork package.
func (bundle Bundle) TeamworkActionPaths() []string {
	return append([]string(nil), bundle.teamworkActionPaths...)
}

// ReadTeamworkAction returns exact embedded bytes only for a Teamwork action
// path frozen into this validated bundle.
func (bundle Bundle) ReadTeamworkAction(path string) ([]byte, error) {
	for _, candidate := range bundle.teamworkActionPaths {
		if candidate == path {
			return bundle.Read(path)
		}
	}
	return nil, errors.New("path is not a Teamwork action in the validated manifest")
}

func (bundle Bundle) Registration(host Host) (Registration, bool) {
	registration, ok := bundle.registration[host]
	return registration, ok
}

func (bundle Bundle) record(path string) (FileRecord, bool) {
	for _, record := range bundle.manifest.Files {
		if record.Path == path {
			return record, true
		}
	}
	return FileRecord{}, false
}

func validateManifest(manifest Manifest) error {
	if manifest.SchemaVersion != 1 || len(manifest.Files) != 13 ||
		!validDigestText(manifest.AssetRevision) {
		return errors.New("managed asset manifest has invalid schema, revision, or file count")
	}
	withoutRevision := cloneManifest(manifest)
	withoutRevision.AssetRevision = ""
	canonical, err := json.Marshal(withoutRevision)
	if err != nil || digestBytes(canonical) != manifest.AssetRevision {
		return errors.New("managed asset revision does not bind canonical manifest metadata")
	}
	for index, record := range manifest.Files {
		if record.Path == "" || strings.HasPrefix(record.Path, "/") || strings.Contains(record.Path, "..") ||
			!validDigestText(record.Digest) || (record.Mode != "0644" && record.Mode != "0755") ||
			len(record.Hosts) == 0 || len(record.Hosts) > 2 {
			return fmt.Errorf("managed asset manifest record %d is invalid", index)
		}
		if index > 0 && manifest.Files[index-1].Path >= record.Path {
			return errors.New("managed asset manifest paths are not strictly ordered")
		}
		for hostIndex, host := range record.Hosts {
			if !host.Valid() || (hostIndex > 0 && record.Hosts[hostIndex-1] >= host) {
				return fmt.Errorf("managed asset %s has invalid Host applicability", record.Path)
			}
		}
		if strings.HasSuffix(record.Path, "/hook.sh") != (record.Mode == "0755") {
			return fmt.Errorf("managed asset %s has the wrong source mode", record.Path)
		}
	}
	return nil
}

func validTeamworkActionPath(path string) bool {
	name := strings.TrimPrefix(path, teamworkActionPathRoot)
	return name != path && len(name) > len(".json") && strings.HasSuffix(name, ".json") &&
		!strings.Contains(name, "/")
}

func validateRegistration(registration Registration) error {
	wantTarget := map[Host]string{HostCodex: "hooks.json", HostClaudeCode: "settings.json"}[registration.Host]
	wantSkillTarget := map[Host]string{
		HostCodex: ".agents/skills/mnemon-harness", HostClaudeCode: ".claude/skills/mnemon-harness",
	}[registration.Host]
	if !registration.Host.Valid() || registration.SchemaVersion != 1 ||
		registration.ManagedKey != "mnemon-harness" || registration.SkillTarget != wantSkillTarget ||
		registration.Target != wantTarget ||
		registration.Value.Event != "UserPromptSubmit" || registration.Value.Hook.Command != "{{HOOK_PATH}}" ||
		registration.Value.Hook.Type != "command" || registration.Value.Hook.Timeout != hookTimeoutSeconds ||
		registration.Value.Hook.StatusMessage == "" {
		return errors.New("Host registration differs from the mandatory bounded Hook contract")
	}
	return nil
}

func validateHook(content []byte) error {
	if !bytes.Equal(content, []byte(hookBody)) {
		return errors.New("Hook differs from the exact fail-closed bounded check")
	}
	return nil
}

func requireExactFileSet(listed map[string]struct{}) error {
	actual := make([]string, 0)
	err := fs.WalkDir(managedFS, managedRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && path != manifestPath {
			actual = append(actual, strings.TrimPrefix(path, managedRoot+"/"))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk managed assets: %w", err)
	}
	if len(actual) != len(listed) {
		return errors.New("managed asset manifest does not cover the exact source tree")
	}
	for _, path := range actual {
		if _, ok := listed[path]; !ok {
			return fmt.Errorf("managed asset %s is not listed in the manifest", path)
		}
	}
	return nil
}

func decodeCanonicalObject(raw []byte, target any) error {
	canonical := bytes.TrimSuffix(raw, []byte("\n"))
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains a trailing value")
	}
	closed, err := json.Marshal(target)
	if err != nil || !bytes.Equal(closed, canonical) {
		return errors.New("JSON is not exact canonical closed-schema bytes")
	}
	return nil
}

func validDigestText(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func cloneManifest(manifest Manifest) Manifest {
	result := manifest
	result.Files = make([]FileRecord, len(manifest.Files))
	for index, record := range manifest.Files {
		result.Files[index] = cloneFileRecord(record)
	}
	return result
}

func cloneFileRecord(record FileRecord) FileRecord {
	record.Hosts = append([]Host(nil), record.Hosts...)
	return record
}
