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
	"sort"
	"strings"
)

const (
	managedRoot  = "managed"
	manifestPath = managedRoot + "/manifest.json"
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

type ArtifactPolicy struct {
	Allowed       bool   `json:"allowed"`
	MaxEntries    uint32 `json:"max_entries"`
	MaxPathBytes  uint32 `json:"max_path_bytes"`
	MaxRoots      uint8  `json:"max_roots"`
	MaxTotalBytes uint64 `json:"max_total_bytes"`
}

type ContentPolicy struct {
	MaxBytes uint32 `json:"max_bytes"`
	Required bool   `json:"required"`
	Source   string `json:"source"`
}

type DeadlinePolicy struct {
	Default string `json:"default"`
	Maximum string `json:"maximum"`
	Minimum string `json:"minimum"`
}

type ReceiptPolicy struct {
	Action     string `json:"action"`
	Handling   string `json:"handling"`
	MaxResults uint8  `json:"max_results"`
	Status     string `json:"status"`
}

type SelectorPolicy struct {
	Channel     string   `json:"channel"`
	Participant []string `json:"participant"`
}

type ActionSchema struct {
	Action         string          `json:"action"`
	AllowedContext []string        `json:"allowed_context"`
	Artifacts      ArtifactPolicy  `json:"artifacts"`
	Content        ContentPolicy   `json:"content"`
	Deadline       *DeadlinePolicy `json:"deadline"`
	Receipt        ReceiptPolicy   `json:"receipt"`
	SchemaVersion  int             `json:"schema_version"`
	Selectors      *SelectorPolicy `json:"selectors"`
}

type Registration struct {
	Host          Host              `json:"host"`
	ManagedKey    string            `json:"managed_key"`
	SchemaVersion int               `json:"schema_version"`
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
	manifest     Manifest
	actions      map[string]ActionSchema
	registration map[Host]Registration
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
	bundle := Bundle{manifest: cloneManifest(manifest), actions: make(map[string]ActionSchema),
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
		if strings.HasPrefix(record.Path, "actions/teamwork/") {
			var schema ActionSchema
			if err := decodeCanonicalObject(content, &schema); err != nil {
				return Bundle{}, fmt.Errorf("managed action %s: %w", record.Path, err)
			}
			if err := validateActionSchema(schema); err != nil {
				return Bundle{}, fmt.Errorf("managed action %s: %w", record.Path, err)
			}
			if _, exists := bundle.actions[schema.Action]; exists {
				return Bundle{}, fmt.Errorf("duplicate managed action %s", schema.Action)
			}
			bundle.actions[schema.Action] = schema
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
	if len(bundle.actions) != 7 || len(bundle.registration) != 2 {
		return Bundle{}, errors.New("managed assets do not contain the closed Teamwork and Host sets")
	}
	return bundle, nil
}

func (bundle Bundle) Manifest() Manifest { return cloneManifest(bundle.manifest) }

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

func (bundle Bundle) Action(name string) (ActionSchema, bool) {
	action, ok := bundle.actions[name]
	if !ok {
		return ActionSchema{}, false
	}
	action.AllowedContext = append([]string(nil), action.AllowedContext...)
	if action.Selectors != nil {
		selectors := *action.Selectors
		selectors.Participant = append([]string(nil), selectors.Participant...)
		action.Selectors = &selectors
	}
	return action, true
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

func validateActionSchema(schema ActionSchema) error {
	requiredContent := map[string]bool{"offer": true, "accept": false, "decline": true,
		"deliver": true, "rework": true, "close": false, "cancel": true}
	artifactsAllowed := map[string]bool{"offer": true, "deliver": true, "rework": true}
	required, known := requiredContent[schema.Action]
	if !known || schema.SchemaVersion != 1 || len(schema.AllowedContext) == 0 ||
		schema.Content.MaxBytes != 8192 || schema.Content.Required != required ||
		schema.Content.Source != "content_file_or_stdin" || schema.Receipt.Action != "teamwork."+schema.Action ||
		schema.Receipt.Status != "accepted" || schema.Receipt.MaxResults == 0 {
		return errors.New("action differs from the closed Teamwork contract")
	}
	if schema.Action == "offer" {
		if schema.Deadline == nil || schema.Deadline.Default != "24h" || schema.Deadline.Minimum != "5m" ||
			schema.Deadline.Maximum != "168h" || schema.Selectors == nil ||
			schema.Selectors.Channel != "optional_when_unambiguous" ||
			strings.Join(schema.Selectors.Participant, ",") != "effective_alias,auto,team" ||
			schema.Receipt.MaxResults != 7 || schema.Receipt.Handling != "context_dependent" {
			return errors.New("offer selectors, deadline, or receipt differ from the closed contract")
		}
	} else if schema.Deadline != nil || schema.Selectors != nil || schema.Receipt.MaxResults != 1 ||
		schema.Receipt.Handling != "completed" {
		return errors.New("non-offer action exposes selectors, deadline, or a wrong receipt")
	}
	if artifactsAllowed[schema.Action] {
		if !schema.Artifacts.Allowed || schema.Artifacts.MaxRoots != 16 ||
			schema.Artifacts.MaxEntries != 4096 || schema.Artifacts.MaxPathBytes != 512 ||
			schema.Artifacts.MaxTotalBytes != 256<<20 {
			return errors.New("action Artifact bounds differ from the closed contract")
		}
	} else if schema.Artifacts != (ArtifactPolicy{}) {
		return errors.New("action that forbids Artifacts carries nonzero Artifact bounds")
	}
	return nil
}

func validateRegistration(registration Registration) error {
	wantTarget := map[Host]string{HostCodex: "hooks.json", HostClaudeCode: "settings.json"}[registration.Host]
	if !registration.Host.Valid() || registration.SchemaVersion != 1 ||
		registration.ManagedKey != "mnemon-harness" || registration.Target != wantTarget ||
		registration.Value.Event != "UserPromptSubmit" || registration.Value.Hook.Command != "{{HOOK_PATH}}" ||
		registration.Value.Hook.Type != "command" || registration.Value.Hook.Timeout != 3 ||
		registration.Value.Hook.StatusMessage == "" {
		return errors.New("Host registration differs from the mandatory bounded Hook contract")
	}
	return nil
}

func validateHook(content []byte) error {
	if len(content) == 0 || len(content) > 256 || !bytes.HasPrefix(content, []byte("#!/bin/sh\n")) {
		return errors.New("Hook body is absent, oversized, or has the wrong interpreter")
	}
	lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
	if len(lines) != 3 || lines[1] != "set -eu" || lines[2] != "exec mnemon-harness hook check" {
		return errors.New("Hook may only fail closed and execute the bounded check")
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

func sortedActionNames(actions map[string]ActionSchema) []string {
	names := make([]string, 0, len(actions))
	for name := range actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
