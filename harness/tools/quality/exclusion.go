package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const exclusionsPath = "harness/test/contracts/go_quality_exclusions.json"

const (
	exclusionGenerated = "generated"
	exclusionTestdata  = "testdata"
)

type exclusionManifest struct {
	SchemaVersion int              `json:"schema_version"`
	Entries       []exclusionEntry `json:"entries"`
}

type exclusionEntry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
	Owner  string `json:"owner"`
}

func loadQualityExclusions(root string, required bool) (exclusionManifest, error) {
	path := filepath.Join(root, filepath.FromSlash(exclusionsPath))
	manifest, err := readExactJSON[exclusionManifest](path)
	if err == nil {
		return manifest, validateExclusionManifest(manifest)
	}
	if !required && os.IsNotExist(unwrapPathError(err)) {
		return exclusionManifest{SchemaVersion: manifestSchemaVersion, Entries: []exclusionEntry{}}, nil
	}
	return exclusionManifest{}, err
}

func unwrapPathError(err error) error {
	for err != nil {
		pathError, ok := err.(*os.PathError)
		if ok {
			return pathError
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		err = unwrapper.Unwrap()
	}
	return nil
}

func validateExclusionManifest(manifest exclusionManifest) error {
	if err := validateSchema(manifest.SchemaVersion, "quality exclusions"); err != nil {
		return err
	}
	if manifest.Entries == nil {
		return fmt.Errorf("quality exclusion entries must be a JSON array, not null")
	}
	if !sort.SliceIsSorted(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].Path < manifest.Entries[j].Path }) {
		return fmt.Errorf("quality exclusion entries must be sorted by path")
	}
	for index, entry := range manifest.Entries {
		if index > 0 && manifest.Entries[index-1].Path == entry.Path {
			return fmt.Errorf("quality exclusions repeat path %s", entry.Path)
		}
		if err := validateExclusionEntry(entry); err != nil {
			return fmt.Errorf("quality exclusion %s: %w", entry.Path, err)
		}
	}
	return nil
}

func validateExclusionEntry(entry exclusionEntry) error {
	if err := validateHarnessPath(entry.Path, "path"); err != nil {
		return err
	}
	if !strings.HasSuffix(entry.Path, ".go") {
		return fmt.Errorf("path must identify one exact Go file")
	}
	if entry.Kind != exclusionGenerated && entry.Kind != exclusionTestdata {
		return fmt.Errorf("unsupported kind %q", entry.Kind)
	}
	if entry.Kind == exclusionTestdata && !strings.Contains(entry.Path, "/testdata/") {
		return fmt.Errorf("testdata exclusion path is not inside testdata")
	}
	if err := requireText(entry.Reason, "reason"); err != nil {
		return err
	}
	return requireText(entry.Owner, "owner")
}

func validateExclusionEvidence(root string, manifest exclusionManifest) error {
	for _, entry := range manifest.Entries {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(entry.Path)))
		if err != nil {
			return fmt.Errorf("quality exclusion %s is stale: %w", entry.Path, err)
		}
		if entry.Kind == exclusionGenerated && !isGeneratedSource(data) {
			return fmt.Errorf("quality exclusion %s lacks a canonical generated header", entry.Path)
		}
	}
	return nil
}

func exclusionKinds(manifest exclusionManifest) map[string]string {
	kinds := make(map[string]string, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		kinds[entry.Path] = entry.Kind
	}
	return kinds
}
