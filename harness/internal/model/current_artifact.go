package model

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// CurrentArtifactRef is a verified local root made visible by current. It
// intentionally carries no produced/referenced role: Agents never choose that
// authority. Event and brief refs contain only a root; the top-level current
// authority may bind that root to one exact readonly materialized path.
type CurrentArtifactRef struct {
	rootDigest Digest
	viewPath   string
}

func NewCurrentArtifactRef(root Digest) (CurrentArtifactRef, error) {
	if root.IsZero() {
		return CurrentArtifactRef{}, invalid("current Artifact ref", "root digest is required")
	}
	return CurrentArtifactRef{rootDigest: root}, nil
}

func NewCurrentArtifactView(root Digest, viewPath string) (CurrentArtifactRef, error) {
	if root.IsZero() {
		return CurrentArtifactRef{}, invalid("current Artifact view", "root digest is required")
	}
	path, err := validateCurrentArtifactViewPath(viewPath)
	if err != nil {
		return CurrentArtifactRef{}, err
	}
	return CurrentArtifactRef{rootDigest: root, viewPath: path}, nil
}

func (r CurrentArtifactRef) RootDigest() Digest { return r.rootDigest }
func (r CurrentArtifactRef) ViewPath() (string, bool) {
	return r.viewPath, r.viewPath != ""
}
func (r CurrentArtifactRef) MarshalJSON() ([]byte, error) {
	if r.rootDigest.IsZero() {
		return nil, invalid("current Artifact ref", "zero root")
	}
	return CanonicalMarshal(currentArtifactWire{RootDigest: r.rootDigest.String(), Path: r.viewPath})
}

func validateCurrentArtifactViewPath(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || len(value) > DefaultCurrentPathBytes ||
		strings.IndexByte(value, 0) >= 0 || strings.Contains(value, `\`) || strings.HasPrefix(value, "/") {
		return "", invalid("current Artifact view path", "must be bounded canonical workspace-relative UTF-8")
	}
	components := strings.Split(value, "/")
	if len(components) < 6 || components[0] != ".mnemon" || components[1] != "harness" ||
		components[2] != "node" || components[3] != "views" {
		return "", invalid("current Artifact view path", "must use the managed readonly view namespace")
	}
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return "", invalid("current Artifact view path", "contains an invalid component")
		}
	}
	if _, err := ParseRunID(components[4]); err != nil {
		return "", invalid("current Artifact view path", "contains an invalid Run identity")
	}
	ordinal, err := strconv.ParseUint(components[5], 10, 32)
	if err != nil || strconv.FormatUint(ordinal, 10) != components[5] {
		return "", invalid("current Artifact view path", "contains a noncanonical ordinal")
	}
	return value, nil
}
