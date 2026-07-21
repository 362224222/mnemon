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

func artifactRefsFromWire(wires []currentArtifactWire, max int) ([]CurrentArtifactRef, error) {
	result := make([]CurrentArtifactRef, len(wires))
	for index, wire := range wires {
		digest, err := ParseDigest(wire.RootDigest)
		if err != nil {
			return nil, err
		}
		if wire.Path == "" {
			result[index], err = NewCurrentArtifactRef(digest)
		} else {
			result[index], err = NewCurrentArtifactView(digest, wire.Path)
		}
		if err != nil {
			return nil, err
		}
	}
	return normalizeCurrentArtifactRefs(result, max)
}

func equalCurrentActions(left, right []OperationKind) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalArtifactRefs(left, right []CurrentArtifactRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].RootDigest() != right[index].RootDigest() ||
			left[index].viewPath != right[index].viewPath {
			return false
		}
	}
	return true
}

func normalizeCurrentArtifactRefs(refs []CurrentArtifactRef, max int) ([]CurrentArtifactRef, error) {
	if len(refs) > max {
		return nil, limit("current Artifact refs", len(refs), max)
	}
	result := append([]CurrentArtifactRef{}, refs...)
	for _, ref := range result {
		if ref.rootDigest.IsZero() {
			return nil, invalid("current Artifact refs", "contains a zero root")
		}
		if ref.viewPath != "" {
			if _, err := validateCurrentArtifactViewPath(ref.viewPath); err != nil {
				return nil, err
			}
		}
	}
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].rootDigest.String() < result[j-1].rootDigest.String(); j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	for index := 1; index < len(result); index++ {
		if result[index].rootDigest == result[index-1].rootDigest {
			return nil, invalid("current Artifact refs", "contains a duplicate root")
		}
	}
	return result, nil
}

func normalizeCurrentArtifactRoots(refs []CurrentArtifactRef, max int) ([]CurrentArtifactRef, error) {
	result, err := normalizeCurrentArtifactRefs(refs, max)
	if err != nil {
		return nil, err
	}
	for _, ref := range result {
		if ref.viewPath != "" {
			return nil, invalid("current Artifact semantic refs", "must not contain a materialized path")
		}
	}
	return result, nil
}

func bindCurrentArtifactViews(roots, supplied []CurrentArtifactRef) ([]CurrentArtifactRef, error) {
	if supplied == nil {
		return append([]CurrentArtifactRef{}, roots...), nil
	}
	views, err := normalizeCurrentArtifactRefs(supplied, MaxCurrentArtifactRefs)
	if err != nil {
		return nil, err
	}
	if len(views) != len(roots) {
		return nil, invariant("current projection Artifact views differ from authorized roots")
	}
	withPath := false
	withoutPath := false
	for index := range roots {
		if roots[index].RootDigest() != views[index].RootDigest() {
			return nil, invariant("current projection Artifact views differ from authorized roots")
		}
		_, ok := views[index].ViewPath()
		withPath = withPath || ok
		withoutPath = withoutPath || !ok
	}
	if withPath && withoutPath {
		return nil, invalid("current projection Artifact views", "must bind every authorized root or none")
	}
	return views, nil
}

func normalizeCurrentChildResults(children []CurrentChildResult, source WorkRef,
	action CurrentWork,
) ([]CurrentChildResult, error) {
	if len(children) == 0 {
		return nil, nil
	}
	if len(children) > MaxChildWorks {
		return nil, limit("current child results", len(children), MaxChildWorks)
	}
	if source.IsZero() || action.Ref().IsZero() || source == action.Ref() ||
		action.LocalRole() != CurrentReviewer ||
		(action.State() != WorkActive && action.State() != WorkRework) {
		return nil, invariant("parent-resume current must bind terminal children to a reviewer parent")
	}
	result := append([]CurrentChildResult{}, children...)
	seen := make(map[WorkRef]struct{}, len(result))
	sourceFound := false
	totalArtifacts := 0
	for index, child := range result {
		if child.Ordinal() != uint8(index) || child.WorkRef().IsZero() ||
			!child.State().Terminal() || child.WorkRef() == action.Ref() {
			return nil, invariant("current child results are not a canonical terminal set")
		}
		if _, exists := seen[child.WorkRef()]; exists {
			return nil, invalid("current child results", "contain duplicate child Work refs")
		}
		seen[child.WorkRef()] = struct{}{}
		sourceFound = sourceFound || child.WorkRef() == source
		totalArtifacts += len(child.ArtifactRefs())
		if totalArtifacts > MaxCurrentArtifactRefs {
			return nil, limit("current child result Artifact refs", totalArtifacts, MaxCurrentArtifactRefs)
		}
	}
	if !sourceFound {
		return nil, invariant("parent-resume source Event does not identify a child result")
	}
	return result, nil
}

func currentChildResultArtifactRefs(children []CurrentChildResult) []CurrentArtifactRef {
	var result []CurrentArtifactRef
	for _, child := range children {
		result = append(result, child.ArtifactRefs()...)
	}
	return result
}

func mergeCurrentArtifactRefs(groups ...[]CurrentArtifactRef) ([]CurrentArtifactRef, error) {
	var merged []CurrentArtifactRef
	for _, group := range groups {
		merged = append(merged, group...)
	}
	seen := make(map[Digest]struct{}, len(merged))
	result := make([]CurrentArtifactRef, 0, len(merged))
	for _, ref := range merged {
		if _, exists := seen[ref.RootDigest()]; exists {
			continue
		}
		seen[ref.RootDigest()] = struct{}{}
		result = append(result, ref)
	}
	return normalizeCurrentArtifactRefs(result, MaxCurrentArtifactRefs)
}
