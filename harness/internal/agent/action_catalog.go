package agent

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

const teamworkActionAssetPrefix = "actions/teamwork/"

// ActionAssetProvider is the Agent-owned structural view of one validated
// managed asset bundle. Revision covers the whole manifest, not only Action
// files. Implementations must return the paths in manifest order.
type ActionAssetProvider interface {
	Revision() string
	TeamworkActionPaths() []string
	ReadTeamworkAction(string) ([]byte, error)
}

// ActionPolicy is an immutable Agent projection of the canonical Teamwork
// Action assets. Its zero value is inert and fails every lookup closed.
type ActionPolicy struct {
	catalog teamwork.ActionCatalog
}

// NewActionPolicy snapshots and parses exactly one stable managed asset
// revision. A provider is read only after its complete, canonical Action path
// set has been established.
func NewActionPolicy(provider ActionAssetProvider) (ActionPolicy, error) {
	if provider == nil {
		return ActionPolicy{}, errors.New("compose Agent Action policy: asset provider is unavailable")
	}
	revisionText := provider.Revision()
	revision, err := model.ParseDigest(revisionText)
	if err != nil || revision.IsZero() {
		return ActionPolicy{}, errors.New("compose Agent Action policy: asset revision is invalid")
	}
	providedPaths := provider.TeamworkActionPaths()
	if err := validateTeamworkActionPaths(providedPaths); err != nil {
		return ActionPolicy{}, fmt.Errorf("compose Agent Action policy: %w", err)
	}
	paths := append([]string(nil), providedPaths...)

	sources := make([]teamwork.ActionSource, len(paths))
	for index, path := range paths {
		raw, readErr := provider.ReadTeamworkAction(path)
		if readErr != nil {
			return ActionPolicy{}, fmt.Errorf("compose Agent Action policy: read %s: %w", path, readErr)
		}
		sources[index] = teamwork.NewActionSource(path, raw)
	}
	catalog, err := teamwork.ParseActionCatalog(revision, sources)
	if err != nil {
		return ActionPolicy{}, fmt.Errorf("compose Agent Action policy: %w", err)
	}
	if provider.Revision() != revisionText {
		return ActionPolicy{}, errors.New("compose Agent Action policy: asset revision changed while reading Actions")
	}
	return ActionPolicy{catalog: catalog}, nil
}

func validateTeamworkActionPaths(paths []string) error {
	if len(paths) != teamwork.TeamworkActionCount {
		return fmt.Errorf("got %d Teamwork Action paths, want %d", len(paths), teamwork.TeamworkActionCount)
	}
	for index, path := range paths {
		if index > 0 && paths[index-1] >= path {
			return errors.New("Teamwork Action paths are not unique strict lexical order")
		}
		if !canonicalTeamworkActionPath(path) {
			return fmt.Errorf("Teamwork Action path %d is not canonical", index)
		}
	}
	return nil
}

func canonicalTeamworkActionPath(path string) bool {
	name, found := strings.CutPrefix(path, teamworkActionAssetPrefix)
	base, jsonFile := strings.CutSuffix(name, ".json")
	return found && jsonFile && base != "" && base != "." && base != ".." && utf8.ValidString(base) &&
		len(base) <= model.MaxIdentifierBytes &&
		!strings.ContainsAny(base, "/\\")
}

func (policy ActionPolicy) AssetRevision() model.Digest { return policy.catalog.AssetRevision() }
func (policy ActionPolicy) Actions() []teamwork.ActionDescriptor {
	return policy.catalog.Actions()
}
func (policy ActionPolicy) Action(name string) (teamwork.ActionDescriptor, bool) {
	return policy.catalog.Action(name)
}
func (policy ActionPolicy) Operation(kind model.OperationKind) (teamwork.ActionDescriptor, bool) {
	return policy.catalog.Operation(kind)
}
