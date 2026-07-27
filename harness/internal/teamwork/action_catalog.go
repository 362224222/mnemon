package teamwork

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	ActionSchemaVersion  = 1
	TeamworkActionCount  = model.TeamworkActionCount
	maxActionSourceBytes = 16 << 10
)

var ErrInvalidActionCatalog = errors.New("invalid Teamwork action catalog")

type ActionContext = model.TeamworkActionContext

const (
	ActionContextNone                    = model.TeamworkActionContextNone
	ActionContextReviewerOffered         = model.TeamworkActionContextReviewerOffered
	ActionContextReviewerActive          = model.TeamworkActionContextReviewerActive
	ActionContextReviewerRework          = model.TeamworkActionContextReviewerRework
	ActionContextParentResume            = model.TeamworkActionContextParentResume
	ActionContextHomeDelivered           = model.TeamworkActionContextHomeDelivered
	ActionContextHomeDeliveredIteration1 = model.TeamworkActionContextHomeDeliveredIteration1
	ActionContextHomeNonterminal         = model.TeamworkActionContextHomeNonterminal
)

type ParticipantSelector string

const (
	ParticipantEffectiveAlias ParticipantSelector = "effective_alias"
)

type ContentSource string
type SelectorChannelMode string
type ReceiptHandling string
type ReceiptStatus string

const (
	ContentFileOrStdin              ContentSource       = "content_file_or_stdin"
	SelectorOptionalWhenUnambiguous SelectorChannelMode = "optional_when_unambiguous"
	ReceiptHandlingCompleted        ReceiptHandling     = "completed"
	ReceiptHandlingContextDependent ReceiptHandling     = "context_dependent"
	ReceiptStatusAccepted           ReceiptStatus       = "accepted"
)

type ActionSource struct {
	path string
	raw  []byte
}

func NewActionSource(path string, raw []byte) ActionSource {
	return ActionSource{path: path, raw: append([]byte(nil), raw...)}
}

func (s ActionSource) Path() string  { return s.path }
func (s ActionSource) Bytes() []byte { return append([]byte(nil), s.raw...) }

type ActionContentPolicy struct {
	maxBytes uint32
	required bool
	source   ContentSource
}

func (p ActionContentPolicy) MaxBytes() uint32      { return p.maxBytes }
func (p ActionContentPolicy) Required() bool        { return p.required }
func (p ActionContentPolicy) Source() ContentSource { return p.source }

type ActionArtifactPolicy struct {
	allowed       bool
	maxEntries    uint32
	maxPathBytes  uint32
	maxRoots      uint8
	maxTotalBytes uint64
}

func (p ActionArtifactPolicy) Allowed() bool         { return p.allowed }
func (p ActionArtifactPolicy) MaxEntries() uint32    { return p.maxEntries }
func (p ActionArtifactPolicy) MaxPathBytes() uint32  { return p.maxPathBytes }
func (p ActionArtifactPolicy) MaxRoots() uint8       { return p.maxRoots }
func (p ActionArtifactPolicy) MaxTotalBytes() uint64 { return p.maxTotalBytes }

type ActionDeadlinePolicy struct {
	defaultDuration time.Duration
	minimumDuration time.Duration
	maximumDuration time.Duration
}

func (p ActionDeadlinePolicy) Default() time.Duration { return p.defaultDuration }
func (p ActionDeadlinePolicy) Minimum() time.Duration { return p.minimumDuration }
func (p ActionDeadlinePolicy) Maximum() time.Duration { return p.maximumDuration }

type ActionSelectorPolicy struct {
	channel     SelectorChannelMode
	participant ParticipantSelector
}

func (p ActionSelectorPolicy) Channel() SelectorChannelMode     { return p.channel }
func (p ActionSelectorPolicy) Participant() ParticipantSelector { return p.participant }

type ActionReceiptPolicy struct {
	action     model.OperationKind
	handling   ReceiptHandling
	maxResults uint8
	status     ReceiptStatus
}

func (p ActionReceiptPolicy) Action() model.OperationKind { return p.action }
func (p ActionReceiptPolicy) Handling() ReceiptHandling   { return p.handling }
func (p ActionReceiptPolicy) MaxResults() uint8           { return p.maxResults }
func (p ActionReceiptPolicy) Status() ReceiptStatus       { return p.status }

type ActionDescriptor struct {
	name, path    string
	schemaVersion int
	ordinal       uint8
	operation     model.OperationKind
	raw           []byte
	contexts      [model.MaxTeamworkActionContexts]ActionContext
	contextLen    uint8
	content       ActionContentPolicy
	artifacts     ActionArtifactPolicy
	deadline      ActionDeadlinePolicy
	hasDeadline   bool
	selectors     ActionSelectorPolicy
	hasSelectors  bool
	receipt       ActionReceiptPolicy
}

func (d ActionDescriptor) Name() string                       { return d.name }
func (d ActionDescriptor) SourcePath() string                 { return d.path }
func (d ActionDescriptor) SchemaVersion() int                 { return d.schemaVersion }
func (d ActionDescriptor) Ordinal() uint8                     { return d.ordinal }
func (d ActionDescriptor) OperationKind() model.OperationKind { return d.operation }
func (d ActionDescriptor) SourceBytes() []byte                { return append([]byte(nil), d.raw...) }
func (d ActionDescriptor) AllowedContexts() []ActionContext {
	return append([]ActionContext(nil), d.contexts[:d.contextLen]...)
}
func (d ActionDescriptor) AllowsContext(context ActionContext) bool {
	for _, candidate := range d.contexts[:d.contextLen] {
		if candidate == context {
			return true
		}
	}
	return false
}
func (d ActionDescriptor) Content() ActionContentPolicy    { return d.content }
func (d ActionDescriptor) Artifacts() ActionArtifactPolicy { return d.artifacts }
func (d ActionDescriptor) Deadline() (ActionDeadlinePolicy, bool) {
	return d.deadline, d.hasDeadline
}
func (d ActionDescriptor) Selectors() (ActionSelectorPolicy, bool) {
	return d.selectors, d.hasSelectors
}
func (d ActionDescriptor) Receipt() ActionReceiptPolicy { return d.receipt }

type ActionCatalog struct {
	actions       [TeamworkActionCount]ActionDescriptor
	assetRevision model.Digest
	ready         bool
}

func (c ActionCatalog) AssetRevision() model.Digest { return c.assetRevision }
func (c ActionCatalog) Actions() []ActionDescriptor {
	if !c.ready {
		return nil
	}
	return append([]ActionDescriptor(nil), c.actions[:]...)
}
func (c ActionCatalog) Action(name string) (ActionDescriptor, bool) {
	if !c.ready {
		return ActionDescriptor{}, false
	}
	for _, action := range c.actions {
		if action.name == name {
			return action, true
		}
	}
	return ActionDescriptor{}, false
}
func (c ActionCatalog) Operation(kind model.OperationKind) (ActionDescriptor, bool) {
	if !c.ready {
		return ActionDescriptor{}, false
	}
	for _, action := range c.actions {
		if action.operation == kind {
			return action, true
		}
	}
	return ActionDescriptor{}, false
}

func ParseActionCatalog(assetRevision model.Digest, sources []ActionSource) (ActionCatalog, error) {
	if assetRevision.IsZero() {
		return ActionCatalog{}, catalogError("whole-manifest asset revision is required")
	}
	if len(sources) != TeamworkActionCount {
		return ActionCatalog{}, catalogError("got %d sources, want %d", len(sources), TeamworkActionCount)
	}
	ordered := append([]ActionSource(nil), sources...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].path < ordered[j].path })
	catalog := ActionCatalog{assetRevision: assetRevision}
	var seenOrdinals [TeamworkActionCount]bool
	for sourceIndex, source := range ordered {
		if sourceIndex > 0 && ordered[sourceIndex-1].path == source.path {
			return ActionCatalog{}, catalogError("duplicate source path %q", source.path)
		}
		descriptor, ordinal, err := parseActionSource(source, catalog.actions, seenOrdinals)
		if err != nil {
			return ActionCatalog{}, err
		}
		catalog.actions[ordinal], seenOrdinals[ordinal] = descriptor, true
	}
	for ordinal, present := range seenOrdinals {
		if !present {
			return ActionCatalog{}, catalogError("semantic ordinal %d is missing", ordinal)
		}
	}
	catalog.ready = true
	return catalog, nil
}

func parseActionSource(source ActionSource, actions [TeamworkActionCount]ActionDescriptor,
	seenOrdinals [TeamworkActionCount]bool,
) (ActionDescriptor, uint8, error) {
	if len(source.raw) == 0 || len(source.raw) > maxActionSourceBytes {
		return ActionDescriptor{}, 0, catalogError("%s has invalid bounded source size", source.path)
	}
	wire, err := decodeActionWire(source.raw)
	if err != nil {
		return ActionDescriptor{}, 0, catalogError("%s: %v", source.path, err)
	}
	if int(wire.Ordinal) >= TeamworkActionCount || seenOrdinals[wire.Ordinal] {
		return ActionDescriptor{}, 0,
			catalogError("%s ordinal %d is outside unique dense range", source.path, wire.Ordinal)
	}
	operation := model.OperationKind(wire.Receipt.Action)
	if wire.Action == "" || source.path != "actions/teamwork/"+wire.Action+".json" ||
		wire.Receipt.Action != "teamwork."+wire.Action || !strings.HasPrefix(string(operation), "teamwork.") ||
		!operation.Valid() || duplicateActionIdentity(actions[:], wire.Action, operation) {
		return ActionDescriptor{}, 0, catalogError("%s action/operation binding is invalid", source.path)
	}
	descriptor, err := projectAction(source, wire, operation)
	if err != nil {
		return ActionDescriptor{}, 0, catalogError("%s: %v", source.path, err)
	}
	return descriptor, wire.Ordinal, nil
}

func duplicateActionIdentity(actions []ActionDescriptor, name string, operation model.OperationKind) bool {
	for _, action := range actions {
		if action.name == name || action.operation == operation {
			return true
		}
	}
	return false
}

type actionWire struct {
	Action         string              `json:"action"`
	AllowedContext []string            `json:"allowed_context"`
	Artifacts      actionArtifactWire  `json:"artifacts"`
	Content        actionContentWire   `json:"content"`
	Deadline       *actionDeadlineWire `json:"deadline"`
	Ordinal        uint8               `json:"ordinal"`
	Receipt        actionReceiptWire   `json:"receipt"`
	SchemaVersion  int                 `json:"schema_version"`
	Selectors      *actionSelectorWire `json:"selectors"`
}
type actionArtifactWire struct {
	Allowed       bool   `json:"allowed"`
	MaxEntries    uint32 `json:"max_entries"`
	MaxPathBytes  uint32 `json:"max_path_bytes"`
	MaxRoots      uint8  `json:"max_roots"`
	MaxTotalBytes uint64 `json:"max_total_bytes"`
}
type actionContentWire struct {
	MaxBytes uint32        `json:"max_bytes"`
	Required bool          `json:"required"`
	Source   ContentSource `json:"source"`
}
type actionDeadlineWire struct {
	Default string `json:"default"`
	Maximum string `json:"maximum"`
	Minimum string `json:"minimum"`
}
type actionReceiptWire struct {
	Action     string          `json:"action"`
	Handling   ReceiptHandling `json:"handling"`
	MaxResults uint8           `json:"max_results"`
	Status     ReceiptStatus   `json:"status"`
}
type actionSelectorWire struct {
	Channel     SelectorChannelMode `json:"channel"`
	Participant ParticipantSelector `json:"participant"`
}

func decodeActionWire(raw []byte) (actionWire, error) {
	canonicalRaw := bytes.TrimSuffix(raw, []byte("\n"))
	var wire actionWire
	decoder := json.NewDecoder(bytes.NewReader(canonicalRaw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return actionWire{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return actionWire{}, errors.New("JSON contains trailing data")
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, canonicalRaw) {
		return actionWire{}, errors.New("JSON is not exact canonical closed-schema bytes")
	}
	return wire, nil
}

func catalogError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidActionCatalog, fmt.Sprintf(format, args...))
}
