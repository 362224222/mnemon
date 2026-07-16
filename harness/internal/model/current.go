package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// CurrentRole is the local Node's authority in the action Work. It is derived
// from the immutable Work participant snapshot, never supplied by an Agent.
type CurrentRole string

const (
	CurrentInitiator CurrentRole = "initiator"
	CurrentReviewer  CurrentRole = "reviewer"
)

func (r CurrentRole) Valid() bool { return r == CurrentInitiator || r == CurrentReviewer }

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

type CurrentBriefSpec struct {
	Content          string
	DeadlineUnixNano int64
	ArtifactRefs     []CurrentArtifactRef
}

// CurrentBrief is the persistent goal/constraints frozen by the Work's one
// immutable review.offered Event. It survives later state-transition Events.
type CurrentBrief struct {
	content          string
	deadlineUnixNano int64
	artifacts        []CurrentArtifactRef
}

func NewCurrentBrief(spec CurrentBriefSpec) (CurrentBrief, error) {
	if err := validateText("current Work brief", spec.Content, MaxContentBytes, false); err != nil {
		return CurrentBrief{}, err
	}
	if spec.DeadlineUnixNano <= 0 {
		return CurrentBrief{}, invalid("current Work brief", "positive deadline is required")
	}
	artifacts, err := normalizeCurrentArtifactRoots(spec.ArtifactRefs, MaxArtifactRefs)
	if err != nil {
		return CurrentBrief{}, err
	}
	return CurrentBrief{content: spec.Content, deadlineUnixNano: spec.DeadlineUnixNano,
		artifacts: artifacts}, nil
}

func (b CurrentBrief) IsZero() bool            { return b.content == "" || b.deadlineUnixNano <= 0 }
func (b CurrentBrief) Content() string         { return b.content }
func (b CurrentBrief) DeadlineUnixNano() int64 { return b.deadlineUnixNano }
func (b CurrentBrief) Deadline() time.Time     { return time.Unix(0, b.deadlineUnixNano).UTC() }
func (b CurrentBrief) ArtifactRefs() []CurrentArtifactRef {
	return append([]CurrentArtifactRef{}, b.artifacts...)
}

type CurrentEventSpec struct {
	Key          EventKey
	Digest       Digest
	Type         EventType
	WorkRef      WorkRef
	Summary      string
	Payload      JSON
	ArtifactRefs []CurrentArtifactRef
	AcceptedAt   time.Time
}

// CurrentEvent is the bounded, immutable Event subset presented by current.
// It deliberately excludes transport, roster and claim authority.
type CurrentEvent struct {
	spec      CurrentEventSpec
	artifacts []CurrentArtifactRef
}

func NewCurrentEvent(spec CurrentEventSpec) (CurrentEvent, error) {
	if spec.Key.IsZero() || spec.Digest.IsZero() || !spec.Type.Valid() || spec.WorkRef.IsZero() {
		return CurrentEvent{}, invalid("current Event", "key, digest, type and WorkRef are required")
	}
	if err := validateText("current Event summary", spec.Summary, MaxSummaryBytes, true); err != nil {
		return CurrentEvent{}, err
	}
	if spec.Payload.IsZero() || spec.Payload.String()[0] != '{' {
		return CurrentEvent{}, invalid("current Event payload", "must be a canonical JSON object")
	}
	artifacts, err := normalizeCurrentArtifactRoots(spec.ArtifactRefs, MaxCurrentArtifactRefs)
	if err != nil {
		return CurrentEvent{}, err
	}
	acceptedAt, err := canonicalTime(spec.AcceptedAt)
	if err != nil {
		return CurrentEvent{}, err
	}
	spec.ArtifactRefs = nil
	spec.AcceptedAt = acceptedAt
	return CurrentEvent{spec: spec, artifacts: artifacts}, nil
}

func (e CurrentEvent) Key() EventKey         { return e.spec.Key }
func (e CurrentEvent) Digest() Digest        { return e.spec.Digest }
func (e CurrentEvent) Type() EventType       { return e.spec.Type }
func (e CurrentEvent) WorkRef() WorkRef      { return e.spec.WorkRef }
func (e CurrentEvent) Summary() string       { return e.spec.Summary }
func (e CurrentEvent) Payload() JSON         { return e.spec.Payload }
func (e CurrentEvent) AcceptedAt() time.Time { return e.spec.AcceptedAt }
func (e CurrentEvent) ArtifactRefs() []CurrentArtifactRef {
	return append([]CurrentArtifactRef{}, e.artifacts...)
}

type CurrentWorkSpec struct {
	Ref              WorkRef
	Version          uint64
	Iteration        uint8
	DeadlineUnixNano int64
	State            WorkState
	StateData        JSON
	LocalRole        CurrentRole
	Brief            CurrentBrief
}

// CurrentWork is the exact Work version against which an Agent action is
// admitted. Participant identities stay in the Store; only the derived local
// role is projected.
type CurrentWork struct {
	spec     CurrentWorkSpec
	brief    CurrentBrief
	hasBrief bool
}

func NewCurrentWork(spec CurrentWorkSpec) (CurrentWork, error) {
	if spec.Ref.IsZero() || !spec.State.Valid() || !spec.LocalRole.Valid() {
		return CurrentWork{}, invalid("current Work", "WorkRef, state and local participant role are required")
	}
	if err := validateSQLitePositive("current Work version", spec.Version); err != nil {
		return CurrentWork{}, err
	}
	if spec.Iteration < 1 || spec.Iteration > 2 || spec.DeadlineUnixNano <= 0 {
		return CurrentWork{}, invalid("current Work", "iteration 1..2 and positive deadline are required")
	}
	if spec.StateData.IsZero() || spec.StateData.String()[0] != '{' {
		return CurrentWork{}, invalid("current Work state", "must be a canonical JSON object")
	}
	if spec.State == WorkOffered && (spec.Version != 1 || spec.Iteration != 1) {
		return CurrentWork{}, invariant("current OFFERED Work must be version 1 iteration 1")
	}
	if spec.State == WorkRework && spec.Iteration != 2 {
		return CurrentWork{}, invariant("current REWORK Work must be iteration 2")
	}
	if (spec.State == WorkActive || spec.State == WorkDeclined) && spec.Iteration != 1 {
		return CurrentWork{}, invariant("current ACTIVE and DECLINED Work must be iteration 1")
	}
	result := CurrentWork{spec: spec}
	result.spec.Brief = CurrentBrief{}
	if !spec.Brief.IsZero() {
		if spec.Brief.DeadlineUnixNano() != spec.DeadlineUnixNano {
			return CurrentWork{}, invariant("current Work brief deadline differs from Work deadline")
		}
		result.brief, result.hasBrief = spec.Brief, true
	}
	return result, nil
}

func (w CurrentWork) Ref() WorkRef            { return w.spec.Ref }
func (w CurrentWork) Version() uint64         { return w.spec.Version }
func (w CurrentWork) Iteration() uint8        { return w.spec.Iteration }
func (w CurrentWork) DeadlineUnixNano() int64 { return w.spec.DeadlineUnixNano }
func (w CurrentWork) Deadline() time.Time     { return time.Unix(0, w.spec.DeadlineUnixNano).UTC() }
func (w CurrentWork) State() WorkState        { return w.spec.State }
func (w CurrentWork) StateData() JSON         { return w.spec.StateData }
func (w CurrentWork) LocalRole() CurrentRole  { return w.spec.LocalRole }
func (w CurrentWork) Brief() (CurrentBrief, bool) {
	return w.brief, w.hasBrief
}

type CurrentProjectionSpec struct {
	SourceEvent    CurrentEvent
	ActionWork     CurrentWork
	AllowedActions []OperationKind
	ArtifactViews  []CurrentArtifactRef
}

// CurrentProjection is the public, bounded read model. The current schema has
// one source Event and one action Work. Future parent-resume projections use a
// new schema version instead of inventing child references in this base path.
type CurrentProjection struct {
	sourceEvent CurrentEvent
	actionWork  CurrentWork
	actions     []OperationKind
	artifacts   []CurrentArtifactRef
	canonical   JSON
	digest      Digest
}

func NewCurrentProjection(spec CurrentProjectionSpec) (CurrentProjection, error) {
	if spec.SourceEvent.Key().IsZero() || spec.ActionWork.Ref().IsZero() {
		return CurrentProjection{}, invalid("current projection", "source Event and action Work are required")
	}
	if spec.SourceEvent.WorkRef() != spec.ActionWork.Ref() {
		return CurrentProjection{}, invariant("base current source Event and action Work differ")
	}
	brief, hasBrief := spec.ActionWork.Brief()
	if !hasBrief {
		var err error
		brief, err = currentBriefFromOfferedSource(spec.SourceEvent, spec.ActionWork)
		if err != nil {
			return CurrentProjection{}, err
		}
		spec.ActionWork.brief, spec.ActionWork.hasBrief = brief, true
	}
	actions, err := normalizeCurrentActions(spec.AllowedActions)
	if err != nil {
		return CurrentProjection{}, err
	}
	if len(actions) == 0 {
		return CurrentProjection{}, invariant("current projection must allow an action or explicit resolution")
	}
	authorizedRoots, err := mergeCurrentArtifactRefs(brief.ArtifactRefs(), spec.SourceEvent.ArtifactRefs())
	if err != nil {
		return CurrentProjection{}, err
	}
	authorized, err := bindCurrentArtifactViews(authorizedRoots, spec.ArtifactViews)
	if err != nil {
		return CurrentProjection{}, err
	}
	result := CurrentProjection{sourceEvent: spec.SourceEvent, actionWork: spec.ActionWork,
		actions: actions, artifacts: authorized}
	canonical, err := JSONFrom(currentProjectionWireFrom(result))
	if err != nil {
		return CurrentProjection{}, fmt.Errorf("current projection: %w", err)
	}
	if len(canonical.Bytes()) > DefaultCurrentJSONBytes {
		return CurrentProjection{}, limit("current projection", len(canonical.Bytes()), DefaultCurrentJSONBytes)
	}
	result.canonical, result.digest = canonical, Sum(canonical.Bytes())
	return result, nil
}

func (p CurrentProjection) SourceEvent() CurrentEvent { return p.sourceEvent }
func (p CurrentProjection) ActionWork() CurrentWork   { return p.actionWork }
func (p CurrentProjection) AllowedActions() []OperationKind {
	return append([]OperationKind{}, p.actions...)
}
func (p CurrentProjection) ArtifactRefs() []CurrentArtifactRef {
	return append([]CurrentArtifactRef{}, p.artifacts...)
}
func (p CurrentProjection) HasMaterializedArtifactViews() bool {
	for _, ref := range p.artifacts {
		if _, ok := ref.ViewPath(); !ok {
			return false
		}
	}
	return true
}
func (p CurrentProjection) CanonicalJSON() JSON { return p.canonical }
func (p CurrentProjection) Digest() Digest      { return p.digest }
func (p CurrentProjection) MarshalJSON() ([]byte, error) {
	if p.canonical.IsZero() {
		return nil, invalid("current projection", "zero projection")
	}
	return p.canonical.MarshalJSON()
}

type CurrentReadReceiptSpec struct {
	RunID           RunID
	ProfileID       ProfileID
	HandlingID      HandlingID
	HandlingAttempt uint32
	Projection      CurrentProjection
	ReadAt          time.Time
}

// CurrentReadReceipt is write-once AgentRun evidence. It contains the exact
// projection so a response-loss replay never re-renders mutable Work state.
// It intentionally contains neither a claim token nor its hash.
type CurrentReadReceipt struct {
	runID           RunID
	profileID       ProfileID
	handlingID      HandlingID
	handlingAttempt uint32
	projection      CurrentProjection
	readAt          time.Time
	canonical       JSON
}

func NewCurrentReadReceipt(spec CurrentReadReceiptSpec) (CurrentReadReceipt, error) {
	if spec.RunID.IsZero() || spec.ProfileID != TeamworkProfileID() || spec.HandlingID.IsZero() ||
		spec.HandlingAttempt == 0 || spec.Projection.CanonicalJSON().IsZero() {
		return CurrentReadReceipt{}, invalid("current-read receipt", "Run, Profile, Handling, attempt and projection are required")
	}
	readAt, err := canonicalTime(spec.ReadAt)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	if readAt.Before(spec.Projection.SourceEvent().AcceptedAt()) {
		return CurrentReadReceipt{}, invariant("current-read receipt precedes source Event acceptance")
	}
	result := CurrentReadReceipt{runID: spec.RunID, profileID: spec.ProfileID,
		handlingID: spec.HandlingID, handlingAttempt: spec.HandlingAttempt,
		projection: spec.Projection, readAt: readAt}
	canonical, err := JSONFrom(currentReceiptWireFrom(result))
	if err != nil {
		return CurrentReadReceipt{}, fmt.Errorf("current-read receipt: %w", err)
	}
	result.canonical = canonical
	return result, nil
}

func (r CurrentReadReceipt) RunID() RunID                    { return r.runID }
func (r CurrentReadReceipt) ProfileID() ProfileID            { return r.profileID }
func (r CurrentReadReceipt) HandlingID() HandlingID          { return r.handlingID }
func (r CurrentReadReceipt) HandlingAttempt() uint32         { return r.handlingAttempt }
func (r CurrentReadReceipt) SourceEvent() EventKey           { return r.projection.SourceEvent().Key() }
func (r CurrentReadReceipt) ActionWork() WorkRef             { return r.projection.ActionWork().Ref() }
func (r CurrentReadReceipt) ActionWorkVersion() uint64       { return r.projection.ActionWork().Version() }
func (r CurrentReadReceipt) AllowedActions() []OperationKind { return r.projection.AllowedActions() }
func (r CurrentReadReceipt) ArtifactRefs() []CurrentArtifactRef {
	return r.projection.ArtifactRefs()
}
func (r CurrentReadReceipt) Projection() CurrentProjection { return r.projection }
func (r CurrentReadReceipt) ProjectionDigest() Digest      { return r.projection.Digest() }
func (r CurrentReadReceipt) ReadAt() time.Time             { return r.readAt }
func (r CurrentReadReceipt) CanonicalJSON() JSON           { return r.canonical }
func (r CurrentReadReceipt) MarshalJSON() ([]byte, error) {
	if r.canonical.IsZero() {
		return nil, invalid("current-read receipt", "zero receipt")
	}
	return r.canonical.MarshalJSON()
}

func currentBriefFromOfferedSource(event CurrentEvent, work CurrentWork) (CurrentBrief, error) {
	if event.Type() != EventReviewOffered || event.WorkRef() != work.Ref() {
		return CurrentBrief{}, invariant("current Work has no durable offered brief")
	}
	var payload struct {
		Content     string `json:"content"`
		Deadline    string `json:"deadline"`
		Iteration   uint8  `json:"iteration"`
		WorkVersion uint64 `json:"work_version"`
	}
	decoder := json.NewDecoder(bytes.NewReader(event.Payload().Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return CurrentBrief{}, invalid("current offered brief", "payload does not match review.offered")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return CurrentBrief{}, invalid("current offered brief", "payload contains trailing data")
	}
	deadline, err := time.Parse(time.RFC3339Nano, payload.Deadline)
	if err != nil || deadline.UTC().Format(time.RFC3339Nano) != payload.Deadline ||
		deadline.UnixNano() != work.DeadlineUnixNano() || payload.WorkVersion != 1 || payload.Iteration != 1 {
		return CurrentBrief{}, invariant("current offered brief does not bind Work deadline/version/iteration")
	}
	return NewCurrentBrief(CurrentBriefSpec{Content: payload.Content,
		DeadlineUnixNano: work.DeadlineUnixNano(), ArtifactRefs: event.ArtifactRefs()})
}

func normalizeCurrentActions(actions []OperationKind) ([]OperationKind, error) {
	if len(actions) == 0 || len(actions) > 10 {
		return nil, invalid("current allowed actions", "must contain 1..10 closed actions")
	}
	result := append([]OperationKind{}, actions...)
	for _, action := range result {
		if !action.Valid() {
			return nil, invalid("current allowed actions", "contains an unknown action")
		}
	}
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && currentActionRank(result[j]) < currentActionRank(result[j-1]); j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, invalid("current allowed actions", "contains a duplicate action")
		}
	}
	return result, nil
}

func currentActionRank(action OperationKind) int {
	switch action {
	case OperationTeamworkOffer:
		return 0
	case OperationTeamworkAccept:
		return 1
	case OperationTeamworkDecline:
		return 2
	case OperationTeamworkDeliver:
		return 3
	case OperationTeamworkRework:
		return 4
	case OperationTeamworkClose:
		return 5
	case OperationTeamworkCancel:
		return 6
	case OperationResolveNoAction:
		return 7
	case OperationResolveRetry:
		return 8
	case OperationResolveReject:
		return 9
	default:
		return 10
	}
}

type currentWorkRefWire struct {
	HomePeerID string `json:"home_peer_id"`
	WorkID     string `json:"work_id"`
}

type currentEventKeyWire struct {
	OriginPeerID string `json:"origin_peer_id"`
	OriginEpoch  string `json:"origin_epoch"`
	EventID      string `json:"event_id"`
}

type currentArtifactWire struct {
	RootDigest string `json:"root_digest"`
	Path       string `json:"path,omitempty"`
}

type currentEventWire struct {
	AcceptedAt  string                `json:"accepted_at"`
	ArtifactRef []currentArtifactWire `json:"artifact_refs"`
	EventDigest string                `json:"event_digest"`
	EventKey    currentEventKeyWire   `json:"event_key"`
	EventType   EventType             `json:"event_type"`
	Payload     json.RawMessage       `json:"payload"`
	Summary     string                `json:"summary"`
	WorkRef     currentWorkRefWire    `json:"work_ref"`
}

type currentBriefWire struct {
	ArtifactRefs     []currentArtifactWire `json:"offer_artifact_refs"`
	Content          string                `json:"content"`
	DeadlineUnixNano int64                 `json:"deadline_unix_nano"`
}

type currentWorkWire struct {
	Brief            currentBriefWire   `json:"brief"`
	DeadlineUnixNano int64              `json:"deadline_unix_nano"`
	Iteration        uint8              `json:"iteration"`
	LocalRole        CurrentRole        `json:"local_role"`
	Ref              currentWorkRefWire `json:"ref"`
	State            WorkState          `json:"state"`
	StateData        json.RawMessage    `json:"state_data"`
	Version          uint64             `json:"version"`
}

type currentProjectionWire struct {
	ActionWork     currentWorkWire       `json:"action_work"`
	AllowedActions []OperationKind       `json:"allowed_actions"`
	ArtifactRefs   []currentArtifactWire `json:"artifact_refs"`
	SchemaVersion  int                   `json:"schema_version"`
	SourceEvent    currentEventWire      `json:"source_event"`
}

type currentReceiptWire struct {
	ActionWorkVersion uint64                `json:"action_work_version"`
	ActionWork        currentWorkRefWire    `json:"action_work"`
	AllowedActions    []OperationKind       `json:"allowed_actions"`
	ArtifactRefs      []currentArtifactWire `json:"artifact_refs"`
	HandlingAttempt   uint32                `json:"handling_attempt"`
	HandlingID        string                `json:"handling_id"`
	ProfileID         string                `json:"profile_id"`
	Projection        currentProjectionWire `json:"projection"`
	ProjectionDigest  string                `json:"projection_digest"`
	ReadAt            string                `json:"read_at"`
	RunID             string                `json:"run_id"`
	SchemaVersion     int                   `json:"schema_version"`
	SourceEvent       currentEventKeyWire   `json:"source_event"`
}

func currentProjectionWireFrom(projection CurrentProjection) currentProjectionWire {
	event := projection.SourceEvent()
	work := projection.ActionWork()
	brief, _ := work.Brief()
	return currentProjectionWire{
		SchemaVersion: SchemaVersion,
		SourceEvent: currentEventWire{
			EventKey:    currentEventKeyWire{event.Key().OriginPeerID().String(), event.Key().OriginEpoch().String(), event.Key().EventID().String()},
			EventDigest: event.Digest().String(), EventType: event.Type(),
			WorkRef: currentWorkRefWire{event.WorkRef().HomePeerID().String(), event.WorkRef().WorkID().String()},
			Summary: event.Summary(), Payload: event.Payload().Bytes(),
			ArtifactRef: artifactWires(event.ArtifactRefs()), AcceptedAt: formatTime(event.AcceptedAt()),
		},
		ActionWork: currentWorkWire{
			Brief: currentBriefWire{ArtifactRefs: artifactWires(brief.ArtifactRefs()),
				Content: brief.Content(), DeadlineUnixNano: brief.DeadlineUnixNano()},
			Ref:     currentWorkRefWire{work.Ref().HomePeerID().String(), work.Ref().WorkID().String()},
			Version: work.Version(), Iteration: work.Iteration(), DeadlineUnixNano: work.DeadlineUnixNano(),
			State: work.State(), StateData: work.StateData().Bytes(), LocalRole: work.LocalRole(),
		},
		AllowedActions: projection.AllowedActions(), ArtifactRefs: artifactWires(projection.ArtifactRefs()),
	}
}

// ParseCurrentProjection accepts only the exact canonical schema-v1 public
// projection. It is used by the Agent-side CLI before adding its local
// context-file reference, so unknown or authority-bearing fields can never be
// reflected into the model prompt.
func ParseCurrentProjection(raw []byte) (CurrentProjection, error) {
	canonical, err := NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return CurrentProjection{}, invalid("current projection", "must use exact canonical JSON")
	}
	var wire currentProjectionWire
	if err := decodeCurrentWire(raw, &wire); err != nil {
		return CurrentProjection{}, err
	}
	if wire.SchemaVersion != SchemaVersion {
		return CurrentProjection{}, invalid("current projection", "unsupported schema version")
	}
	projection, err := currentProjectionFromWire(wire)
	if err != nil {
		return CurrentProjection{}, err
	}
	if !bytes.Equal(projection.CanonicalJSON().Bytes(), raw) {
		return CurrentProjection{}, invalid("current projection", "omits or changes a schema-v1 field")
	}
	return projection, nil
}

func currentReceiptWireFrom(receipt CurrentReadReceipt) currentReceiptWire {
	projection := receipt.Projection()
	return currentReceiptWire{
		SchemaVersion: SchemaVersion, RunID: receipt.RunID().String(), ProfileID: receipt.ProfileID().String(),
		HandlingID: receipt.HandlingID().String(), HandlingAttempt: receipt.HandlingAttempt(),
		SourceEvent:       currentEventKeyWire{receipt.SourceEvent().OriginPeerID().String(), receipt.SourceEvent().OriginEpoch().String(), receipt.SourceEvent().EventID().String()},
		ActionWork:        currentWorkRefWire{receipt.ActionWork().HomePeerID().String(), receipt.ActionWork().WorkID().String()},
		ActionWorkVersion: receipt.ActionWorkVersion(), AllowedActions: receipt.AllowedActions(),
		ArtifactRefs: artifactWires(receipt.ArtifactRefs()), Projection: currentProjectionWireFrom(projection),
		ProjectionDigest: receipt.ProjectionDigest().String(), ReadAt: formatTime(receipt.ReadAt()),
	}
}

func artifactWires(refs []CurrentArtifactRef) []currentArtifactWire {
	result := make([]currentArtifactWire, len(refs))
	for index, ref := range refs {
		path, _ := ref.ViewPath()
		result[index] = currentArtifactWire{RootDigest: ref.RootDigest().String(), Path: path}
	}
	return result
}

// ParseCurrentReadReceipt accepts only the exact canonical schema-v1 shape.
// Reconstructing through constructors detects unknown fields, noncanonical
// ordering and disagreement between duplicated binding fields and projection.
func ParseCurrentReadReceipt(raw []byte) (CurrentReadReceipt, error) {
	canonical, err := NewJSON(raw)
	if err != nil {
		return CurrentReadReceipt{}, fmt.Errorf("parse current-read receipt: %w", err)
	}
	if !bytes.Equal(canonical.Bytes(), raw) {
		return CurrentReadReceipt{}, invalid("current-read receipt", "must use exact canonical JSON")
	}
	var wire currentReceiptWire
	if err := decodeCurrentWire(raw, &wire); err != nil {
		return CurrentReadReceipt{}, err
	}
	if wire.SchemaVersion != SchemaVersion {
		return CurrentReadReceipt{}, invalid("current-read receipt", "unsupported schema version")
	}
	projection, err := currentProjectionFromWire(wire.Projection)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	runID, err := ParseRunID(wire.RunID)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	profileID, err := ParseProfileID(wire.ProfileID)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	handlingID, err := ParseHandlingID(wire.HandlingID)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	readAt, err := time.Parse(time.RFC3339Nano, wire.ReadAt)
	if err != nil {
		return CurrentReadReceipt{}, invalid("current-read receipt read_at", "must be RFC3339Nano")
	}
	receipt, err := NewCurrentReadReceipt(CurrentReadReceiptSpec{RunID: runID, ProfileID: profileID,
		HandlingID: handlingID, HandlingAttempt: wire.HandlingAttempt, Projection: projection, ReadAt: readAt})
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	source, err := eventKeyFromWire(wire.SourceEvent)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	actionWork, err := workRefFromWire(wire.ActionWork)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	actions, err := normalizeCurrentActions(wire.AllowedActions)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	artifacts, err := artifactRefsFromWire(wire.ArtifactRefs, MaxCurrentArtifactRefs)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	projectionDigest, err := ParseDigest(wire.ProjectionDigest)
	if err != nil {
		return CurrentReadReceipt{}, err
	}
	if source != receipt.SourceEvent() || actionWork != receipt.ActionWork() ||
		wire.ActionWorkVersion != receipt.ActionWorkVersion() || projectionDigest != receipt.ProjectionDigest() ||
		!equalCurrentActions(actions, receipt.AllowedActions()) || !equalArtifactRefs(artifacts, receipt.ArtifactRefs()) {
		return CurrentReadReceipt{}, invariant("current-read receipt binding fields differ from projection")
	}
	if !bytes.Equal(receipt.CanonicalJSON().Bytes(), raw) {
		return CurrentReadReceipt{}, invalid("current-read receipt", "field order or collection order is not canonical")
	}
	return receipt, nil
}

func currentProjectionFromWire(wire currentProjectionWire) (CurrentProjection, error) {
	if wire.SchemaVersion != SchemaVersion {
		return CurrentProjection{}, invalid("current projection", "unsupported schema version")
	}
	eventKey, err := eventKeyFromWire(wire.SourceEvent.EventKey)
	if err != nil {
		return CurrentProjection{}, err
	}
	eventDigest, err := ParseDigest(wire.SourceEvent.EventDigest)
	if err != nil {
		return CurrentProjection{}, err
	}
	eventWork, err := workRefFromWire(wire.SourceEvent.WorkRef)
	if err != nil {
		return CurrentProjection{}, err
	}
	payload, err := NewJSON(wire.SourceEvent.Payload)
	if err != nil {
		return CurrentProjection{}, err
	}
	artifacts, err := artifactRefsFromWire(wire.SourceEvent.ArtifactRef, MaxCurrentArtifactRefs)
	if err != nil {
		return CurrentProjection{}, err
	}
	acceptedAt, err := time.Parse(time.RFC3339Nano, wire.SourceEvent.AcceptedAt)
	if err != nil {
		return CurrentProjection{}, invalid("current Event accepted_at", "must be RFC3339Nano")
	}
	event, err := NewCurrentEvent(CurrentEventSpec{Key: eventKey, Digest: eventDigest,
		Type: wire.SourceEvent.EventType, WorkRef: eventWork, Summary: wire.SourceEvent.Summary,
		Payload: payload, ArtifactRefs: artifacts, AcceptedAt: acceptedAt})
	if err != nil {
		return CurrentProjection{}, err
	}
	workRef, err := workRefFromWire(wire.ActionWork.Ref)
	if err != nil {
		return CurrentProjection{}, err
	}
	state, err := NewJSON(wire.ActionWork.StateData)
	if err != nil {
		return CurrentProjection{}, err
	}
	briefArtifacts, err := artifactRefsFromWire(wire.ActionWork.Brief.ArtifactRefs, MaxArtifactRefs)
	if err != nil {
		return CurrentProjection{}, err
	}
	brief, err := NewCurrentBrief(CurrentBriefSpec{Content: wire.ActionWork.Brief.Content,
		DeadlineUnixNano: wire.ActionWork.Brief.DeadlineUnixNano, ArtifactRefs: briefArtifacts})
	if err != nil {
		return CurrentProjection{}, err
	}
	work, err := NewCurrentWork(CurrentWorkSpec{Ref: workRef, Version: wire.ActionWork.Version,
		Iteration: wire.ActionWork.Iteration, DeadlineUnixNano: wire.ActionWork.DeadlineUnixNano,
		State: wire.ActionWork.State, StateData: state, LocalRole: wire.ActionWork.LocalRole, Brief: brief})
	if err != nil {
		return CurrentProjection{}, err
	}
	authorized, err := artifactRefsFromWire(wire.ArtifactRefs, MaxCurrentArtifactRefs)
	if err != nil {
		return CurrentProjection{}, err
	}
	projection, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: event, ActionWork: work,
		AllowedActions: wire.AllowedActions, ArtifactViews: authorized})
	if err != nil {
		return CurrentProjection{}, err
	}
	return projection, nil
}

func decodeCurrentWire(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("parse current-read receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid("current-read receipt", "unexpected trailing JSON value")
	}
	return nil
}

func eventKeyFromWire(wire currentEventKeyWire) (EventKey, error) {
	peer, err := ParsePeerID(wire.OriginPeerID)
	if err != nil {
		return EventKey{}, err
	}
	epoch, err := ParseOriginEpoch(wire.OriginEpoch)
	if err != nil {
		return EventKey{}, err
	}
	eventID, err := ParseEventID(wire.EventID)
	if err != nil {
		return EventKey{}, err
	}
	return NewEventKey(peer, epoch, eventID)
}

func workRefFromWire(wire currentWorkRefWire) (WorkRef, error) {
	peer, err := ParsePeerID(wire.HomePeerID)
	if err != nil {
		return WorkRef{}, err
	}
	workID, err := ParseWorkID(wire.WorkID)
	if err != nil {
		return WorkRef{}, err
	}
	return NewWorkRef(peer, workID)
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

func mergeCurrentArtifactRefs(left, right []CurrentArtifactRef) ([]CurrentArtifactRef, error) {
	merged := append([]CurrentArtifactRef{}, left...)
	seen := make(map[Digest]struct{}, len(merged))
	for _, ref := range merged {
		seen[ref.RootDigest()] = struct{}{}
	}
	for _, ref := range right {
		if _, exists := seen[ref.RootDigest()]; exists {
			continue
		}
		seen[ref.RootDigest()] = struct{}{}
		merged = append(merged, ref)
	}
	return normalizeCurrentArtifactRefs(merged, MaxCurrentArtifactRefs)
}
