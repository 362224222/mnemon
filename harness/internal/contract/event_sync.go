package contract

import (
	"fmt"
	"strconv"
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func SyncedEventEnvelopeFromMaterial(material SyncedEventMaterial) (eventmodel.EventEnvelope, error) {
	subject := eventmodel.Subject(string(material.ResourceRef.Kind), string(material.ResourceRef.ID))
	ev := eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            syncedEventID(material.LocalDecisionID, subject),
		Type:          string(material.ResourceRef.Kind) + ".accepted",
		Subject:       subject,
		Actor:         string(material.Actor),
		Payload: map[string]any{
			"resource_version": int64(material.ResourceVersion),
			"fields":           cloneSyncFields(material.Fields),
		},
		CorrelationID: material.CorrelationID,
		CreatedAt:     material.DecidedAt,
	}
	env := eventmodel.SyncedEnvelope(ev, material.OriginReplicaID, strconv.FormatInt(material.LocalIngestSeq, 10), material.FieldsDigest, material.DecidedAt)
	if err := env.Validate(); err != nil {
		return eventmodel.EventEnvelope{}, err
	}
	return env, nil
}

func SyncedEventMaterialFromEnvelope(env eventmodel.EventEnvelope) (SyncedEventMaterial, error) {
	if env.Phase != eventmodel.PhaseSynced {
		return SyncedEventMaterial{}, fmt.Errorf("sync event must have phase %q, got %q", eventmodel.PhaseSynced, env.Phase)
	}
	if err := env.Validate(); err != nil {
		return SyncedEventMaterial{}, err
	}
	ref, err := ResourceRefFromEventSubject(env.Event.Subject)
	if err != nil {
		return SyncedEventMaterial{}, err
	}
	decisionID := DecisionIDFromEventID(env.Event.ID)
	if decisionID == "" {
		return SyncedEventMaterial{}, fmt.Errorf("synced event %q does not carry a decision identity", env.Event.ID)
	}
	version, err := int64FromSyncAny(env.Event.Payload["resource_version"])
	if err != nil {
		return SyncedEventMaterial{}, fmt.Errorf("synced event %q has invalid resource_version: %w", env.Event.ID, err)
	}
	fields, ok := env.Event.Payload["fields"].(map[string]any)
	if !ok {
		return SyncedEventMaterial{}, fmt.Errorf("synced event %q payload.fields must be an object", env.Event.ID)
	}
	origin, _ := env.Meta["origin_mnemond"].(string)
	cursor, _ := env.Meta["cursor"].(string)
	digest, _ := env.Meta["digest"].(string)
	syncedAt, _ := env.Meta["synced_at"].(string)
	seq, _ := strconv.ParseInt(cursor, 10, 64)
	return SyncedEventMaterial{
		OriginReplicaID: strings.TrimSpace(origin),
		LocalDecisionID: decisionID,
		LocalIngestSeq:  seq,
		Actor:           ActorID(env.Event.Actor),
		CorrelationID:   env.Event.CorrelationID,
		ResourceRef:     ref,
		ResourceVersion: Version(version),
		FieldsDigest:    strings.TrimSpace(digest),
		Fields:          cloneSyncFields(fields),
		DecidedAt:       strings.TrimSpace(syncedAt),
		Status:          "pending",
	}, nil
}

func EventExchangeResultFromEnvelope(env eventmodel.EventEnvelope, status, diagnostic string) EventExchangeResult {
	origin, _ := env.Meta["origin_mnemond"].(string)
	return EventExchangeResult{
		OriginMnemond: strings.TrimSpace(origin),
		EventID:       env.Event.ID,
		Subject:       env.Event.Subject,
		Status:        status,
		Diagnostic:    diagnostic,
	}
}

func ResourceRefFromEventSubject(subject eventmodel.EventSubject) (ResourceRef, error) {
	parts := strings.SplitN(string(subject), "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return ResourceRef{}, fmt.Errorf("event subject %q is not kind/id", subject)
	}
	return ResourceRef{Kind: ResourceKind(parts[0]), ID: ResourceID(parts[1])}, nil
}

func (r EventExchangeResult) ResourceRef() (ResourceRef, error) {
	return ResourceRefFromEventSubject(r.Subject)
}

func (r EventExchangeResult) LocalDecisionID() string {
	return DecisionIDFromEventID(r.EventID)
}

func DecisionIDFromEventID(eventID string) string {
	head, _, _ := strings.Cut(strings.TrimSpace(eventID), ":")
	return head
}

func syncedEventID(decisionID string, subject eventmodel.EventSubject) string {
	decisionID = strings.TrimSpace(decisionID)
	if decisionID == "" {
		decisionID = "decision"
	}
	return decisionID + ":" + string(subject)
}

func cloneSyncFields(fields map[string]any) map[string]any {
	if fields == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(fields))
	for key, value := range fields {
		out[key] = value
	}
	return out
}

func int64FromSyncAny(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		i := int64(v)
		if v != float64(i) {
			return 0, fmt.Errorf("not an integer")
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}
