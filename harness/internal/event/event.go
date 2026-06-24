package event

import (
	"fmt"
	"math"
	"strings"
)

const SchemaVersion = 1

type EventPhase string

const (
	PhaseObserved EventPhase = "observed"
	PhaseAccepted EventPhase = "accepted"
	PhaseSynced   EventPhase = "synced"
	PhaseDerived  EventPhase = "derived"
)

type EventSubject string

type Event struct {
	SchemaVersion int            `json:"schema_version"`
	ID            string         `json:"id"`
	Type          string         `json:"type"`
	Subject       EventSubject   `json:"subject"`
	Actor         string         `json:"actor"`
	Audience      string         `json:"audience"`
	Payload       map[string]any `json:"payload"`
	CausedBy      []string       `json:"caused_by"`
	CorrelationID string         `json:"correlation_id"`
	TTL           string         `json:"ttl"`
	CreatedAt     string         `json:"created_at"`
}

type EventEnvelope struct {
	SchemaVersion int            `json:"schema_version"`
	Phase         EventPhase     `json:"phase"`
	Event         Event          `json:"event"`
	Meta          map[string]any `json:"meta"`
}

func Subject(kind, id string) EventSubject {
	kind = strings.TrimSpace(kind)
	id = strings.TrimSpace(id)
	if kind == "" || id == "" {
		return ""
	}
	return EventSubject(kind + "/" + id)
}

func NewEnvelope(phase EventPhase, ev Event, meta map[string]any) EventEnvelope {
	return EventEnvelope{
		SchemaVersion: SchemaVersion,
		Phase:         phase,
		Event:         ev,
		Meta:          copyMeta(meta),
	}
}

func ObservedEnvelope(ev Event, externalID, host, lifecycle string) EventEnvelope {
	return NewEnvelope(PhaseObserved, ev, map[string]any{
		"external_id": strings.TrimSpace(externalID),
		"host":        strings.TrimSpace(host),
		"lifecycle":   strings.TrimSpace(lifecycle),
	})
}

func AcceptedEnvelope(ev Event, decisionID string, ingestSeq int64, acceptedAt, acceptedBy string) EventEnvelope {
	return NewEnvelope(PhaseAccepted, ev, map[string]any{
		"decision_id": strings.TrimSpace(decisionID),
		"ingest_seq":  ingestSeq,
		"accepted_at": strings.TrimSpace(acceptedAt),
		"accepted_by": strings.TrimSpace(acceptedBy),
	})
}

func SyncedEnvelope(ev Event, originMnemond, cursor, digest, syncedAt string) EventEnvelope {
	return NewEnvelope(PhaseSynced, ev, map[string]any{
		"origin_mnemond": strings.TrimSpace(originMnemond),
		"cursor":         strings.TrimSpace(cursor),
		"digest":         strings.TrimSpace(digest),
		"synced_at":      strings.TrimSpace(syncedAt),
	})
}

func DerivedEnvelope(ev Event, derivedAt, expiresAt, presentationHint string, suggestedEventTypes []string) EventEnvelope {
	return NewEnvelope(PhaseDerived, ev, map[string]any{
		"derived_at":            strings.TrimSpace(derivedAt),
		"expires_at":            strings.TrimSpace(expiresAt),
		"presentation_hint":     strings.TrimSpace(presentationHint),
		"suggested_event_types": append([]string(nil), suggestedEventTypes...),
	})
}

func (p EventPhase) Valid() bool {
	_, ok := phaseMetaSchemas[p]
	return ok
}

func (ev Event) Validate() error {
	if ev.SchemaVersion <= 0 {
		return fmt.Errorf("event schema_version must be positive")
	}
	if strings.TrimSpace(ev.ID) == "" {
		return fmt.Errorf("event id is required")
	}
	if strings.TrimSpace(ev.Type) == "" {
		return fmt.Errorf("event type is required")
	}
	if strings.TrimSpace(string(ev.Subject)) == "" {
		return fmt.Errorf("event subject is required")
	}
	if strings.TrimSpace(ev.Actor) == "" {
		return fmt.Errorf("event actor is required")
	}
	for key := range phaseMetaKeys {
		if _, ok := ev.Payload[key]; ok {
			return fmt.Errorf("event payload must not carry phase meta key %q", key)
		}
	}
	return nil
}

func (env EventEnvelope) Validate() error {
	if env.SchemaVersion <= 0 {
		return fmt.Errorf("event envelope schema_version must be positive")
	}
	if !env.Phase.Valid() {
		return fmt.Errorf("unknown event envelope phase %q", env.Phase)
	}
	if err := env.Event.Validate(); err != nil {
		return err
	}
	return validateMeta(env.Phase, env.Meta)
}

type metaKind int

const (
	metaString metaKind = iota
	metaInt
	metaStringSlice
)

var phaseMetaSchemas = map[EventPhase]map[string]metaKind{
	PhaseObserved: {
		"external_id": metaString,
		"host":        metaString,
		"lifecycle":   metaString,
	},
	PhaseAccepted: {
		"decision_id": metaString,
		"ingest_seq":  metaInt,
		"accepted_at": metaString,
		"accepted_by": metaString,
	},
	PhaseSynced: {
		"origin_mnemond": metaString,
		"cursor":         metaString,
		"digest":         metaString,
		"synced_at":      metaString,
	},
	PhaseDerived: {
		"derived_at":            metaString,
		"expires_at":            metaString,
		"presentation_hint":     metaString,
		"suggested_event_types": metaStringSlice,
	},
}

var phaseMetaKeys = func() map[string]bool {
	keys := map[string]bool{}
	for _, schema := range phaseMetaSchemas {
		for key := range schema {
			keys[key] = true
		}
	}
	return keys
}()

func validateMeta(phase EventPhase, meta map[string]any) error {
	schema, ok := phaseMetaSchemas[phase]
	if !ok {
		return fmt.Errorf("unknown event envelope phase %q", phase)
	}
	if len(meta) == 0 {
		return fmt.Errorf("%s event envelope meta is required", phase)
	}
	for key := range meta {
		if _, ok := schema[key]; !ok {
			return fmt.Errorf("%s event envelope meta has unknown key %q", phase, key)
		}
	}
	for key, kind := range schema {
		value, ok := meta[key]
		if !ok {
			return fmt.Errorf("%s event envelope meta missing key %q", phase, key)
		}
		if err := validateMetaValue(phase, key, kind, value); err != nil {
			return err
		}
	}
	return nil
}

func validateMetaValue(phase EventPhase, key string, kind metaKind, value any) error {
	switch kind {
	case metaString:
		s, ok := value.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return fmt.Errorf("%s event envelope meta key %q must be a non-empty string", phase, key)
		}
	case metaInt:
		if !positiveInt(value) {
			return fmt.Errorf("%s event envelope meta key %q must be a positive integer", phase, key)
		}
	case metaStringSlice:
		if !stringSlice(value) {
			return fmt.Errorf("%s event envelope meta key %q must be a string list", phase, key)
		}
	}
	return nil
}

func positiveInt(value any) bool {
	switch v := value.(type) {
	case int:
		return v > 0
	case int8:
		return v > 0
	case int16:
		return v > 0
	case int32:
		return v > 0
	case int64:
		return v > 0
	case uint:
		return v > 0
	case uint8:
		return v > 0
	case uint16:
		return v > 0
	case uint32:
		return v > 0
	case uint64:
		return v > 0
	case float64:
		return v > 0 && math.Trunc(v) == v
	default:
		return false
	}
}

func stringSlice(value any) bool {
	switch v := value.(type) {
	case []string:
		for _, s := range v {
			if strings.TrimSpace(s) == "" {
				return false
			}
		}
		return true
	case []any:
		for _, item := range v {
			s, ok := item.(string)
			if !ok || strings.TrimSpace(s) == "" {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func copyMeta(meta map[string]any) map[string]any {
	if meta == nil {
		return nil
	}
	out := make(map[string]any, len(meta))
	for key, value := range meta {
		out[key] = value
	}
	return out
}
