package contract

import (
	"fmt"
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func EventEnvelopeFromObservation(env ObservationEnvelope, host, lifecycle string) (eventmodel.EventEnvelope, error) {
	ev := eventFromObservation(env.Source, env.Event)
	if ev.ID == "" && env.ExternalID != "" && ev.Actor != "" {
		ev.ID = "observed:" + ev.Actor + ":" + env.ExternalID
	}
	out := eventmodel.ObservedEnvelope(ev, env.ExternalID, host, lifecycle)
	if err := out.Validate(); err != nil {
		return eventmodel.EventEnvelope{}, fmt.Errorf("adapt contract observation: %w", err)
	}
	return out, nil
}

func eventFromObservation(source ActorID, ev Event) eventmodel.Event {
	actor := strings.TrimSpace(string(source))
	if actor == "" {
		actor = strings.TrimSpace(string(ev.Actor))
	}
	out := eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            strings.TrimSpace(ev.ID),
		Type:          strings.TrimSpace(ev.Type),
		Subject:       eventSubject(ev),
		Actor:         actor,
		Payload:       copyEventPayload(ev.Payload),
		CausedBy:      eventCausedBy(ev.CausedBy),
		CorrelationID: strings.TrimSpace(ev.CorrelationID),
		CreatedAt:     strings.TrimSpace(ev.TS),
	}
	if ttl, ok := eventmodel.PayloadRule(ev.Payload)["ttl"].(string); ok {
		out.TTL = strings.TrimSpace(ttl)
	}
	return out
}

func eventSubject(ev Event) eventmodel.EventSubject {
	kind := eventKind(ev.Type)
	if rule := eventmodel.PayloadRule(ev.Payload); len(rule) > 0 {
		for _, key := range eventSubjectIDKeys(kind) {
			if id, _ := rule[key].(string); strings.TrimSpace(id) != "" {
				if subject := eventmodel.Subject(kind, id); subject != "" {
					return subject
				}
			}
		}
	}
	if len(ev.ResourceRefs) > 0 {
		ref := ev.ResourceRefs[0]
		if subject := eventmodel.Subject(string(ref.Kind), string(ref.ID)); subject != "" {
			return subject
		}
	}
	if kind == "" {
		return ""
	}
	return eventmodel.Subject(kind, "project")
}

func eventKind(eventType string) string {
	kind := strings.TrimSpace(eventType)
	if i := strings.Index(kind, "."); i >= 0 {
		kind = kind[:i]
	}
	return kind
}

func eventSubjectIDKeys(kind string) []string {
	keys := []string{kind + "_id", "id"}
	for _, key := range []string{"assignment_id", "signal_id", "intent_id", "actor"} {
		if key != kind+"_id" {
			keys = append(keys, key)
		}
	}
	return keys
}

func eventCausedBy(id string) []string {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	return []string{id}
}

func copyEventPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}
