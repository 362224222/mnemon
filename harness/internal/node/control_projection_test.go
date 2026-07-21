package node

import (
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestParseInitiationProjectionRequiresCanonicalClosedProjection(t *testing.T) {
	t.Parallel()
	projection := InitiationProjection{SchemaVersion: SchemaVersion}
	projection.InitiationContext.Channels = []InitiationChannel{{
		AllowTeam:  true,
		LocalAlias: "alpha",
		Participants: []InitiationParticipant{
			{EffectiveAlias: "helper", Eligible: true, Reachable: true},
		},
	}}
	raw, err := model.CanonicalMarshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseInitiationProjection(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed, projection) {
		t.Fatalf("parsed projection = %#v, want %#v", parsed, projection)
	}
	if _, err := ParseInitiationProjection(append([]byte{' '}, raw...)); err == nil {
		t.Fatal("noncanonical projection accepted")
	}
}
