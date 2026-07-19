package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestProbeManagedOperationIsReadOnlyAndIndependentOfLiveAuthority(t *testing.T) {
	t.Parallel()
	fixture := newManagedContextFixture(t, "probe", model.OperationTeamworkAccept)
	spec := fixture.spec("probe-key", "probe-request", model.OperationTeamworkAccept)
	missing, err := fixture.store.ProbeManagedOperation(context.Background(), ManagedOperationProbeSpec{
		Profile: fixture.profile, ClientKeyHash: model.Sum([]byte("missing-key")),
		RequestDigest: spec.RequestDigest, Kind: spec.Kind,
		ClaimContextHash: spec.ClaimContextHash, HasClaimContext: true,
	})
	if err != nil || missing.Found || !missing.Operation.ID().IsZero() {
		t.Fatalf("missing probe = (%#v, %v)", missing, err)
	}

	reservation, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	started, err := fixture.store.ProbeManagedOperation(context.Background(), ManagedOperationProbeSpec{
		Profile: fixture.profile, ClientKeyHash: spec.ClientKeyHash,
		RequestDigest: spec.RequestDigest, Kind: spec.Kind,
		ClaimContextHash: spec.ClaimContextHash, HasClaimContext: true,
	})
	if err != nil || started.Found || !started.Operation.ID().IsZero() {
		t.Fatalf("started probe = (%#v, %v)", started, err)
	}

	disabled, receipt := rejectManagedOperationProbeFixture(t, fixture, spec, reservation.Operation)
	terminal, err := fixture.store.ProbeManagedOperation(context.Background(), ManagedOperationProbeSpec{
		Profile: disabled, ClientKeyHash: spec.ClientKeyHash,
		RequestDigest: spec.RequestDigest, Kind: spec.Kind,
		ClaimContextHash: spec.ClaimContextHash, HasClaimContext: true,
	})
	if err != nil || !terminal.Found || terminal.Operation.Status() != model.OperationRejected ||
		terminal.Operation.AgentRunID() != reservation.Operation.AgentRunID() {
		t.Fatalf("terminal probe = (%#v, %v)", terminal, err)
	}
	storedReceipt, hasReceipt := terminal.Operation.Result()
	if !hasReceipt || storedReceipt.String() != receipt.String() {
		t.Fatalf("terminal receipt = (%s, %t)", storedReceipt.String(), hasReceipt)
	}
}

func TestProbeManagedOperationRejectsMismatchedRequestAuthority(t *testing.T) {
	t.Parallel()
	fixture := newTerminalManagedOperationProbeFixture(t, "probe-mismatch")
	mutations := []struct {
		name   string
		mutate func(*ManagedOperationProbeSpec)
	}{
		{name: "request", mutate: func(value *ManagedOperationProbeSpec) {
			value.RequestDigest = model.Sum([]byte("changed-request"))
		}},
		{name: "kind", mutate: func(value *ManagedOperationProbeSpec) {
			value.Kind = model.OperationTeamworkDecline
		}},
		{name: "context", mutate: func(value *ManagedOperationProbeSpec) {
			value.ClaimContextHash = model.Sum([]byte("changed-context"))
		}},
		{name: "context presence", mutate: func(value *ManagedOperationProbeSpec) {
			value.ClaimContextHash, value.HasClaimContext = model.Digest{}, false
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			probe := fixture.probeSpec()
			mutation.mutate(&probe)
			if _, err := fixture.fixture.store.ProbeManagedOperation(context.Background(), probe); !errors.Is(err, ErrOperationMismatch) {
				t.Fatalf("mismatch error = %v", err)
			}
		})
	}
}

func TestProbeManagedOperationAuthenticatesProfileBeforeReadingTerminal(t *testing.T) {
	t.Parallel()
	fixture := newTerminalManagedOperationProbeFixture(t, "probe-profile")
	forgedSpec := fixture.profile.Spec()
	forgedSpec.CredentialHash = model.Sum([]byte("different-credential"))
	forged, err := model.NewProfile(forgedSpec)
	if err != nil {
		t.Fatal(err)
	}
	probe := fixture.probeSpec()
	probe.Profile = forged
	if _, err := fixture.fixture.store.ProbeManagedOperation(context.Background(), probe); !errors.Is(err, ErrManagedProfileAuthority) {
		t.Fatalf("forged Profile probe error = %v", err)
	}
}

type terminalManagedOperationProbeFixture struct {
	fixture *managedContextFixture
	spec    ManagedOperationSpec
	profile model.Profile
}

func newTerminalManagedOperationProbeFixture(t *testing.T, suffix string) terminalManagedOperationProbeFixture {
	t.Helper()
	fixture := newManagedContextFixture(t, suffix, model.OperationTeamworkAccept)
	spec := fixture.spec(suffix+"-key", suffix+"-request", model.OperationTeamworkAccept)
	reservation, err := fixture.store.ReserveManagedOperation(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	disabled, _ := rejectManagedOperationProbeFixture(t, fixture, spec, reservation.Operation)
	return terminalManagedOperationProbeFixture{fixture: fixture, spec: spec, profile: disabled}
}

func rejectManagedOperationProbeFixture(t testing.TB, fixture *managedContextFixture,
	spec ManagedOperationSpec, operation model.Operation,
) (model.Profile, model.JSON) {
	t.Helper()
	receipt := mustManagedOperationRejectionReceipt(t, operation.ID(), "work_conflict",
		"current Work changed before admission")
	if _, err := fixture.store.RejectOperation(context.Background(), operation.ID(),
		spec.LeaseOwner, spec.At.Add(1), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.db.Exec("UPDATE profiles SET enabled=0 WHERE profile_id=?",
		fixture.profile.ID().String()); err != nil {
		t.Fatal(err)
	}
	disabled, err := readProfile(context.Background(), fixture.store.db)
	if err != nil || disabled.Enabled() {
		t.Fatalf("disabled Profile = (%#v, %v)", disabled, err)
	}
	return disabled, receipt
}

func (fixture terminalManagedOperationProbeFixture) probeSpec() ManagedOperationProbeSpec {
	return ManagedOperationProbeSpec{Profile: fixture.profile,
		ClientKeyHash: fixture.spec.ClientKeyHash, RequestDigest: fixture.spec.RequestDigest,
		Kind: fixture.spec.Kind, ClaimContextHash: fixture.spec.ClaimContextHash, HasClaimContext: true}
}
