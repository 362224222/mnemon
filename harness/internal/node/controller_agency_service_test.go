package node

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type blockingAgencyService struct {
	entered chan struct{}
	release chan struct{}
}

func (*blockingAgencyService) AgencyAttach(context.Context) (AgencyAttachment, error) {
	return AgencyAttachment{}, nil
}

func (*blockingAgencyService) AgencyCurrent(context.Context, AgencyAuthority) (AgencyView, error) {
	return AgencyView{}, nil
}

func (*blockingAgencyService) AgencySubmit(context.Context, AgencyAuthority,
	AgencySubmission,
) (AgencyReceipt, error) {
	return AgencyReceipt{}, nil
}

func (service *blockingAgencyService) AgencyCapture(context.Context,
	[]byte,
) (AgencyArtifactCapture, error) {
	close(service.entered)
	<-service.release
	return AgencyArtifactCapture{}, nil
}

func (*blockingAgencyService) AgencyStatus(context.Context) (AgencyStatusSnapshot, error) {
	return AgencyStatusSnapshot{Ready: true}, nil
}

func TestControllerAgencyAdmissionDrainsBeforeSealAndRejectsAfterward(t *testing.T) {
	gate := newControllerAdmissionGate()
	next := &blockingAgencyService{entered: make(chan struct{}), release: make(chan struct{})}
	service := controllerAgencyAdmissionService{gate: gate, next: next}
	captureDone := make(chan error, 1)
	go func() {
		_, err := service.AgencyCapture(context.Background(), []byte("candidate"))
		captureDone <- err
	}()
	select {
	case <-next.entered:
	case <-time.After(time.Second):
		t.Fatal("Agency capture never entered admission")
	}

	type sealResult struct {
		generation uint64
		err        error
	}
	sealed := make(chan sealResult, 1)
	go func() {
		generation, err := gate.seal(context.Background())
		sealed <- sealResult{generation: generation, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, err := service.AgencyStatus(context.Background())
		if errors.Is(err, ErrManagedAdmission) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Agency admission was not sealed")
		}
		runtime.Gosched()
	}
	select {
	case result := <-sealed:
		t.Fatalf("seal returned before Agency request drained: %#v", result)
	default:
	}
	close(next.release)
	if err := <-captureDone; err != nil {
		t.Fatal(err)
	}
	result := <-sealed
	if result.err != nil || result.generation == 0 {
		t.Fatalf("seal = (%d,%v)", result.generation, result.err)
	}
	if _, err := service.AgencyStatus(context.Background()); !errors.Is(err, ErrManagedAdmission) ||
		ClassifyAgencyError(err) != AgencyErrorUnavailable {
		t.Fatalf("AgencyStatus after seal = %v", err)
	}
	gate.reopen(result.generation)
	if status, err := service.AgencyStatus(context.Background()); err != nil || !status.Ready {
		t.Fatalf("AgencyStatus after reopen = (%#v,%v)", status, err)
	}
}

type fixedHealthProbe struct{ health HealthResponse }

func (probe fixedHealthProbe) ProbeHealth(context.Context) (HealthResponse, *APIError) {
	return probe.health, nil
}

type fixedAgencyHealthProbe struct{ fixedHealthProbe }

func (fixedAgencyHealthProbe) ProbeAgencyStatus(context.Context) (AgencyStatusSnapshot, *APIError) {
	return AgencyStatusSnapshot{Ready: true}, nil
}

type fixedControlClientFactory struct{ client DaemonHealthProbe }

func (factory fixedControlClientFactory) NewControlClient(string) (DaemonHealthProbe, error) {
	return factory.client, nil
}

func TestControllerReadinessRequiresMountedAgencyStatusWhenComposed(t *testing.T) {
	revision := model.Sum([]byte("Agency readiness route")).String()
	health := HealthResponse{AssetRevision: revision, SchemaVersion: SchemaVersion, Status: "ready"}
	withoutAgency := fixedControlClientFactory{client: fixedHealthProbe{health: health}}
	if err := proveControllerHTTP(context.Background(), withoutAgency, "/unused", revision, false); err != nil {
		t.Fatalf("legacy readiness = %v", err)
	}
	if err := proveControllerHTTP(context.Background(), withoutAgency, "/unused", revision, true); err == nil {
		t.Fatal("Agency readiness accepted a client with no Agency status route")
	}
	withAgency := fixedControlClientFactory{client: fixedAgencyHealthProbe{
		fixedHealthProbe: fixedHealthProbe{health: health}}}
	if err := proveControllerHTTP(context.Background(), withAgency, "/unused", revision, true); err != nil {
		t.Fatalf("Agency readiness = %v", err)
	}
}
