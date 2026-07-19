package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type artifactCapturerFunc func(context.Context, []string) (artifact.Closure, error)

func (function artifactCapturerFunc) Capture(ctx context.Context,
	paths []string,
) (artifact.Closure, error) {
	return function(ctx, paths)
}

type artifactVerifierFunc func(context.Context, artifact.Closure) error

func (function artifactVerifierFunc) VerifyClosure(ctx context.Context, closure artifact.Closure) error {
	return function(ctx, closure)
}

type artifactCaptureLifecycleFunc func() (*artifact.CASLease, error)

func (function artifactCaptureLifecycleFunc) AcquireUse() (*artifact.CASLease, error) {
	return function()
}

type artifactCaptureStoreStub struct {
	closure   func(context.Context, store.VerifiedArtifactClosure) (store.VerifiedArtifactClosureCheckpoint, error)
	operation func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error)
}

func (stub *artifactCaptureStoreStub) CheckpointVerifiedArtifactClosure(ctx context.Context,
	closure store.VerifiedArtifactClosure,
) (store.VerifiedArtifactClosureCheckpoint, error) {
	if stub.closure == nil {
		return store.VerifiedArtifactClosureCheckpoint{}, errors.New("unexpected closure checkpoint")
	}
	return stub.closure(ctx, closure)
}

func (stub *artifactCaptureStoreStub) CheckpointOperationCapture(ctx context.Context,
	operation model.OperationID, owner string, at time.Time, checkpoint model.JSON,
) (bool, error) {
	if stub.operation == nil {
		return false, errors.New("unexpected operation checkpoint")
	}
	return stub.operation(ctx, operation, owner, at, checkpoint)
}

type artifactCoordinatorClock struct {
	values []time.Time
	calls  int
}

func (clock *artifactCoordinatorClock) Now() time.Time {
	if clock.calls >= len(clock.values) {
		clock.calls++
		return time.Time{}
	}
	value := clock.values[clock.calls]
	clock.calls++
	return value
}

func TestArtifactCaptureCoordinatorVerifiesAndCheckpointsFreshClosureInOrder(t *testing.T) {
	at := time.Date(2026, 7, 16, 18, 0, 0, 0, time.UTC)
	closure, cas, _ := artifactCoordinatorClosure(t, at)
	operation := artifactCoordinatorStartedOperation(t, "fresh", at, at.Add(time.Minute), nil)
	reservation := store.ManagedOperationReservation{Operation: operation, Acquired: true}
	order := make([]string, 0, 4)
	var durable store.VerifiedArtifactClosure
	var operationAt time.Time
	var operationCheckpoint model.JSON

	capturer := artifactCapturerFunc(func(ctx context.Context, paths []string) (artifact.Closure, error) {
		order = append(order, "capture")
		if len(paths) != 1 || paths[0] != "result.txt" {
			t.Fatalf("capture paths = %#v", paths)
		}
		return closure, nil
	})
	verifier := artifactVerifierFunc(func(ctx context.Context, value artifact.Closure) error {
		order = append(order, "verify")
		if !value.SameContent(closure) {
			t.Fatal("verifier received another closure")
		}
		return cas.VerifyClosure(ctx, value)
	})
	stub := &artifactCaptureStoreStub{
		closure: func(_ context.Context, value store.VerifiedArtifactClosure) (
			store.VerifiedArtifactClosureCheckpoint, error,
		) {
			order = append(order, "closure")
			durable = value
			return store.VerifiedArtifactClosureCheckpoint{Closure: value}, nil
		},
		operation: func(_ context.Context, id model.OperationID, owner string, now time.Time,
			checkpoint model.JSON,
		) (bool, error) {
			order = append(order, "operation")
			if id != operation.ID() || owner != operation.LeaseOwner() {
				t.Fatalf("operation fence = %s %q", id.String(), owner)
			}
			operationAt, operationCheckpoint = now, checkpoint
			return false, nil
		},
	}
	clock := &artifactCoordinatorClock{values: []time.Time{at.Add(time.Second), at.Add(2 * time.Second)}}
	coordinator, err := NewArtifactCaptureCoordinator(capturer, verifier, cas, stub, clock)
	if err != nil {
		t.Fatal(err)
	}
	result, apiErr := coordinator.Checkpoint(context.Background(), reservation, []string{"result.txt"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if result.Replayed || result.Checkpoint.String() != closure.Checkpoint().String() || len(result.Roots) != 1 ||
		!operationAt.Equal(at.Add(2*time.Second)) || operationCheckpoint.String() != result.Checkpoint.String() {
		t.Fatalf("fresh result = %#v at=%s operation_checkpoint=%s", result, operationAt,
			operationCheckpoint.String())
	}
	if strings.Join(order, ",") != "capture,verify,closure,operation" || clock.calls != 2 {
		t.Fatalf("fresh order/clock = %v calls=%d", order, clock.calls)
	}
	if len(durable.Roots) != 1 || len(durable.Blocks) != 1 || len(durable.RootBlocks) != 1 ||
		durable.Roots[0].RootDigest != result.Roots[0].RootDigest ||
		durable.Roots[0].ManifestDigest != result.Roots[0].ManifestDigest ||
		!durable.Roots[0].CreatedAt.Equal(at) || !durable.Roots[0].VerifiedAt.Equal(at) {
		t.Fatalf("Store closure projection = %#v", durable)
	}
}

func TestArtifactCaptureCoordinatorHoldsLifecycleThroughOperationOwnershipCheckpoint(t *testing.T) {
	at := time.Date(2026, 7, 16, 18, 30, 0, 0, time.UTC)
	closure, cas, _ := artifactCoordinatorClosure(t, at)
	operation := artifactCoordinatorStartedOperation(t, "lifecycle", at, at.Add(time.Minute), nil)
	reservation := store.ManagedOperationReservation{Operation: operation, Acquired: true}
	captureEntered, closureEntered, operationEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	captureContinue, closureContinue, operationContinue := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 1)
	defer artifactCoordinatorUnblock(captureContinue)
	defer artifactCoordinatorUnblock(closureContinue)
	defer artifactCoordinatorUnblock(operationContinue)

	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) {
			close(captureEntered)
			<-captureContinue
			return closure, nil
		}), artifactVerifierFunc(cas.VerifyClosure), cas, &artifactCaptureStoreStub{
			closure: func(context.Context, store.VerifiedArtifactClosure) (
				store.VerifiedArtifactClosureCheckpoint, error,
			) {
				close(closureEntered)
				<-closureContinue
				return store.VerifiedArtifactClosureCheckpoint{}, nil
			},
			operation: func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error) {
				close(operationEntered)
				<-operationContinue
				return false, nil
			},
		}, &artifactCoordinatorClock{values: []time.Time{at.Add(time.Second), at.Add(2 * time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	type checkpointOutcome struct {
		result ArtifactCaptureResult
		err    *localapi.APIError
	}
	checkpointDone := make(chan checkpointOutcome, 1)
	go func() {
		result, apiErr := coordinator.Checkpoint(context.Background(), reservation, []string{"result.txt"})
		checkpointDone <- checkpointOutcome{result: result, err: apiErr}
	}()
	artifactCoordinatorAwait(t, captureEntered, "live capture")

	exclusiveStarted := make(chan struct{})
	exclusiveDone := make(chan error, 1)
	go func() {
		close(exclusiveStarted)
		lease, acquireErr := cas.AcquireExclusive()
		if acquireErr == nil {
			lease.Release()
		}
		exclusiveDone <- acquireErr
	}()
	artifactCoordinatorAwait(t, exclusiveStarted, "exclusive collector start")
	artifactCoordinatorRequireBlocked(t, exclusiveDone, "live capture")

	captureContinue <- struct{}{}
	artifactCoordinatorAwait(t, closureEntered, "closure checkpoint")
	artifactCoordinatorRequireBlocked(t, exclusiveDone, "closure checkpoint")
	closureContinue <- struct{}{}
	artifactCoordinatorAwait(t, operationEntered, "operation ownership checkpoint")
	artifactCoordinatorRequireBlocked(t, exclusiveDone, "operation ownership checkpoint")
	operationContinue <- struct{}{}

	select {
	case outcome := <-checkpointDone:
		if outcome.err != nil || outcome.result.Replayed || len(outcome.result.Roots) != 1 {
			t.Fatalf("checkpoint outcome = (%#v, %#v)", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Artifact checkpoint did not complete")
	}
	select {
	case acquireErr := <-exclusiveDone:
		if acquireErr != nil {
			t.Fatalf("exclusive collection after checkpoint = %v", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("exclusive collection remained blocked after operation ownership checkpoint")
	}
}

func TestArtifactCaptureCoordinatorReleasesLifecycleAfterFailureAndCancellation(t *testing.T) {
	at := time.Date(2026, 7, 16, 18, 45, 0, 0, time.UTC)
	tests := []struct {
		name      string
		capture   func(context.Context, artifact.Closure) (artifact.Closure, error)
		closure   error
		operation error
		code      localapi.ErrorCode
	}{
		{name: "capture cancellation", capture: func(ctx context.Context, _ artifact.Closure) (artifact.Closure, error) {
			return artifact.Closure{}, ctx.Err()
		}, code: localapi.CodeOperationPending},
		{name: "closure checkpoint failure", closure: store.ErrArtifactConflict, code: localapi.CodeInternal},
		{name: "operation checkpoint failure", operation: store.ErrOperationFence,
			code: localapi.CodeOperationPending},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			closure, cas, _ := artifactCoordinatorClosure(t, at)
			operation := artifactCoordinatorStartedOperation(t, "release-"+strconv.Itoa(index),
				at, at.Add(time.Minute), nil)
			capture := test.capture
			if capture == nil {
				capture = func(context.Context, artifact.Closure) (artifact.Closure, error) {
					return closure, nil
				}
			}
			coordinator, err := NewArtifactCaptureCoordinator(
				artifactCapturerFunc(func(ctx context.Context, _ []string) (artifact.Closure, error) {
					return capture(ctx, closure)
				}), artifactVerifierFunc(cas.VerifyClosure), cas, &artifactCaptureStoreStub{
					closure: func(_ context.Context, value store.VerifiedArtifactClosure) (
						store.VerifiedArtifactClosureCheckpoint, error,
					) {
						return store.VerifiedArtifactClosureCheckpoint{Closure: value}, test.closure
					},
					operation: func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error) {
						return false, test.operation
					},
				}, &artifactCoordinatorClock{values: []time.Time{at.Add(time.Second), at.Add(2 * time.Second)}})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if test.name == "capture cancellation" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			_, apiErr := coordinator.Checkpoint(ctx,
				store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"result.txt"})
			if apiErr == nil || apiErr.Code != test.code {
				t.Fatalf("failure = %#v, want %s", apiErr, test.code)
			}
			artifactCoordinatorRequireExclusive(t, cas)
		})
	}
}

func TestArtifactCaptureCoordinatorMapsLifecycleAcquisitionFailureToInternal(t *testing.T) {
	at := time.Date(2026, 7, 16, 18, 50, 0, 0, time.UTC)
	operation := artifactCoordinatorStartedOperation(t, "lifecycle-failure", at, at.Add(time.Minute), nil)
	liveCalls := 0
	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) {
			liveCalls++
			return artifact.Closure{}, nil
		}), artifactVerifierFunc(func(context.Context, artifact.Closure) error {
			liveCalls++
			return nil
		}), artifactCaptureLifecycleFunc(func() (*artifact.CASLease, error) {
			return nil, errors.New("collector lifecycle unavailable")
		}), &artifactCaptureStoreStub{}, &artifactCoordinatorClock{values: []time.Time{at.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := coordinator.Checkpoint(context.Background(),
		store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"result.txt"})
	if apiErr == nil || apiErr.Code != localapi.CodeInternal || apiErr.Retryable || liveCalls != 0 {
		t.Fatalf("lifecycle acquisition failure = %#v, live calls=%d", apiErr, liveCalls)
	}
}

func TestArtifactCaptureCoordinatorReusesDurableCheckpointWithoutLiveReads(t *testing.T) {
	at := time.Date(2026, 7, 16, 19, 0, 0, 0, time.UTC)
	checkpoint := artifactCoordinatorCheckpoint(t, "replay")
	operation := artifactCoordinatorStartedOperation(t, "replay", at, at.Add(time.Minute), &checkpoint)
	reservation := store.ManagedOperationReservation{Operation: operation, Replayed: true, Acquired: true}
	captureCalls, verifyCalls, closureCalls, operationCalls := 0, 0, 0, 0
	stub := &artifactCaptureStoreStub{
		closure: func(context.Context, store.VerifiedArtifactClosure) (
			store.VerifiedArtifactClosureCheckpoint, error,
		) {
			closureCalls++
			return store.VerifiedArtifactClosureCheckpoint{}, nil
		},
		operation: func(_ context.Context, id model.OperationID, owner string, now time.Time,
			value model.JSON,
		) (bool, error) {
			operationCalls++
			if id != operation.ID() || owner != operation.LeaseOwner() ||
				!now.Equal(at.Add(time.Second)) || value.String() != checkpoint.String() {
				t.Fatalf("replay checkpoint fence = %s %q %s %s", id, owner, now, value.String())
			}
			return true, nil
		},
	}
	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) {
			captureCalls++
			return artifact.Closure{}, errors.New("live workspace must not be read")
		}),
		artifactVerifierFunc(func(context.Context, artifact.Closure) error {
			verifyCalls++
			return errors.New("CAS must not be reverified")
		}), artifactCoordinatorNoLifecycle(t), stub,
		&artifactCoordinatorClock{values: []time.Time{at.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	result, apiErr := coordinator.Checkpoint(context.Background(), reservation,
		[]string{"missing-now.txt"})
	if apiErr != nil || !result.Replayed || result.Checkpoint.String() != checkpoint.String() ||
		len(result.Roots) != 1 {
		t.Fatalf("durable replay = (%#v, %#v)", result, apiErr)
	}
	if captureCalls != 0 || verifyCalls != 0 || closureCalls != 0 || operationCalls != 1 {
		t.Fatalf("durable replay calls = capture %d verify %d closure %d operation %d",
			captureCalls, verifyCalls, closureCalls, operationCalls)
	}
}

func TestArtifactCaptureCoordinatorRejectsMalformedDurableCheckpointWithoutLiveReads(t *testing.T) {
	at := time.Date(2026, 7, 16, 19, 30, 0, 0, time.UTC)
	malformed, err := model.NewJSON([]byte(`{"extra":true,"roots":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	operation := artifactCoordinatorStartedOperation(t, "malformed-replay", at,
		at.Add(time.Minute), &malformed)
	storeCalls := 0
	stub := &artifactCaptureStoreStub{
		closure: func(context.Context, store.VerifiedArtifactClosure) (
			store.VerifiedArtifactClosureCheckpoint, error,
		) {
			storeCalls++
			return store.VerifiedArtifactClosureCheckpoint{}, nil
		},
		operation: func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error) {
			storeCalls++
			return false, nil
		},
	}
	clock := &artifactCoordinatorClock{}
	coordinator := artifactCoordinatorWithNoLiveCalls(t, stub, clock)
	_, apiErr := coordinator.Checkpoint(context.Background(),
		store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"missing"})
	if apiErr == nil || apiErr.Code != localapi.CodeInternal || storeCalls != 0 || clock.calls != 0 {
		t.Fatalf("malformed durable checkpoint = %#v, Store=%d clock=%d", apiErr, storeCalls, clock.calls)
	}
}

func TestArtifactCaptureCoordinatorSupportsEmptyAndTerminalDurableCapture(t *testing.T) {
	at := time.Date(2026, 7, 16, 20, 0, 0, 0, time.UTC)
	t.Run("empty", func(t *testing.T) {
		operation := artifactCoordinatorStartedOperation(t, "empty", at, at.Add(time.Minute), nil)
		calls := 0
		stub := &artifactCaptureStoreStub{operation: func(_ context.Context, id model.OperationID,
			owner string, now time.Time, checkpoint model.JSON,
		) (bool, error) {
			calls++
			if id != operation.ID() || owner != operation.LeaseOwner() || !now.Equal(at.Add(2*time.Second)) ||
				checkpoint.String() != `{"roots":[]}` {
				t.Fatalf("empty checkpoint = %s %q %s %s", id, owner, now, checkpoint.String())
			}
			return false, nil
		}}
		coordinator := artifactCoordinatorWithNoLiveCalls(t, stub,
			&artifactCoordinatorClock{values: []time.Time{at.Add(time.Second), at.Add(2 * time.Second)}})
		result, apiErr := coordinator.Checkpoint(context.Background(),
			store.ManagedOperationReservation{Operation: operation, Acquired: true}, nil)
		if apiErr != nil || result.Replayed || result.Roots == nil || len(result.Roots) != 0 ||
			result.Checkpoint.String() != `{"roots":[]}` || calls != 1 {
			t.Fatalf("empty capture = (%#v, %#v), calls=%d", result, apiErr, calls)
		}
	})

	t.Run("terminal replay", func(t *testing.T) {
		checkpoint := artifactCoordinatorCheckpoint(t, "terminal")
		operation := artifactCoordinatorTerminalOperation(t, "terminal", at, &checkpoint)
		storeCalls := 0
		stub := &artifactCaptureStoreStub{
			closure: func(context.Context, store.VerifiedArtifactClosure) (
				store.VerifiedArtifactClosureCheckpoint, error,
			) {
				storeCalls++
				return store.VerifiedArtifactClosureCheckpoint{}, nil
			},
			operation: func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error) {
				storeCalls++
				return false, nil
			},
		}
		clock := &artifactCoordinatorClock{}
		coordinator := artifactCoordinatorWithNoLiveCalls(t, stub, clock)
		result, apiErr := coordinator.Checkpoint(context.Background(),
			store.ManagedOperationReservation{Operation: operation, Replayed: true}, []string{"gone"})
		if apiErr != nil || !result.Replayed || result.Checkpoint.String() != checkpoint.String() ||
			storeCalls != 0 || clock.calls != 0 {
			t.Fatalf("terminal replay = (%#v, %#v), store=%d clock=%d",
				result, apiErr, storeCalls, clock.calls)
		}
	})
}

func TestArtifactCaptureCoordinatorFailsClosedBeforeDurableCheckpoint(t *testing.T) {
	at := time.Date(2026, 7, 16, 21, 0, 0, 0, time.UTC)
	closure, cas, _ := artifactCoordinatorClosure(t, at)
	operation := artifactCoordinatorStartedOperation(t, "verify-failure", at, at.Add(time.Minute), nil)
	storeCalls := 0
	stub := &artifactCaptureStoreStub{
		closure: func(context.Context, store.VerifiedArtifactClosure) (
			store.VerifiedArtifactClosureCheckpoint, error,
		) {
			storeCalls++
			return store.VerifiedArtifactClosureCheckpoint{}, nil
		},
		operation: func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error) {
			storeCalls++
			return false, nil
		},
	}
	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) { return closure, nil }),
		artifactVerifierFunc(func(context.Context, artifact.Closure) error { return artifact.ErrClosureMismatch }),
		cas, stub, &artifactCoordinatorClock{values: []time.Time{at.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := coordinator.Checkpoint(context.Background(),
		store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"result.txt"})
	if apiErr == nil || apiErr.Code != localapi.CodeInternal || storeCalls != 0 {
		t.Fatalf("verification failure = %#v, Store calls=%d", apiErr, storeCalls)
	}
}

func TestArtifactCaptureCoordinatorDoesNotCheckpointOperationAfterClosureConflict(t *testing.T) {
	at := time.Date(2026, 7, 16, 21, 30, 0, 0, time.UTC)
	closure, cas, _ := artifactCoordinatorClosure(t, at)
	operation := artifactCoordinatorStartedOperation(t, "closure-conflict", at, at.Add(time.Minute), nil)
	closureCalls, operationCalls := 0, 0
	stub := &artifactCaptureStoreStub{
		closure: func(context.Context, store.VerifiedArtifactClosure) (
			store.VerifiedArtifactClosureCheckpoint, error,
		) {
			closureCalls++
			return store.VerifiedArtifactClosureCheckpoint{}, store.ErrArtifactConflict
		},
		operation: func(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error) {
			operationCalls++
			return false, nil
		},
	}
	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) { return closure, nil }),
		artifactVerifierFunc(cas.VerifyClosure), cas, stub,
		&artifactCoordinatorClock{values: []time.Time{at.Add(time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := coordinator.Checkpoint(context.Background(),
		store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"result.txt"})
	if apiErr == nil || apiErr.Code != localapi.CodeInternal || closureCalls != 1 || operationCalls != 0 {
		t.Fatalf("closure conflict = %#v, closure=%d operation=%d", apiErr, closureCalls, operationCalls)
	}
}

func TestArtifactCaptureCoordinatorUsesPostCaptureTimeForLeaseFence(t *testing.T) {
	at := time.Date(2026, 7, 16, 22, 0, 0, 0, time.UTC)
	closure, cas, _ := artifactCoordinatorClosure(t, at)
	operation := artifactCoordinatorStartedOperation(t, "fresh-fence", at, at.Add(time.Minute), nil)
	closureCalls, operationCalls := 0, 0
	postCapture := at.Add(2 * time.Minute)
	stub := &artifactCaptureStoreStub{
		closure: func(_ context.Context, value store.VerifiedArtifactClosure) (
			store.VerifiedArtifactClosureCheckpoint, error,
		) {
			closureCalls++
			return store.VerifiedArtifactClosureCheckpoint{Closure: value}, nil
		},
		operation: func(_ context.Context, _ model.OperationID, _ string, now time.Time,
			_ model.JSON,
		) (bool, error) {
			operationCalls++
			if !now.Equal(postCapture) {
				t.Fatalf("operation checkpoint used stale time %s, want %s", now, postCapture)
			}
			return false, store.ErrOperationFence
		},
	}
	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) { return closure, nil }),
		artifactVerifierFunc(cas.VerifyClosure), cas, stub,
		&artifactCoordinatorClock{values: []time.Time{at.Add(time.Second), postCapture}})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := coordinator.Checkpoint(context.Background(),
		store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"result.txt"})
	if apiErr == nil || apiErr.Code != localapi.CodeOperationPending || !apiErr.Retryable ||
		closureCalls != 1 || operationCalls != 1 {
		t.Fatalf("post-capture fence = %#v, closure=%d operation=%d", apiErr, closureCalls, operationCalls)
	}
}

func TestArtifactCaptureCoordinatorClassifiesLiveAndStoreErrors(t *testing.T) {
	at := time.Date(2026, 7, 16, 23, 0, 0, 0, time.UTC)
	liveTests := []struct {
		err  error
		code localapi.ErrorCode
	}{
		{artifact.ErrArtifactLimit, localapi.CodeArtifactTooLarge},
		{artifact.ErrArtifactPath, localapi.CodeArtifactInvalid},
		{artifact.ErrArtifactChanged, localapi.CodeArtifactInvalid},
		{os.ErrNotExist, localapi.CodeArtifactInvalid},
		{artifact.ErrCASCorruption, localapi.CodeInternal},
		{context.Canceled, localapi.CodeOperationPending},
	}
	for index, test := range liveTests {
		t.Run("live-"+strconv.Itoa(index), func(t *testing.T) {
			operation := artifactCoordinatorStartedOperation(t, "live-"+strconv.Itoa(index), at,
				at.Add(time.Minute), nil)
			stub := &artifactCaptureStoreStub{}
			coordinator, err := NewArtifactCaptureCoordinator(
				artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) {
					return artifact.Closure{}, errors.Join(errors.New("capture failed"), test.err)
				}), artifactVerifierFunc(func(context.Context, artifact.Closure) error {
					t.Fatal("verifier called after capture error")
					return nil
				}), artifactCoordinatorLifecycle(t), stub,
				&artifactCoordinatorClock{values: []time.Time{at.Add(time.Second)}})
			if err != nil {
				t.Fatal(err)
			}
			_, apiErr := coordinator.Checkpoint(context.Background(),
				store.ManagedOperationReservation{Operation: operation, Acquired: true}, []string{"path"})
			if apiErr == nil || apiErr.Code != test.code || apiErr.OperationID == nil ||
				*apiErr.OperationID != operation.ID().String() {
				t.Fatalf("classified error = %#v, want %s", apiErr, test.code)
			}
		})
	}

	storeTests := []struct {
		err  error
		code localapi.ErrorCode
	}{
		{store.ErrOperationFence, localapi.CodeOperationPending},
		{store.ErrOperationMismatch, localapi.CodeOperationMismatch},
		{store.ErrArtifactConflict, localapi.CodeInternal},
	}
	for index, test := range storeTests {
		t.Run("store-"+strconv.Itoa(index), func(t *testing.T) {
			operation := artifactCoordinatorStartedOperation(t, "store-"+strconv.Itoa(index), at,
				at.Add(time.Minute), nil)
			stub := &artifactCaptureStoreStub{operation: func(context.Context, model.OperationID,
				string, time.Time, model.JSON,
			) (bool, error) {
				return false, test.err
			}}
			coordinator := artifactCoordinatorWithNoLiveCalls(t, stub,
				&artifactCoordinatorClock{values: []time.Time{at.Add(time.Second), at.Add(2 * time.Second)}})
			_, apiErr := coordinator.Checkpoint(context.Background(),
				store.ManagedOperationReservation{Operation: operation, Acquired: true}, nil)
			if apiErr == nil || apiErr.Code != test.code {
				t.Fatalf("classified Store error = %#v, want %s", apiErr, test.code)
			}
		})
	}
}

func artifactCoordinatorClosure(t *testing.T, at time.Time) (artifact.Closure, *artifact.CAS, string) {
	t.Helper()
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "result.txt"), []byte("managed Artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	cas, err := artifact.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	capturer, err := artifact.NewCapturer(workspace, cas, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	closure, err := capturer.Capture(context.Background(), []string{"result.txt"})
	if err != nil {
		t.Fatal(err)
	}
	return closure, cas, workspace
}

func artifactCoordinatorStartedOperation(t *testing.T, suffix string, createdAt, leaseUntil time.Time,
	checkpoint *model.JSON,
) model.Operation {
	t.Helper()
	id, _ := model.ParseOperationID("operation-artifact-" + suffix)
	run, _ := model.ParseRunID("run-artifact-" + suffix)
	operation, err := model.NewOperation(model.OperationSpec{ID: id, ProfileID: model.TeamworkProfileID(),
		AgentRunID: run, ClientKeyHash: model.Sum([]byte("key-" + suffix)),
		Kind: model.OperationTeamworkOffer, RequestDigest: model.Sum([]byte("request-" + suffix)),
		Status: model.OperationStarted, LeaseOwner: "run-owner-" + suffix, LeaseUntil: &leaseUntil,
		Capture: checkpoint, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func artifactCoordinatorTerminalOperation(t *testing.T, suffix string, createdAt time.Time,
	checkpoint *model.JSON,
) model.Operation {
	t.Helper()
	id, _ := model.ParseOperationID("operation-artifact-" + suffix)
	run, _ := model.ParseRunID("run-artifact-" + suffix)
	result, _ := model.NewJSON([]byte(`{"status":"accepted"}`))
	finishedAt := createdAt.Add(time.Second)
	operation, err := model.NewOperation(model.OperationSpec{ID: id, ProfileID: model.TeamworkProfileID(),
		AgentRunID: run, ClientKeyHash: model.Sum([]byte("key-" + suffix)),
		Kind: model.OperationTeamworkOffer, RequestDigest: model.Sum([]byte("request-" + suffix)),
		Status: model.OperationCommitted, Capture: checkpoint, Result: &result,
		CreatedAt: createdAt, FinishedAt: &finishedAt})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func artifactCoordinatorCheckpoint(t *testing.T, suffix string) model.JSON {
	t.Helper()
	checkpoint, err := buildArtifactCaptureCheckpoint([]ArtifactCaptureRoot{{
		ManifestDigest: model.Sum([]byte("manifest-" + suffix)),
		RootDigest:     model.Sum([]byte("root-" + suffix)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func artifactCoordinatorWithNoLiveCalls(t *testing.T, stub ArtifactCaptureStore,
	clock ServiceClock,
) *ArtifactCaptureCoordinator {
	t.Helper()
	coordinator, err := NewArtifactCaptureCoordinator(
		artifactCapturerFunc(func(context.Context, []string) (artifact.Closure, error) {
			t.Fatal("live capturer was called")
			return artifact.Closure{}, nil
		}),
		artifactVerifierFunc(func(context.Context, artifact.Closure) error {
			t.Fatal("live verifier was called")
			return nil
		}), artifactCoordinatorNoLifecycle(t), stub, clock)
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func artifactCoordinatorLifecycle(t *testing.T) *artifact.CAS {
	t.Helper()
	cas, err := artifact.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	return cas
}

func artifactCoordinatorNoLifecycle(t *testing.T) ArtifactCaptureLifecycle {
	t.Helper()
	return artifactCaptureLifecycleFunc(func() (*artifact.CASLease, error) {
		t.Fatal("Artifact CAS lifecycle was acquired")
		return nil, nil
	})
}

func artifactCoordinatorUnblock(channel chan<- struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func artifactCoordinatorAwait(t *testing.T, channel <-chan struct{}, stage string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatalf("timed out awaiting %s", stage)
	}
}

func artifactCoordinatorRequireBlocked(t *testing.T, result <-chan error, stage string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("exclusive collection entered during %s: %v", stage, err)
	case <-time.After(50 * time.Millisecond):
	}
}

func artifactCoordinatorRequireExclusive(t *testing.T, cas *artifact.CAS) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		lease, err := cas.AcquireExclusive()
		if err == nil {
			lease.Release()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("exclusive lifecycle acquisition = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exclusive lifecycle remained blocked after capture failure")
	}
}
