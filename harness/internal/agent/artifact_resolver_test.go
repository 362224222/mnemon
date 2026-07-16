package agent

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type artifactResolverCheckpointer struct {
	result      ArtifactCaptureResult
	apiErr      *localapi.APIError
	calls       int
	reservation store.ManagedOperationReservation
	paths       []string
}

func (stub *artifactResolverCheckpointer) Checkpoint(_ context.Context,
	reservation store.ManagedOperationReservation, paths []string,
) (ArtifactCaptureResult, *localapi.APIError) {
	stub.calls++
	stub.reservation = reservation
	stub.paths = make([]string, len(paths))
	copy(stub.paths, paths)
	return stub.result, stub.apiErr
}

func TestArtifactResolverCapturesOrdinaryPathsAsCanonicalProducedRefs(t *testing.T) {
	operation := artifactResolverOperation(t, "produced", model.OperationTeamworkOffer, nil)
	roots := artifactResolverRoots("produced-a", "produced-b")
	stub := &artifactResolverCheckpointer{result: artifactResolverCaptureResult(t, roots, false)}
	resolver, err := NewArtifactResolver(stub)
	if err != nil {
		t.Fatal(err)
	}
	result, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: []string{`./outputs\one.txt`, "outputs/./two.txt"},
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if stub.calls != 1 || stub.reservation.Operation.ID() != operation.ID() ||
		strings.Join(stub.paths, ",") != "outputs/one.txt,outputs/two.txt" {
		t.Fatalf("capture bridge = calls %d operation %s paths %#v", stub.calls,
			stub.reservation.Operation.ID(), stub.paths)
	}
	if len(result.References) != 2 {
		t.Fatalf("produced refs = %#v", result.References)
	}
	for index, ref := range result.References {
		if ref.Role() != model.ArtifactProduced || ref.RootDigest() != roots[index].RootDigest {
			t.Fatalf("produced ref[%d] = root %s role %s", index, ref.RootDigest(), ref.Role())
		}
		if index > 0 && result.References[index-1].RootDigest().String() >= ref.RootDigest().String() {
			t.Fatal("produced refs are not canonical")
		}
	}
}

func TestArtifactResolverZeroPathsStillCreatesEmptyDurableCheckpoint(t *testing.T) {
	operation := artifactResolverOperation(t, "empty", model.OperationTeamworkOffer, nil)
	stub := &artifactResolverCheckpointer{result: artifactResolverCaptureResult(t, nil, false)}
	resolver, _ := NewArtifactResolver(stub)
	result, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: nil,
	})
	if apiErr != nil || stub.calls != 1 || stub.paths == nil || len(stub.paths) != 0 ||
		result.References == nil || len(result.References) != 0 {
		t.Fatalf("empty resolution = (%#v, %#v), calls=%d paths=%#v",
			result, apiErr, stub.calls, stub.paths)
	}
}

func TestArtifactResolverDurableReplayDoesNotInspectMissingWorkspacePath(t *testing.T) {
	checkpointRoots := artifactResolverRoots("replay")
	checkpoint, err := buildArtifactCaptureCheckpoint(checkpointRoots)
	if err != nil {
		t.Fatal(err)
	}
	operation := artifactResolverOperation(t, "replay", model.OperationTeamworkOffer, &checkpoint)
	stub := &artifactResolverCheckpointer{result: artifactResolverCaptureResult(t, checkpointRoots, true)}
	resolver, _ := NewArtifactResolver(stub)
	result, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Replayed: true, Acquired: true},
		Action:      operation.Kind(), Paths: []string{"workspace-file-that-no-longer-exists"},
	})
	if apiErr != nil || stub.calls != 1 || len(result.References) != 1 ||
		result.References[0].RootDigest() != checkpointRoots[0].RootDigest ||
		result.References[0].Role() != model.ArtifactProduced {
		t.Fatalf("resolver replay = (%#v, %#v), calls=%d", result, apiErr, stub.calls)
	}
}

func TestArtifactResolverRejectsInternalReadonlyAndEscapingPathsBeforeCapture(t *testing.T) {
	operation := artifactResolverOperation(t, "confinement", model.OperationTeamworkDeliver, nil)
	tests := []string{
		".", "./", ".mnemon", "./.MNEMON", ".mnemon/harness", "./.mnemon/harness/node/views/run/0", `.mnemon\harness\node`,
		`.MNEMON/HARNESS/node`, `.mnemon/./harness/node`, ".mnemon//harness/node",
		".mnemon/harness/../output", "dir/../../output", "../output", "/tmp/output",
		"output/", "output//file", "safe\x00file", string([]byte{0xff}),
	}
	for _, requested := range tests {
		t.Run(strings.ReplaceAll(requested, "/", "_"), func(t *testing.T) {
			stub := &artifactResolverCheckpointer{}
			resolver, _ := NewArtifactResolver(stub)
			_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
				Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
				Action:      operation.Kind(), Paths: []string{requested}, HasCurrent: true,
			})
			if apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid || stub.calls != 0 ||
				apiErr.OperationID == nil || *apiErr.OperationID != operation.ID().String() {
				t.Fatalf("path %q resolution = %#v, capture calls=%d", requested, apiErr, stub.calls)
			}
			if requested == ".mnemon/harness" && !strings.Contains(apiErr.Message, "view mapping") {
				t.Fatalf("readonly path diagnostic = %q", apiErr.Message)
			}
		})
	}
}

func TestArtifactResolverDoesNotOvermatchOrdinaryNearInternalPath(t *testing.T) {
	operation := artifactResolverOperation(t, "near-internal", model.OperationTeamworkOffer, nil)
	roots := artifactResolverRoots("near-internal")
	stub := &artifactResolverCheckpointer{result: artifactResolverCaptureResult(t, roots, false)}
	resolver, _ := NewArtifactResolver(stub)
	result, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: []string{".mnemon/harness-output/result.txt"},
	})
	if apiErr != nil || stub.calls != 1 || len(result.References) != 1 ||
		stub.paths[0] != ".mnemon/harness-output/result.txt" {
		t.Fatalf("near-internal path = (%#v, %#v), calls=%d paths=%#v",
			result, apiErr, stub.calls, stub.paths)
	}
}

func TestArtifactResolverPropagatesStableCaptureErrorsAndRejectsDrift(t *testing.T) {
	operation := artifactResolverOperation(t, "stable-error", model.OperationTeamworkOffer, nil)
	operationID := operation.ID().String()
	pending := localapi.NewAPIError(localapi.CodeOperationPending, "capture remains pending")
	pending.OperationID = &operationID
	stub := &artifactResolverCheckpointer{apiErr: pending}
	resolver, _ := NewArtifactResolver(stub)
	_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: []string{"result"},
	})
	if apiErr != pending || !apiErr.Retryable || stub.calls != 1 {
		t.Fatalf("stable capture error = %#v, calls=%d", apiErr, stub.calls)
	}

	t.Run("action mismatch", func(t *testing.T) {
		mismatch := &artifactResolverCheckpointer{}
		resolver, _ := NewArtifactResolver(mismatch)
		_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
			Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
			Action:      model.OperationTeamworkDeliver,
		})
		if apiErr == nil || apiErr.Code != localapi.CodeOperationMismatch || mismatch.calls != 0 {
			t.Fatalf("action mismatch = %#v, calls=%d", apiErr, mismatch.calls)
		}
	})

	t.Run("forbidden artifacts", func(t *testing.T) {
		closeOperation := artifactResolverOperation(t, "forbidden", model.OperationTeamworkClose, nil)
		forbidden := &artifactResolverCheckpointer{}
		resolver, _ := NewArtifactResolver(forbidden)
		_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
			Reservation: store.ManagedOperationReservation{Operation: closeOperation, Acquired: true},
			Action:      closeOperation.Kind(), Paths: []string{"result"},
		})
		if apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid || forbidden.calls != 0 {
			t.Fatalf("forbidden Artifact = %#v, calls=%d", apiErr, forbidden.calls)
		}
	})

	t.Run("checkpoint drift", func(t *testing.T) {
		roots := artifactResolverRoots("expected")
		drift := artifactResolverRoots("different")
		drifted := &artifactResolverCheckpointer{result: ArtifactCaptureResult{
			Checkpoint: artifactResolverCheckpoint(t, roots), Roots: drift,
		}}
		resolver, _ := NewArtifactResolver(drifted)
		_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
			Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
			Action:      operation.Kind(), Paths: []string{"result"},
		})
		if apiErr == nil || apiErr.Code != localapi.CodeInternal || drifted.calls != 1 {
			t.Fatalf("checkpoint drift = %#v, calls=%d", apiErr, drifted.calls)
		}
	})
}

func artifactResolverCaptureResult(t *testing.T, roots []ArtifactCaptureRoot,
	replayed bool,
) ArtifactCaptureResult {
	t.Helper()
	if roots == nil {
		roots = make([]ArtifactCaptureRoot, 0)
	}
	return ArtifactCaptureResult{Checkpoint: artifactResolverCheckpoint(t, roots),
		Roots: append([]ArtifactCaptureRoot(nil), roots...), Replayed: replayed}
}

func artifactResolverCheckpoint(t *testing.T, roots []ArtifactCaptureRoot) model.JSON {
	t.Helper()
	checkpoint, err := buildArtifactCaptureCheckpoint(roots)
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func artifactResolverRoots(suffixes ...string) []ArtifactCaptureRoot {
	roots := make([]ArtifactCaptureRoot, len(suffixes))
	for index, suffix := range suffixes {
		roots[index] = ArtifactCaptureRoot{ManifestDigest: model.Sum([]byte("manifest-" + suffix)),
			RootDigest: model.Sum([]byte("root-" + suffix))}
	}
	sort.Slice(roots, func(left, right int) bool {
		return roots[left].RootDigest.String() < roots[right].RootDigest.String()
	})
	return roots
}

func artifactResolverOperation(t *testing.T, suffix string, kind model.OperationKind,
	checkpoint *model.JSON,
) model.Operation {
	t.Helper()
	at := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	leaseUntil := at.Add(time.Minute)
	id, _ := model.ParseOperationID("operation-resolver-" + suffix)
	run, _ := model.ParseRunID("run-resolver-" + suffix)
	var contextHash *model.Digest
	if kind != model.OperationTeamworkOffer {
		value := model.Sum([]byte("context-" + suffix))
		contextHash = &value
	}
	operation, err := model.NewOperation(model.OperationSpec{ID: id, ProfileID: model.TeamworkProfileID(),
		AgentRunID: run, ClientKeyHash: model.Sum([]byte("key-" + suffix)), ContextHash: contextHash,
		Kind: kind, RequestDigest: model.Sum([]byte("request-" + suffix)), Status: model.OperationStarted,
		LeaseOwner: "run-owner-" + suffix, LeaseUntil: &leaseUntil, Capture: checkpoint, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

var _ ArtifactCaptureCheckpointer = (*artifactResolverCheckpointer)(nil)
