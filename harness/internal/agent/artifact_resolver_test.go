package agent

import (
	"context"
	"errors"
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

type artifactResolverViewValidator struct {
	err     error
	calls   int
	current model.CurrentReadReceipt
	ref     model.CurrentArtifactRef
}

func (stub *artifactResolverViewValidator) Validate(_ context.Context,
	current model.CurrentReadReceipt, ref model.CurrentArtifactRef,
) error {
	stub.calls++
	stub.current, stub.ref = current, ref
	return stub.err
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
	resolver, err := NewArtifactResolver(stub, &artifactResolverViewValidator{})
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
	resolver, _ := NewArtifactResolver(stub, &artifactResolverViewValidator{})
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

func TestArtifactResolverMapsExactCurrentViewsAsReferencedWithoutRecapture(t *testing.T) {
	operation := artifactResolverOperation(t, "referenced", model.OperationTeamworkDeliver, nil)
	current, currentRef := artifactResolverCurrent(t, operation)
	viewPath, _ := currentRef.ViewPath()
	producedRoots := artifactResolverRoots("new-output")
	capture := &artifactResolverCheckpointer{result: artifactResolverCaptureResult(t, producedRoots, false)}
	views := &artifactResolverViewValidator{}
	resolver, err := NewArtifactResolver(capture, views)
	if err != nil {
		t.Fatal(err)
	}
	result, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: []string{viewPath, "new-output"}, Current: current, HasCurrent: true,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if views.calls != 1 || views.current.CanonicalJSON().String() != current.CanonicalJSON().String() ||
		views.ref.RootDigest() != currentRef.RootDigest() || capture.calls != 1 ||
		strings.Join(capture.paths, ",") != "new-output" || len(result.References) != 2 {
		t.Fatalf("referenced coordination = views %#v capture %#v refs %#v", views, capture, result.References)
	}
	roles := make(map[model.Digest]model.ArtifactRole)
	for _, ref := range result.References {
		roles[ref.RootDigest()] = ref.Role()
	}
	if roles[currentRef.RootDigest()] != model.ArtifactReferenced ||
		roles[producedRoots[0].RootDigest] != model.ArtifactProduced {
		t.Fatalf("resolved Artifact roles = %#v", roles)
	}

	views.err = errors.New("injected view mode drift")
	capture.calls = 0
	if _, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: []string{viewPath}, Current: current, HasCurrent: true,
	}); apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid || capture.calls != 0 {
		t.Fatalf("drifted view = %#v capture calls=%d", apiErr, capture.calls)
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
	resolver, _ := NewArtifactResolver(stub, &artifactResolverViewValidator{})
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
			resolver, _ := NewArtifactResolver(stub, &artifactResolverViewValidator{})
			_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
				Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
				Action:      operation.Kind(), Paths: []string{requested},
			})
			if apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid || stub.calls != 0 ||
				apiErr.OperationID == nil || *apiErr.OperationID != operation.ID().String() {
				t.Fatalf("path %q resolution = %#v, capture calls=%d", requested, apiErr, stub.calls)
			}
			if requested == ".mnemon/harness" && !strings.Contains(apiErr.Message, "current receipt") {
				t.Fatalf("readonly path diagnostic = %q", apiErr.Message)
			}
		})
	}
}

func TestArtifactResolverDoesNotOvermatchOrdinaryNearInternalPath(t *testing.T) {
	operation := artifactResolverOperation(t, "near-internal", model.OperationTeamworkOffer, nil)
	roots := artifactResolverRoots("near-internal")
	stub := &artifactResolverCheckpointer{result: artifactResolverCaptureResult(t, roots, false)}
	resolver, _ := NewArtifactResolver(stub, &artifactResolverViewValidator{})
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
	resolver, _ := NewArtifactResolver(stub, &artifactResolverViewValidator{})
	_, apiErr := resolver.Coordinate(context.Background(), ArtifactCoordinationSpec{
		Reservation: store.ManagedOperationReservation{Operation: operation, Acquired: true},
		Action:      operation.Kind(), Paths: []string{"result"},
	})
	if apiErr != pending || !apiErr.Retryable || stub.calls != 1 {
		t.Fatalf("stable capture error = %#v, calls=%d", apiErr, stub.calls)
	}

	t.Run("action mismatch", func(t *testing.T) {
		mismatch := &artifactResolverCheckpointer{}
		resolver, _ := NewArtifactResolver(mismatch, &artifactResolverViewValidator{})
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
		resolver, _ := NewArtifactResolver(forbidden, &artifactResolverViewValidator{})
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
		resolver, _ := NewArtifactResolver(drifted, &artifactResolverViewValidator{})
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

func artifactResolverCurrent(t *testing.T,
	operation model.Operation,
) (model.CurrentReadReceipt, model.CurrentArtifactRef) {
	t.Helper()
	root := model.Sum([]byte("current-resolver-root"))
	viewPath := ".mnemon/harness/node/views/" + operation.AgentRunID().String() + "/0/input.txt"
	view, err := model.NewCurrentArtifactView(root, viewPath)
	if err != nil {
		t.Fatal(err)
	}
	semantic, _ := model.NewCurrentArtifactRef(root)
	home, _ := model.ParsePeerID("peer-resolver-home")
	origin, _ := model.ParsePeerID("peer-resolver-origin")
	epoch, _ := model.ParseOriginEpoch("epoch-resolver-origin")
	eventID, _ := model.ParseEventID("event-resolver-current")
	key, _ := model.NewEventKey(origin, epoch, eventID)
	workID, _ := model.ParseWorkID("work-resolver-current")
	workRef, _ := model.NewWorkRef(home, workID)
	acceptedAt := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	deadline := acceptedAt.Add(24 * time.Hour)
	payload, _ := model.NewJSON([]byte(`{"content":"review","deadline":"2026-07-18T00:00:00Z","iteration":1,"work_version":1}`))
	event, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: key,
		Digest: model.Sum([]byte("event-resolver-current")), Type: model.EventReviewOffered,
		WorkRef: workRef, Summary: "Review", Payload: payload,
		ArtifactRefs: []model.CurrentArtifactRef{semantic}, AcceptedAt: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	brief, err := model.NewCurrentBrief(model.CurrentBriefSpec{Content: "review",
		DeadlineUnixNano: deadline.UnixNano(), ArtifactRefs: []model.CurrentArtifactRef{semantic}})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := model.NewJSON([]byte(`{"content":"review"}`))
	work, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: workRef, Version: 1, Iteration: 1,
		DeadlineUnixNano: deadline.UnixNano(), State: model.WorkOffered, StateData: state,
		LocalRole: model.CurrentReviewer, Brief: brief})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: event,
		ActionWork: work, AllowedActions: []model.OperationKind{model.OperationTeamworkDeliver},
		ArtifactViews: []model.CurrentArtifactRef{view}})
	if err != nil {
		t.Fatal(err)
	}
	handlingID, _ := model.ParseHandlingID("handling-resolver-current")
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{RunID: operation.AgentRunID(),
		ProfileID: model.TeamworkProfileID(), HandlingID: handlingID, HandlingAttempt: 1,
		Projection: projection, ReadAt: acceptedAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	return receipt, view
}

var _ ArtifactCaptureCheckpointer = (*artifactResolverCheckpointer)(nil)

var _ ArtifactViewValidator = (*artifactResolverViewValidator)(nil)
