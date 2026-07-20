package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelAuthorityCoordinatorAcceptSignsFreshTwiceAndReplayZeroTimes(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "accept-sign-count")
	trace := []string{}
	signer := newChannelAuthorityTestSigner(t, fixture.owner)
	coordinator, err := newChannelAuthorityCoordinator(fixture.store,
		newChannelAuthorityRuntimeTrace(&trace), signer)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := coordinator.AcceptEnrollmentAuthority(context.Background(), fixture.acceptance)
	if err != nil || accepted.Status != peer.ChannelEnrollmentAccepted ||
		accepted.Member.PeerID() != fixture.joiner.PeerID() || len(accepted.Roster) != 2 ||
		signer.calls.Load() != 2 {
		t.Fatalf("fresh AcceptEnrollmentAuthority() = (%#v,%v), signer calls=%d",
			accepted, err, signer.calls.Load())
	}
	originalMember, originalReceipt := accepted.Member, accepted.Receipt
	accepted.Roster[0] = model.Member{}
	replayed, err := coordinator.AcceptEnrollmentAuthority(context.Background(), fixture.acceptance)
	if err != nil || replayed.Status != peer.ChannelEnrollmentReplayed ||
		replayed.Member.Head() != originalMember.Head() ||
		!bytes.Equal(replayed.Receipt.WireJSON().Bytes(), originalReceipt.WireJSON().Bytes()) ||
		len(replayed.Roster) != 2 || replayed.Roster[0].IsZero() || signer.calls.Load() != 2 {
		t.Fatalf("replayed AcceptEnrollmentAuthority() = (%#v,%v), signer calls=%d",
			replayed, err, signer.calls.Load())
	}
	assertChannelAuthorityTrace(t, trace, "begin", "install")
}

func TestChannelAuthorityCoordinatorSignerFailureLeavesAuthorityUnchanged(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "accept-sign-failure")
	before, err := fixture.store.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected owner signer failure")
	signer := newChannelAuthorityTestSigner(t, fixture.owner)
	signer.failAt, signer.failure = 1, injected
	trace := []string{}
	coordinator, err := newChannelAuthorityCoordinator(fixture.store,
		newChannelAuthorityRuntimeTrace(&trace), signer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.AcceptEnrollmentAuthority(context.Background(), fixture.acceptance)
	var controlFailure *peer.ChannelEnrollmentControlFailure
	if !errors.Is(err, injected) || !errors.Is(err, ErrChannelAuthority) ||
		errors.As(err, &controlFailure) || signer.calls.Load() != 1 {
		t.Fatalf("signer failure = %v, control=%#v, calls=%d", err, controlFailure,
			signer.calls.Load())
	}
	after, readErr := fixture.store.ReadChannelMeshAuthority(context.Background())
	if readErr != nil || !reflect.DeepEqual(before, after) || len(trace) != 0 {
		t.Fatalf("authority after signer failure = (%#v,%v), trace=%v, before=%#v",
			after, readErr, trace, before)
	}
}

func TestChannelAuthorityCoordinatorEnrollmentResponseLossResolution(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		resolution    store.ChannelAuthorityPlanResolution
		commit        bool
		retry         bool
		wantStatus    peer.ChannelEnrollmentStatus
		wantErr       bool
		wantAuthority bool
		wantTrace     []string
	}{
		{name: "candidate installs and replays", commit: true, retry: true,
			wantStatus: peer.ChannelEnrollmentAccepted,
			wantTrace:  []string{"begin", "install"}},
		{name: "unchanged aborts", resolution: store.ChannelAuthorityPlanUnchanged,
			wantErr: true, wantTrace: []string{"begin", "abort"}},
		{name: "diverged fails closed", resolution: store.ChannelAuthorityPlanDiverged, commit: true,
			wantErr: true, wantAuthority: true, wantTrace: []string{"begin", "fail_closed"}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChannelAuthorityEnrollmentFixture(t, "response-loss-"+test.name)
			responseLost := errors.New("enrollment commit response lost")
			wrapped := &channelEnrollmentResponseLossStore{Store: fixture.store,
				commitErr: responseLost, resolution: test.resolution, commit: test.commit,
				loseOnce: test.retry}
			var trace []string
			signer := newChannelAuthorityTestSigner(t, fixture.owner)
			coordinator, err := newChannelAuthorityCoordinator(wrapped,
				newChannelAuthorityRuntimeTrace(&trace), signer)
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := coordinator.AcceptEnrollmentAuthority(context.Background(),
				fixture.acceptance)
			if test.wantErr {
				var controlFailure *peer.ChannelEnrollmentControlFailure
				if !errors.Is(err, responseLost) ||
					(errors.Is(err, ErrChannelAuthority) != test.wantAuthority) ||
					errors.As(err, &controlFailure) {
					t.Fatalf("response loss error = %v, control=%#v", err, controlFailure)
				}
			} else if err != nil || accepted.Status != test.wantStatus {
				t.Fatalf("candidate response loss = (%#v,%v)", accepted, err)
			}
			if test.retry {
				replayed, replayErr := coordinator.AcceptEnrollmentAuthority(context.Background(),
					fixture.acceptance)
				if replayErr != nil || replayed.Status != peer.ChannelEnrollmentReplayed ||
					signer.calls.Load() != 2 {
					t.Fatalf("response-loss replay = (%#v,%v), signer calls=%d",
						replayed, replayErr, signer.calls.Load())
				}
			}
			if !reflect.DeepEqual(trace, test.wantTrace) {
				t.Fatalf("response-loss trace = %v, want %v", trace, test.wantTrace)
			}
		})
	}
}

func TestChannelAuthorityCoordinatorResolvesNilErrorCommitResultMismatch(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		commit    bool
		mutate    func(*store.AcceptChannelEnrollmentResult)
		wantOK    bool
		wantTrace []string
	}{
		{name: "committed mismatch resolves candidate", commit: true, wantOK: true,
			mutate: func(result *store.AcceptChannelEnrollmentResult) {
				result.Status = store.ChannelEnrollmentReplayed
			}, wantTrace: []string{"begin", "install"}},
		{name: "uncommitted forged result resolves unchanged",
			wantTrace: []string{"begin", "abort"}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChannelAuthorityEnrollmentFixture(t, "result-mismatch-"+test.name)
			wrapped := &channelEnrollmentResultMismatchStore{Store: fixture.store,
				commit: test.commit, mutate: test.mutate}
			var trace []string
			coordinator, err := newChannelAuthorityCoordinator(wrapped,
				newChannelAuthorityRuntimeTrace(&trace),
				newChannelAuthorityTestSigner(t, fixture.owner))
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := coordinator.AcceptEnrollmentAuthority(context.Background(),
				fixture.acceptance)
			if test.wantOK {
				if err != nil || accepted.Status != peer.ChannelEnrollmentAccepted {
					t.Fatalf("resolved committed mismatch = (%#v,%v)", accepted, err)
				}
			} else {
				var failure *peer.ChannelEnrollmentControlFailure
				if !errors.Is(err, ErrChannelAuthority) || errors.As(err, &failure) {
					t.Fatalf("uncommitted forged result = %v, control=%#v", err, failure)
				}
			}
			if !reflect.DeepEqual(trace, test.wantTrace) {
				t.Fatalf("result-mismatch trace = %v, want %v", trace, test.wantTrace)
			}
		})
	}
}

type channelEnrollmentResultMismatchStore struct {
	*store.Store
	commit bool
	mutate func(*store.AcceptChannelEnrollmentResult)
}

func (st *channelEnrollmentResultMismatchStore) CommitChannelEnrollment(ctx context.Context,
	plan store.ChannelEnrollmentPlan,
) (store.AcceptChannelEnrollmentResult, error) {
	if !st.commit {
		return store.AcceptChannelEnrollmentResult{}, nil
	}
	result, err := st.Store.CommitChannelEnrollment(ctx, plan)
	if err == nil && st.mutate != nil {
		st.mutate(&result)
	}
	return result, err
}

type channelEnrollmentResponseLossStore struct {
	*store.Store
	commitErr  error
	resolution store.ChannelAuthorityPlanResolution
	commit     bool
	loseOnce   bool
	lost       atomic.Bool
}

func (st *channelEnrollmentResponseLossStore) CommitChannelEnrollment(ctx context.Context,
	plan store.ChannelEnrollmentPlan,
) (store.AcceptChannelEnrollmentResult, error) {
	if !st.commit {
		return store.AcceptChannelEnrollmentResult{}, st.commitErr
	}
	result, err := st.Store.CommitChannelEnrollment(ctx, plan)
	if err != nil {
		return store.AcceptChannelEnrollmentResult{}, err
	}
	if st.loseOnce && !st.lost.CompareAndSwap(false, true) {
		return result, nil
	}
	return store.AcceptChannelEnrollmentResult{}, st.commitErr
}

func (st *channelEnrollmentResponseLossStore) ResolveChannelEnrollment(ctx context.Context,
	plan store.ChannelEnrollmentPlan,
) (store.ChannelAuthorityPlanResolution, error) {
	if st.resolution.Valid() {
		return st.resolution, nil
	}
	return st.Store.ResolveChannelEnrollment(ctx, plan)
}

func TestMapChannelEnrollmentPrecommitErrorUsesOnlyStableDomainRejections(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		cause error
		code  peer.ChannelProtocolErrorCode
	}{
		{store.ErrChannelEnrollmentOwner, peer.ChannelErrorWrongOwner},
		{store.ErrChannelEnrollmentProof, peer.ChannelErrorBadProof},
		{store.ErrChannelEnrollmentTokenExpired, peer.ChannelErrorTokenExpired},
		{store.ErrChannelEnrollmentTokenClosed, peer.ChannelErrorTokenClosed},
		{store.ErrChannelEnrollmentTokenExhausted, peer.ChannelErrorTokenExhausted},
		{store.ErrChannelFull, peer.ChannelErrorChannelFull},
		{store.ErrChannelEnrollmentChannelClosed, peer.ChannelErrorChannelClosed},
		{store.ErrChannelEnrollmentMemberRevoked, peer.ChannelErrorMemberRevoked},
		{store.ErrChannelEnrollmentStale, peer.ChannelErrorRosterGap},
		{store.ErrChannelEnrollmentConflict, peer.ChannelErrorRosterConflict},
		{store.ErrChannelEnrollmentUnavailable, peer.ChannelErrorInvalidToken},
		{store.ErrChannelEnrollmentInput, peer.ChannelErrorInvalidToken},
	} {
		mapped := mapChannelEnrollmentPrecommitError(fmt.Errorf("Store rejection: %w", test.cause))
		var failure *peer.ChannelEnrollmentControlFailure
		if !errors.As(mapped, &failure) || failure.Code() != test.code ||
			!errors.Is(mapped, peer.ErrChannelEnrollmentControlFailure) ||
			errors.Is(mapped, test.cause) {
			t.Fatalf("map enrollment rejection %v = %#v, want code %s",
				test.cause, mapped, test.code)
		}
	}
	for _, internal := range []error{errors.New("internal signer or runtime failure"),
		store.ErrChannelAuthorityInvariant, store.ErrChannelAuthorityPlan,
		store.ErrChannelAuthorityPlanDiverged, context.Canceled, context.DeadlineExceeded} {
		if mapped := mapChannelEnrollmentPrecommitError(internal); mapped != internal {
			t.Fatalf("internal failure was projected as protocol rejection: %v", mapped)
		}
	}
}

func TestChannelAuthorityCoordinatorRequiresSigner(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "required-signer")
	trace := []string{}
	runtime := newChannelAuthorityRuntimeTrace(&trace)
	if coordinator, err := newChannelAuthorityCoordinator(fixture.store, runtime, nil); coordinator != nil || !errors.Is(err, ErrChannelAuthority) {
		t.Fatalf("nil signer constructor = (%#v,%v)", coordinator, err)
	}
	var typedNil *channelAuthorityTestSigner
	if coordinator, err := newChannelAuthorityCoordinator(fixture.store, runtime, typedNil); coordinator != nil || !errors.Is(err, ErrChannelAuthority) {
		t.Fatalf("typed nil signer constructor = (%#v,%v)", coordinator, err)
	}
}

func TestChannelAuthorityCoordinatorEnrollmentFailureProjectionIsPhaseBound(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		phase     string
		cause     error
		wantCode  peer.ChannelProtocolErrorCode
		wantTrace []string
	}{
		{name: "challenge business rejection is typed", phase: "challenge",
			cause: store.ErrChannelEnrollmentProof, wantCode: peer.ChannelErrorBadProof},
		{name: "challenge invariant resets", phase: "challenge",
			cause: store.ErrChannelAuthorityInvariant},
		{name: "signing prepare business rejection is typed", phase: "signing",
			cause: store.ErrChannelEnrollmentStale, wantCode: peer.ChannelErrorRosterGap},
		{name: "signed prepare business-looking error resets", phase: "signed",
			cause: store.ErrChannelEnrollmentConflict},
		{name: "commit business-looking error resets", phase: "commit",
			cause: store.ErrChannelEnrollmentStale, wantTrace: []string{"begin", "abort"}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChannelAuthorityEnrollmentFixture(t, "phase-"+test.phase+"-"+test.name)
			wrapped := &channelEnrollmentPhaseFailureStore{Store: fixture.store}
			switch test.phase {
			case "challenge":
				wrapped.challengeErr = test.cause
			case "signing":
				wrapped.signingErr = test.cause
			case "signed":
				wrapped.signedErr = test.cause
			case "commit":
				wrapped.commitErr = test.cause
			}
			var trace []string
			coordinator, err := newChannelAuthorityCoordinator(wrapped,
				newChannelAuthorityRuntimeTrace(&trace),
				newChannelAuthorityTestSigner(t, fixture.owner))
			if err != nil {
				t.Fatal(err)
			}
			if test.phase == "challenge" {
				_, err = coordinator.PrepareEnrollmentChallenge(context.Background(), fixture.challenge)
			} else {
				_, err = coordinator.AcceptEnrollmentAuthority(context.Background(), fixture.acceptance)
			}
			var failure *peer.ChannelEnrollmentControlFailure
			if test.wantCode != "" {
				if !errors.As(err, &failure) || failure.Code() != test.wantCode ||
					errors.Is(err, test.cause) {
					t.Fatalf("typed phase failure = %v, control=%#v", err, failure)
				}
			} else if !errors.Is(err, test.cause) || errors.As(err, &failure) {
				t.Fatalf("internal phase failure = %v, control=%#v", err, failure)
			}
			if !reflect.DeepEqual(trace, test.wantTrace) {
				t.Fatalf("phase failure trace = %v, want %v", trace, test.wantTrace)
			}
		})
	}
}

type channelEnrollmentPhaseFailureStore struct {
	*store.Store
	challengeErr error
	signingErr   error
	signedErr    error
	commitErr    error
}

func (st *channelEnrollmentPhaseFailureStore) PrepareChannelEnrollment(ctx context.Context,
	spec store.PrepareChannelEnrollmentSpec,
) (store.PrepareChannelEnrollmentResult, error) {
	if st.challengeErr != nil {
		return store.PrepareChannelEnrollmentResult{}, st.challengeErr
	}
	return st.Store.PrepareChannelEnrollment(ctx, spec)
}

func (st *channelEnrollmentPhaseFailureStore) PrepareChannelEnrollmentSigning(ctx context.Context,
	spec store.PrepareChannelEnrollmentSigningSpec,
) (store.ChannelEnrollmentSigningPlan, error) {
	if st.signingErr != nil {
		return store.ChannelEnrollmentSigningPlan{}, st.signingErr
	}
	return st.Store.PrepareChannelEnrollmentSigning(ctx, spec)
}

func (st *channelEnrollmentPhaseFailureStore) PrepareSignedChannelEnrollment(ctx context.Context,
	plan store.ChannelEnrollmentSigningPlan, signatures store.ChannelEnrollmentSignatures,
) (store.ChannelEnrollmentPlan, error) {
	if st.signedErr != nil {
		return store.ChannelEnrollmentPlan{}, st.signedErr
	}
	return st.Store.PrepareSignedChannelEnrollment(ctx, plan, signatures)
}

func (st *channelEnrollmentPhaseFailureStore) CommitChannelEnrollment(ctx context.Context,
	plan store.ChannelEnrollmentPlan,
) (store.AcceptChannelEnrollmentResult, error) {
	if st.commitErr != nil {
		return store.AcceptChannelEnrollmentResult{}, st.commitErr
	}
	return st.Store.CommitChannelEnrollment(ctx, plan)
}

func TestChannelAuthorityCoordinatorCopiesAliasedSignerBuffers(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "aliased-signer")
	signer := &channelAuthorityAliasingSigner{delegate: newChannelAuthorityTestSigner(t, fixture.owner),
		shared: make([]byte, ed25519.SignatureSize), store: fixture.store}
	trace := []string{}
	coordinator, err := newChannelAuthorityCoordinator(fixture.store,
		newChannelAuthorityRuntimeTrace(&trace), signer)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := coordinator.AcceptEnrollmentAuthority(context.Background(), fixture.acceptance)
	if err != nil || accepted.Status != peer.ChannelEnrollmentAccepted || signer.calls.Load() != 2 {
		t.Fatalf("aliased signer acceptance = (%#v,%v), calls=%d",
			accepted, err, signer.calls.Load())
	}
}

type channelAuthorityAliasingSigner struct {
	delegate *channelAuthorityTestSigner
	shared   []byte
	store    *store.Store
	calls    atomic.Int64
}

func (signer *channelAuthorityAliasingSigner) Sign(ctx context.Context,
	message []byte,
) ([]byte, error) {
	signer.calls.Add(1)
	if _, err := signer.store.ReadChannelMeshAuthority(ctx); err != nil {
		return nil, err
	}
	signature, err := signer.delegate.Sign(ctx, message)
	if err != nil {
		return nil, err
	}
	copy(signer.shared, signature)
	return signer.shared, nil
}

func TestChannelAuthorityCoordinatorRejectsInvalidSignerWithoutTypedFailure(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "invalid-signer")
	signer := &channelAuthorityInvalidSigner{}
	trace := []string{}
	coordinator, err := newChannelAuthorityCoordinator(fixture.store,
		newChannelAuthorityRuntimeTrace(&trace), signer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.AcceptEnrollmentAuthority(context.Background(), fixture.acceptance)
	var failure *peer.ChannelEnrollmentControlFailure
	if !errors.Is(err, store.ErrChannelEnrollmentOwner) || errors.As(err, &failure) ||
		signer.calls.Load() != 2 || len(trace) != 0 {
		t.Fatalf("invalid signer failure = %v, control=%#v, calls=%d, trace=%v",
			err, failure, signer.calls.Load(), trace)
	}
}

type channelAuthorityInvalidSigner struct{ calls atomic.Int64 }

func (signer *channelAuthorityInvalidSigner) Sign(context.Context, []byte) ([]byte, error) {
	signer.calls.Add(1)
	return make([]byte, ed25519.SignatureSize), nil
}

func TestChannelEnrollmentAcceptanceProjectionBindsExactRequestAndResult(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "projection-binding")
	result, err := fixture.store.AcceptChannelEnrollment(context.Background(),
		store.AcceptChannelEnrollmentSpec{AuthenticatedPeerID: fixture.acceptance.AuthenticatedPeerID,
			Transcript:           fixture.acceptance.Transcript,
			AdvertisedMultiaddrs: append([]string(nil), fixture.acceptance.AdvertisedMultiaddrs...),
			Proof:                fixture.acceptance.Proof, Signer: newChannelAuthorityTestSigner(t, fixture.owner),
			At: fixture.acceptance.At})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := channelEnrollmentAcceptanceAuthority(fixture.acceptance, result)
	if err != nil || projected.Status != peer.ChannelEnrollmentAccepted || len(projected.Roster) != 2 {
		t.Fatalf("valid acceptance projection = (%#v,%v)", projected, err)
	}
	other := newChannelAuthorityEnrollmentFixture(t, "projection-binding-other")
	for _, test := range []struct {
		name    string
		control peer.ChannelEnrollmentAcceptanceControl
		mutate  func(*store.AcceptChannelEnrollmentResult)
	}{
		{name: "wrong authenticated peer", control: func() peer.ChannelEnrollmentAcceptanceControl {
			control := fixture.acceptance
			control.AuthenticatedPeerID = fixture.owner.PeerID()
			return control
		}()},
		{name: "wrong advertised addresses", control: func() peer.ChannelEnrollmentAcceptanceControl {
			control := fixture.acceptance
			control.AdvertisedMultiaddrs = other.joiner.Multiaddrs()
			return control
		}()},
		{name: "wrong transcript", control: other.acceptance},
		{name: "wrong Channel", control: fixture.acceptance,
			mutate: func(value *store.AcceptChannelEnrollmentResult) { value.Channel = model.Channel{} }},
		{name: "wrong historical member", control: fixture.acceptance,
			mutate: func(value *store.AcceptChannelEnrollmentResult) {
				value.Member = value.Roster.Members()[0]
			}},
		{name: "wrong roster", control: fixture.acceptance,
			mutate: func(value *store.AcceptChannelEnrollmentResult) {
				value.Roster = fixture.channel.Roster()
			}},
		{name: "wrong receipt", control: fixture.acceptance,
			mutate: func(value *store.AcceptChannelEnrollmentResult) {
				value.Receipt = model.EnrollmentReceipt{}
			}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			candidate := result
			if test.mutate != nil {
				test.mutate(&candidate)
			}
			_, err := channelEnrollmentAcceptanceAuthority(test.control, candidate)
			var controlFailure *peer.ChannelEnrollmentControlFailure
			if !errors.Is(err, ErrChannelAuthority) || errors.As(err, &controlFailure) {
				t.Fatalf("mismatched acceptance projection = %v, control=%#v", err, controlFailure)
			}
		})
	}
	wrongStatus := result
	wrongStatus.Status = store.ChannelEnrollmentReplayed
	if sameChannelEnrollmentResult(result, wrongStatus) {
		t.Fatal("exact prepared-result fence accepted a changed enrollment status")
	}
}

func TestChannelAuthorityCoordinatorRejectsMismatchedEnrollmentClaims(t *testing.T) {
	t.Parallel()
	fixture := newChannelAuthorityEnrollmentFixture(t, "mismatched-claims")
	trace := []string{}
	coordinator, err := newChannelAuthorityCoordinator(fixture.store,
		newChannelAuthorityRuntimeTrace(&trace), newChannelAuthorityTestSigner(t, fixture.owner))
	if err != nil {
		t.Fatal(err)
	}
	other := newChannelAuthorityEnrollmentFixture(t, "mismatched-claims-other")
	for _, test := range []struct {
		mutate func(*peer.ChannelEnrollmentChallengeControl)
		code   peer.ChannelProtocolErrorCode
	}{
		{mutate: func(control *peer.ChannelEnrollmentChallengeControl) {
			control.RequestID = other.challenge.RequestID
		}, code: peer.ChannelErrorBadProof},
		{mutate: func(control *peer.ChannelEnrollmentChallengeControl) {
			control.JoinerPublicKey = other.challenge.JoinerPublicKey
		}, code: peer.ChannelErrorInvalidToken},
		{mutate: func(control *peer.ChannelEnrollmentChallengeControl) {
			control.JoinerOriginEpoch = other.challenge.JoinerOriginEpoch
		}, code: peer.ChannelErrorBadProof},
	} {
		control := fixture.challenge
		control.JoinerPublicKey = append([]byte(nil), fixture.challenge.JoinerPublicKey...)
		test.mutate(&control)
		_, err := coordinator.PrepareEnrollmentChallenge(context.Background(), control)
		var failure *peer.ChannelEnrollmentControlFailure
		if !errors.As(err, &failure) || failure.Code() != test.code {
			t.Fatalf("mismatched challenge = %v, control=%#v", err, failure)
		}
	}
	badProof := fixture.acceptance
	badProof.Proof = model.Sum([]byte("wrong owner enrollment proof"))
	_, err = coordinator.AcceptEnrollmentAuthority(context.Background(), badProof)
	assertChannelEnrollmentControlCode(t, err, peer.ChannelErrorBadProof)
	badAddresses := fixture.acceptance
	badAddresses.AdvertisedMultiaddrs = fixture.owner.Multiaddrs()
	_, err = coordinator.AcceptEnrollmentAuthority(context.Background(), badAddresses)
	assertChannelEnrollmentControlCode(t, err, peer.ChannelErrorInvalidToken)
	if len(trace) != 0 {
		t.Fatalf("rejected claims touched runtime: %v", trace)
	}
}

func assertChannelEnrollmentControlCode(t testing.TB, err error,
	want peer.ChannelProtocolErrorCode,
) {
	t.Helper()
	var failure *peer.ChannelEnrollmentControlFailure
	if !errors.As(err, &failure) || failure.Code() != want {
		t.Fatalf("enrollment failure = %v, control=%#v, want %s", err, failure, want)
	}
}

func TestNewChannelAuthorityCoordinatorBindsNodeIdentity(t *testing.T) {
	fixture := newRealChannelAuthorityCoordinatorFixture(t)
	wrong := newChannelAuthorityNodeIdentity(t,
		testkit.NewIdentity(t, "node-owner-enrollment-wrong-local-identity"))
	if coordinator, err := NewChannelAuthorityCoordinator(context.Background(), fixture.store,
		fixture.runtime, wrong); coordinator != nil || !errors.Is(err, ErrChannelAuthority) {
		t.Fatalf("wrong Node identity constructor = (%#v,%v)", coordinator, err)
	}
	if coordinator, err := NewChannelAuthorityCoordinator(nil, fixture.store,
		fixture.runtime, newChannelAuthorityNodeIdentity(t, fixture.channel.Owner())); coordinator != nil || !errors.Is(err, ErrChannelAuthority) {
		t.Fatalf("nil context constructor = (%#v,%v)", coordinator, err)
	}
}
