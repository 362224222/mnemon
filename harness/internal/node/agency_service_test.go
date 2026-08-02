package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
)

func TestLocalAgencySubmitCannotImplicitlyIssueCurrent(t *testing.T) {
	fixture := newLocalAgencyServiceFixture(t)
	authorityValue := agencyServiceAuthority(t, fixture.attachment, "operation:current-not-issued")
	intent, err := agency.NewAgentIntent(agency.IntentSpec{
		Kind:        agencyServiceLabel(t, "work.request"),
		Payload:     agencyServicePayload(t, "must observe a View first"),
		Consequence: agency.ConsequenceCreateHandlings,
		Successors:  []agency.TargetRef{agency.SelfTarget()},
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := NewAgencySubmission("operation:submit-without-current",
		intent.CanonicalJSON(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.AgencySubmit(fixture.ctx, authorityValue, submission); !errors.Is(err, authority.ErrCurrentUnavailable) {
		t.Fatalf("AgencySubmit(without Current) = %v, want ErrCurrentUnavailable", err)
	}
	if _, err := fixture.service.AgencyCurrent(fixture.ctx, authorityValue); err != nil {
		t.Fatalf("Current after rejected implicit issue = %v", err)
	}
}

func TestLocalAgencyServiceRunsAuthorityAndCASLoopWithoutR5State(t *testing.T) {
	fixture := newLocalAgencyServiceFixture(t)
	authorityValue := agencyServiceAuthority(t, fixture.attachment, "operation:current-reference")
	view, err := fixture.service.AgencyCurrent(fixture.ctx, authorityValue)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(view.CanonicalJSON(), fixture.attachment.Credential) ||
		bytes.Contains(view.CanonicalJSON(), []byte(fixture.attachment.ID)) {
		t.Fatal("Agent View exposed private attachment authority")
	}

	content := []byte("review the Artifact against its stated acceptance criteria")
	capture, err := fixture.service.AgencyCapture(fixture.ctx, content)
	if err != nil {
		t.Fatal(err)
	}
	if capture.Digest != AgencyContentDigest(content) || capture.ByteSize != int64(len(content)) {
		t.Fatalf("capture = %s/%d", capture.Digest, capture.ByteSize)
	}
	handle, err := agency.NewOpaqueHandle(capture.Handle)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := agency.NewArtifactCandidate(handle)
	if err != nil {
		t.Fatal(err)
	}
	publishOperation := "operation:publish-reference"
	publishIntent, err := agency.NewAgentIntent(agency.IntentSpec{
		Kind:         agencyServiceLabel(t, "knowledge.playbook"),
		Payload:      agencyServicePayload(t, "review before completion"),
		Consequence:  agency.ConsequencePublishReference,
		ReferenceKey: agencyServiceReferenceKey(t, "playbook.review"),
		Artifacts:    []agency.ArtifactInput{artifact},
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := NewAgencySubmission(publishOperation, publishIntent.CanonicalJSON(),
		[]AgencyCandidateBinding{{Handle: capture.Handle, Digest: capture.Digest}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := fixture.service.AgencySubmit(fixture.ctx, authorityValue, submission)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(receipt.CanonicalJSON(), []byte(`"outcome":"accepted"`)) ||
		!bytes.Contains(receipt.CanonicalJSON(), []byte(`"replayed":false`)) {
		t.Fatalf("first receipt = %s", receipt.CanonicalJSON())
	}
	replayed, err := fixture.service.AgencySubmit(fixture.ctx, authorityValue, submission)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(replayed.CanonicalJSON(), []byte(`"outcome":"accepted"`)) ||
		!bytes.Contains(replayed.CanonicalJSON(), []byte(`"replayed":true`)) {
		t.Fatalf("replayed receipt = %s", replayed.CanonicalJSON())
	}
	assertAgencyReceiptPrivate(t, replayed, [][]byte{fixture.attachment.Credential,
		[]byte(fixture.attachment.ID), []byte("operation:current-reference"),
		[]byte(publishOperation), []byte(capture.Digest)})

	fresh, err := fixture.service.AgencyCurrent(fixture.ctx,
		agencyServiceAuthority(t, fixture.attachment, "operation:current-after-reference"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(fresh.CanonicalJSON(), []byte(`"key":"playbook.review"`)) {
		t.Fatalf("fresh View omitted accepted Reference: %s", fresh.CanonicalJSON())
	}
}

type localAgencyServiceFixture struct {
	ctx        context.Context
	service    *LocalAgencyService
	attachment AgencyAttachment
}

func newLocalAgencyServiceFixture(t *testing.T) localAgencyServiceFixture {
	t.Helper()
	ctx := context.Background()
	adapter := newTestAdapter(t)
	authorityDir := filepath.Join(t.TempDir(), "authority")
	if err := os.Mkdir(authorityDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := authority.OpenWithArtifactVerifier(ctx,
		filepath.Join(authorityDir, "authority.sqlite"), adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	principal := agencyServicePrincipal(t, "principal:local-agency")
	if err := store.EnrollPrincipal(ctx, principal); err != nil {
		t.Fatal(err)
	}
	service, err := NewLocalAgencyService(principal, store, adapter, LocalAgencyServiceOptions{
		Clock:  agencyServiceClock{now: time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)},
		Random: bytes.NewReader(make([]byte, 32)),
	})
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := service.AgencyAttach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return localAgencyServiceFixture{ctx: ctx, service: service, attachment: attachment}
}

func assertAgencyReceiptPrivate(t *testing.T, receipt AgencyReceipt, privateValues [][]byte) {
	t.Helper()
	for _, private := range privateValues {
		if len(private) != 0 && bytes.Contains(receipt.CanonicalJSON(), private) {
			t.Fatalf("Agent Receipt exposed private authority %q", private)
		}
	}
}

type agencyServiceClock struct{ now time.Time }

func (clock agencyServiceClock) Now() time.Time { return clock.now }

func agencyServicePrincipal(t *testing.T, value string) agency.AgentPrincipalID {
	t.Helper()
	principal, err := agency.NewAgentPrincipalID(value)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func agencyServiceAuthority(t *testing.T, attachment AgencyAttachment,
	current string,
) AgencyAuthority {
	t.Helper()
	authorityValue, err := NewAgencyAuthority(attachment.ID, attachment.Credential, current)
	if err != nil {
		t.Fatal(err)
	}
	return authorityValue
}

func agencyServiceLabel(t *testing.T, value string) agency.SemanticLabel {
	t.Helper()
	label, err := agency.NewSemanticLabel(value)
	if err != nil {
		t.Fatal(err)
	}
	return label
}

func agencyServicePayload(t *testing.T, value string) agency.SemanticPayload {
	t.Helper()
	payload, err := agency.NewSemanticPayload(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func agencyServiceReferenceKey(t *testing.T, value string) agency.ReferenceKey {
	t.Helper()
	key, err := agency.NewReferenceKey(value)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
