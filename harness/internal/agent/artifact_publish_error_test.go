package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestAcceptedArtifactPublicationErrorClassification(t *testing.T) {
	operation, _ := model.ParseOperationID("operation-accepted-publish-errors")
	for _, test := range []struct {
		name string
		err  error
		code ControlErrorCode
	}{
		{name: "transient I/O", err: errors.New("temporary filesystem failure"),
			code: CodeOperationPending},
		{name: "cancelled", err: context.Canceled, code: CodeOperationPending},
		{name: "CAS corruption", err: artifact.ErrCASCorruption, code: CodeInternal},
		{name: "invalid manifest", err: artifact.ErrInvalidManifest, code: CodeInternal},
		{name: "closure mismatch", err: artifact.ErrClosureMismatch, code: CodeInternal},
		{name: "durable closure conflict", err: store.ErrArtifactStageConflict,
			code: CodeInternal},
		{name: "durable fence mismatch", err: store.ErrArtifactStageFence,
			code: CodeInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			apiErr := mapAcceptedArtifactPublicationError(test.err, operation)
			if apiErr == nil || apiErr.Code != test.code ||
				apiErr.Retryable != test.code.Retryable() {
				t.Fatalf("classification = %#v, want %s", apiErr, test.code)
			}
		})
	}
}
