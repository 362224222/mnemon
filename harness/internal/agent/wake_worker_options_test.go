package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestValidateWakeWorkerOptionsNormalizesDefaults(t *testing.T) {
	at := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	options := wakeWorkerOptionsValidationFixture(t, at)
	lease, err := validateWakeWorkerOptions(&options)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := model.ParseHandlingBudget(options.Profile.HandlingBudget())
	if err != nil {
		t.Fatal(err)
	}
	if lease != time.Duration(budget.Spec().ClaimLeaseSeconds)*time.Second ||
		options.Clock == nil || options.Timer == nil ||
		options.PollInterval != defaultWakeWorkerPoll ||
		options.BackoffInterval != defaultWakeWorkerBackoff ||
		options.SettlementTimeout != defaultWakeWorkerSettlement {
		t.Fatalf("normalized options = %#v, lease %s", options, lease)
	}
}

func TestValidateWakeWorkerOptionsFailsClosed(t *testing.T) {
	if _, err := validateWakeWorkerOptions(nil); !errors.Is(err, ErrWakeWorker) {
		t.Fatalf("nil options error = %v", err)
	}
	options := wakeWorkerOptionsValidationFixture(t, time.Date(2026, 7, 18, 12, 5, 0, 0, time.UTC))
	options.PollInterval = time.Nanosecond
	if _, err := validateWakeWorkerOptions(&options); !errors.Is(err, ErrWakeWorker) {
		t.Fatalf("invalid duration error = %v", err)
	}
}

func wakeWorkerOptionsValidationFixture(t *testing.T, at time.Time) WakeWorkerOptions {
	t.Helper()
	profile := wakeWorkerTestProfile(t, at)
	return WakeWorkerOptions{
		Profile: profile, AssetRevision: profile.ActiveAssetRevision(),
		Store: newWakeWorkerTestStore(),
		Preparer: wakeWorkerPreparerFunc(func(context.Context,
			model.Profile,
		) (PreparedWake, error) {
			return PreparedWake{}, nil
		}),
		Adapter: wakeWorkerAdapterFunc(func(context.Context,
			CodexWakeRequest,
		) (CodexWakeResult, error) {
			return CodexWakeResult{}, nil
		}),
		Gate: WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
	}
}
