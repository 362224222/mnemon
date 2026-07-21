package agent

import (
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateWakeWorkerOptions(options *WakeWorkerOptions) (time.Duration, error) {
	if options == nil {
		return 0, fmt.Errorf("%w: options are required", ErrWakeWorker)
	}
	if err := validateWakeWorkerAuthority(*options); err != nil {
		return 0, err
	}
	normalizeWakeWorkerOptions(options)
	if err := validateWakeWorkerDurations(*options); err != nil {
		return 0, err
	}
	budget, err := model.ParseHandlingBudget(options.Profile.HandlingBudget())
	if err != nil {
		return 0, fmt.Errorf("%w: Profile handling budget is invalid", ErrWakeWorker)
	}
	return time.Duration(budget.Spec().ClaimLeaseSeconds) * time.Second, nil
}

func validateWakeWorkerAuthority(options WakeWorkerOptions) error {
	runtime, supported := model.RuntimeForHost(options.Profile.Host())
	if options.Profile.ID() != model.TeamworkProfileID() || !options.Profile.Enabled() ||
		!supported || options.Profile.Runtime() != runtime || options.AssetRevision == "" ||
		options.Profile.ActiveAssetRevision() != options.AssetRevision || options.Store == nil ||
		options.Preparer == nil || options.Adapter == nil || options.Gate == nil {
		return fmt.Errorf("%w: active managed Profile, Store, gate, preparer and adapter are required",
			ErrWakeWorker)
	}
	return nil
}

func normalizeWakeWorkerOptions(options *WakeWorkerOptions) {
	if options.Clock == nil {
		options.Clock = wallServiceClock{}
	}
	if options.Timer == nil {
		options.Timer = wallWakeWorkerTimer{}
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultWakeWorkerPoll
	}
	if options.BackoffInterval == 0 {
		options.BackoffInterval = defaultWakeWorkerBackoff
	}
	if options.SettlementTimeout == 0 {
		options.SettlementTimeout = defaultWakeWorkerSettlement
	}
}

func validateWakeWorkerDurations(options WakeWorkerOptions) error {
	if options.PollInterval < time.Millisecond || options.PollInterval > maxWakeWorkerPoll ||
		options.BackoffInterval < time.Millisecond || options.BackoffInterval > maxWakeWorkerBackoff ||
		options.SettlementTimeout < time.Millisecond ||
		options.SettlementTimeout > maxWakeWorkerSettlement {
		return fmt.Errorf("%w: poll, backoff or settlement bound is invalid", ErrWakeWorker)
	}
	return nil
}
