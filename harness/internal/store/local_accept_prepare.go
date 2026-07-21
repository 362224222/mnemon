package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func prepareLocalAcceptanceTx(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	trustedNow time.Time,
) (model.Operation, time.Time, time.Time, error) {
	if err := validateLocalAcceptanceTxInput(ctx, tx, spec); err != nil {
		return model.Operation{}, time.Time{}, time.Time{}, err
	}
	operation, err := readStartedLocalAcceptanceOperation(ctx, tx, spec.Operation)
	if err != nil {
		return model.Operation{}, time.Time{}, time.Time{}, err
	}
	trustedNow, acceptedAt, err := normalizeLocalAcceptanceTimes(spec, trustedNow)
	if err != nil {
		return model.Operation{}, time.Time{}, time.Time{}, err
	}
	if err := requireLocalAcceptanceFence(operation, spec.Operation, trustedNow); err != nil {
		return model.Operation{}, time.Time{}, time.Time{}, err
	}
	return operation, acceptedAt, trustedNow, nil
}

func validateLocalAcceptanceTxInput(ctx context.Context, tx *sql.Tx,
	spec LocalAcceptanceSpec,
) error {
	if ctx == nil || tx == nil {
		return errors.New("commit local acceptance: nil context or transaction")
	}
	if spec.Controller == (spec.Operation != nil) {
		return errors.New("commit local acceptance: choose controller or operation authority")
	}
	if len(spec.Items) == 0 || len(spec.Items) > model.MaxChildWorks ||
		spec.Scope.Count() != uint8(len(spec.Items)) {
		return errors.New("commit local acceptance: batch must contain exactly Scope count 1..7")
	}
	return nil
}

func readStartedLocalAcceptanceOperation(ctx context.Context, tx *sql.Tx,
	authority *LocalOperationAuthority,
) (model.Operation, error) {
	if authority == nil {
		return model.Operation{}, nil
	}
	operation, err := readLocalAcceptanceOperation(ctx, tx, authority)
	if err != nil {
		return model.Operation{}, err
	}
	if operation.Status() != model.OperationStarted {
		return model.Operation{}, ErrOperationTerminal
	}
	return operation, nil
}

func normalizeLocalAcceptanceTimes(spec LocalAcceptanceSpec,
	trustedNow time.Time,
) (time.Time, time.Time, error) {
	trustedNow = trustedNow.Round(0).UTC()
	acceptedAt := spec.Items[0].Publication.Event().AcceptedAt()
	if trustedNow.IsZero() || acceptedAt.IsZero() || trustedNow.Before(acceptedAt) {
		return time.Time{}, time.Time{}, errors.New("commit local acceptance: trusted commit time is invalid")
	}
	return trustedNow, acceptedAt, nil
}

func requireLocalAcceptanceFence(operation model.Operation,
	authority *LocalOperationAuthority, trustedNow time.Time,
) error {
	if authority == nil {
		return nil
	}
	return requireOperationFence(operation, authority.LeaseOwner, trustedNow)
}

func bindManagedAcceptanceAuthority(spec LocalAcceptanceSpec, authority managedAcceptanceState,
	operation model.Operation, events []model.Event,
) (LocalAcceptanceSpec, managedAcceptanceState, error) {
	if err := validateManagedAcceptanceEvents(authority, operation, events); err != nil {
		return LocalAcceptanceSpec{}, managedAcceptanceState{}, err
	}
	references, err := managedAuthorizedReferences(authority, events)
	if err != nil {
		return LocalAcceptanceSpec{}, managedAcceptanceState{}, err
	}
	authority.authorizedReferences = references
	spec.AuthorizedReferences = authority.authorizedReferences
	spec.Derivation = authority.derivation
	return spec, authority, nil
}
