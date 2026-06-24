package state

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

var (
	errSchema = errors.New("schema")
	errAuthz  = errors.New("authz")
)

type conflictError struct{ conflicts []contract.Conflict }

func (e *conflictError) Error() string { return "conflict" }
