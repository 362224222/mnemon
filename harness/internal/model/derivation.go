package model

import "time"

type WorkDerivationSpec struct {
	OperationID     OperationID
	ChildOrdinal    uint8
	ChildChannelID  ChannelID
	Child           WorkRef
	ParentChannelID ChannelID
	Parent          WorkRef
	ParentVersion   uint64
	ParentEventID   EventID
	CreatedAt       time.Time
}

type WorkDerivation struct {
	spec WorkDerivationSpec
}

func NewWorkDerivation(spec WorkDerivationSpec) (WorkDerivation, error) {
	if spec.OperationID.IsZero() || spec.ChildChannelID.IsZero() || spec.Child.IsZero() ||
		spec.ParentChannelID.IsZero() || spec.Parent.IsZero() || spec.ParentEventID.IsZero() {
		return WorkDerivation{}, invalid("work derivation", "operation, child, parent and Event identity are required")
	}
	if spec.ChildOrdinal >= MaxChildWorks {
		return WorkDerivation{}, limit("child ordinal", int(spec.ChildOrdinal), MaxChildWorks-1)
	}
	if err := validateSQLitePositive("parent version", spec.ParentVersion); err != nil {
		return WorkDerivation{}, err
	}
	if spec.Child == spec.Parent {
		return WorkDerivation{}, invariant("derived child Work must differ from its parent")
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return WorkDerivation{}, err
	}
	spec.CreatedAt = createdAt
	return WorkDerivation{spec: spec}, nil
}

func (d WorkDerivation) OperationID() OperationID   { return d.spec.OperationID }
func (d WorkDerivation) ChildOrdinal() uint8        { return d.spec.ChildOrdinal }
func (d WorkDerivation) ChildChannelID() ChannelID  { return d.spec.ChildChannelID }
func (d WorkDerivation) Child() WorkRef             { return d.spec.Child }
func (d WorkDerivation) ParentChannelID() ChannelID { return d.spec.ParentChannelID }
func (d WorkDerivation) Parent() WorkRef            { return d.spec.Parent }
func (d WorkDerivation) ParentVersion() uint64      { return d.spec.ParentVersion }
func (d WorkDerivation) ParentEventID() EventID     { return d.spec.ParentEventID }
func (d WorkDerivation) CreatedAt() time.Time       { return d.spec.CreatedAt }

func (d WorkDerivation) ValidateOperation(operation Operation) error {
	if operation.ID() != d.spec.OperationID || operation.Kind() != OperationTeamworkOffer ||
		operation.Status() != OperationCommitted {
		return invariant("derivation requires its committed teamwork.offer operation")
	}
	if _, ok := operation.ContextHash(); !ok {
		return invariant("derivation offer operation must be context-bound")
	}
	return nil
}

func (d WorkDerivation) ValidateParent(parent ReviewWork, source Event) error {
	if parent.Ref() != d.spec.Parent || parent.ChannelID() != d.spec.ParentChannelID ||
		parent.Version() != d.spec.ParentVersion || parent.UpdatedBy() != d.spec.ParentEventID {
		return invariant("derivation parent snapshot does not match frozen identity/version")
	}
	if parent.State() != WorkActive && parent.State() != WorkRework {
		return invariant("derivation parent must be ACTIVE or REWORK")
	}
	if source.ID() != d.spec.ParentEventID || source.Scope().ChannelID() != d.spec.ParentChannelID ||
		source.Scope().WorkRef() != d.spec.Parent {
		return invariant("derivation parent Event does not match frozen scope")
	}
	return parent.ValidateUpdateEvent(source)
}
