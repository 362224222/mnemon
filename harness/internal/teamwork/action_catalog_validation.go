package teamwork

import (
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func projectAction(source ActionSource, wire actionWire, operation model.OperationKind) (ActionDescriptor, error) {
	if wire.SchemaVersion != ActionSchemaVersion || wire.Content.MaxBytes == 0 ||
		wire.Content.MaxBytes > model.MaxContentBytes || wire.Content.Source != ContentFileOrStdin {
		return ActionDescriptor{}, errors.New("schema or content policy is invalid")
	}
	contexts, contextLen, err := projectContexts(wire.AllowedContext)
	if err != nil {
		return ActionDescriptor{}, err
	}
	artifacts, err := projectArtifacts(wire.Artifacts)
	if err != nil {
		return ActionDescriptor{}, err
	}
	receipt, err := projectReceipt(wire.Receipt, operation)
	if err != nil {
		return ActionDescriptor{}, err
	}
	deadline, hasDeadline, selectors, hasSelectors, err := projectSelection(wire.Deadline, wire.Selectors)
	if err != nil {
		return ActionDescriptor{}, err
	}
	return ActionDescriptor{name: wire.Action, path: source.path, schemaVersion: wire.SchemaVersion,
		ordinal: wire.Ordinal, operation: operation,
		raw: append([]byte(nil), source.raw...), contexts: contexts, contextLen: contextLen,
		content:   ActionContentPolicy{maxBytes: wire.Content.MaxBytes, required: wire.Content.Required, source: wire.Content.Source},
		artifacts: artifacts, deadline: deadline, hasDeadline: hasDeadline, selectors: selectors, hasSelectors: hasSelectors,
		receipt: receipt}, nil
}

func projectContexts(values []string) ([8]ActionContext, uint8, error) {
	if len(values) == 0 || len(values) > 8 {
		return [8]ActionContext{}, 0, errors.New("allowed_context count is invalid")
	}
	var result [8]ActionContext
	for index, value := range values {
		context := ActionContext(value)
		if !knownActionContext(context) {
			return [8]ActionContext{}, 0, errors.New("allowed_context contains an unknown value")
		}
		for prior := 0; prior < index; prior++ {
			if result[prior] == context {
				return [8]ActionContext{}, 0, errors.New("allowed_context contains a duplicate")
			}
		}
		result[index] = context
	}
	return result, uint8(len(values)), nil
}

func knownActionContext(context ActionContext) bool {
	switch context {
	case ActionContextNone, ActionContextReviewerOffered, ActionContextReviewerActive, ActionContextReviewerRework,
		ActionContextParentResume, ActionContextHomeDelivered, ActionContextHomeDeliveredIteration1, ActionContextHomeNonterminal:
		return true
	default:
		return false
	}
}

func projectArtifacts(wire actionArtifactWire) (ActionArtifactPolicy, error) {
	if !wire.Allowed {
		if wire != (actionArtifactWire{}) {
			return ActionArtifactPolicy{}, errors.New("forbidden Artifact policy has nonzero bounds")
		}
	} else if wire.MaxEntries == 0 || wire.MaxEntries > 4096 || wire.MaxPathBytes == 0 ||
		wire.MaxPathBytes > model.MaxIdentifierBytes || wire.MaxRoots == 0 || wire.MaxRoots > model.MaxArtifactRefs ||
		wire.MaxTotalBytes == 0 || wire.MaxTotalBytes > 256<<20 {
		return ActionArtifactPolicy{}, errors.New("Artifact policy exceeds closed global bounds")
	}
	return ActionArtifactPolicy{allowed: wire.Allowed, maxEntries: wire.MaxEntries, maxPathBytes: wire.MaxPathBytes,
		maxRoots: wire.MaxRoots, maxTotalBytes: wire.MaxTotalBytes}, nil
}

func projectReceipt(wire actionReceiptWire, operation model.OperationKind) (ActionReceiptPolicy, error) {
	if wire.Action != string(operation) || wire.Status != ReceiptStatusAccepted || wire.MaxResults == 0 ||
		wire.MaxResults > model.MaxChildWorks ||
		(wire.Handling != ReceiptHandlingCompleted && wire.Handling != ReceiptHandlingContextDependent) {
		return ActionReceiptPolicy{}, errors.New("receipt is not bound to the closed action or result shape")
	}
	return ActionReceiptPolicy{action: operation, handling: wire.Handling,
		maxResults: wire.MaxResults, status: wire.Status}, nil
}

func projectSelection(deadlineWire *actionDeadlineWire,
	selectorWire *actionSelectorWire,
) (ActionDeadlinePolicy, bool, ActionSelectorPolicy, bool, error) {
	var deadline ActionDeadlinePolicy
	if deadlineWire != nil {
		parsed, err := parseDeadline(*deadlineWire)
		if err != nil {
			return ActionDeadlinePolicy{}, false, ActionSelectorPolicy{}, false, err
		}
		deadline = parsed
	}
	var selectors ActionSelectorPolicy
	if selectorWire != nil {
		parsed, err := parseSelectors(*selectorWire)
		if err != nil {
			return ActionDeadlinePolicy{}, false, ActionSelectorPolicy{}, false, err
		}
		selectors = parsed
	}
	return deadline, deadlineWire != nil, selectors, selectorWire != nil, nil
}

func parseDeadline(wire actionDeadlineWire) (ActionDeadlinePolicy, error) {
	minimum, minErr := time.ParseDuration(wire.Minimum)
	defaultDuration, defaultErr := time.ParseDuration(wire.Default)
	maximum, maxErr := time.ParseDuration(wire.Maximum)
	if minErr != nil || defaultErr != nil || maxErr != nil || minimum < MinimumOfferDeadline ||
		maximum > MaximumOfferDeadline || defaultDuration < minimum || defaultDuration > maximum {
		return ActionDeadlinePolicy{}, errors.New("deadline is malformed or exceeds global bounds")
	}
	return ActionDeadlinePolicy{defaultDuration: defaultDuration,
		minimumDuration: minimum, maximumDuration: maximum}, nil
}

func parseSelectors(wire actionSelectorWire) (ActionSelectorPolicy, error) {
	if wire.Channel != SelectorOptionalWhenUnambiguous || len(wire.Participant) == 0 || len(wire.Participant) > 3 {
		return ActionSelectorPolicy{}, errors.New("selector shape is invalid")
	}
	result := ActionSelectorPolicy{channel: wire.Channel, count: uint8(len(wire.Participant))}
	for index, value := range wire.Participant {
		selector := ParticipantSelector(value)
		if selector != ParticipantEffectiveAlias && selector != ParticipantAuto && selector != ParticipantTeam {
			return ActionSelectorPolicy{}, errors.New("participant selector is unknown")
		}
		for prior := 0; prior < index; prior++ {
			if result.participants[prior] == selector {
				return ActionSelectorPolicy{}, errors.New("participant selector is duplicated")
			}
		}
		result.participants[index] = selector
	}
	return result, nil
}
