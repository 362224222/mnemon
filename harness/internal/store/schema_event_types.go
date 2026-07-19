package store

import (
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const eventTypeValuesPlaceholder = "{{MNEMON_EVENT_TYPE_VALUES}}"

func eventTypeSQLProjection() string {
	descriptors := model.EventTypeDescriptors()
	if len(descriptors) == 0 {
		panic("store: model has no EventType descriptors")
	}
	values := make([]string, len(descriptors))
	for index, descriptor := range descriptors {
		value := string(descriptor.Type())
		if value == "" {
			panic("store: model has an empty EventType descriptor")
		}
		values[index] = "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	return strings.Join(values, ",\n    ")
}
