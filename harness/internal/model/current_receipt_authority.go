package model

import "time"

func parseCurrentReceiptAuthority(wire currentReceiptWire) (EventID, time.Time, time.Time, error) {
	updatedBy, err := ParseEventID(wire.ActionWorkUpdatedBy)
	if err != nil {
		return EventID{}, time.Time{}, time.Time{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, wire.ActionWorkUpdatedAt)
	if err != nil {
		return EventID{}, time.Time{}, time.Time{},
			invalid("current-read receipt Work update", "must be RFC3339Nano")
	}
	readAt, err := time.Parse(time.RFC3339Nano, wire.ReadAt)
	if err != nil {
		return EventID{}, time.Time{}, time.Time{},
			invalid("current-read receipt read_at", "must be RFC3339Nano")
	}
	return updatedBy, updatedAt, readAt, nil
}
