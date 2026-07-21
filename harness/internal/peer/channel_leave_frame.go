package peer

import (
	"bytes"
	"encoding/json"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func canonicalMemberControlPayload(payload ChannelFramePayload) (
	ChannelFrameType, model.JSON, bool, error,
) {
	var frameType ChannelFrameType
	var zero bool
	switch value := payload.(type) {
	case DataBaseline:
		frameType, zero = ChannelFrameDataBaseline, value.IsZero()
	case DataBaselineAck:
		frameType, zero = ChannelFrameDataBaselineAck, value.IsZero()
	case LeaveRequest:
		frameType, zero = ChannelFrameLeaveRequest, value.IsZero()
	case LeaveReceipt:
		frameType, zero = ChannelFrameLeaveReceipt, value.IsZero()
	default:
		return "", model.JSON{}, false, nil
	}
	if zero || payload.channelFrameType() != frameType || payload.CanonicalJSON().IsZero() {
		return "", model.JSON{}, true, channelFrameError("zero or inconsistent member control payload", nil)
	}
	return frameType, payload.CanonicalJSON(), true, nil
}

func maxChannelFrameBytes() int {
	if HermeticLimits().DirectFrameBytes < model.MaxCanonicalJSONBytes {
		return HermeticLimits().DirectFrameBytes
	}
	return model.MaxCanonicalJSONBytes
}

// LeaveRequest carries the member-signed durable request unchanged. Secure
// transport identity remains separate evidence and is checked by the service.
type LeaveRequest struct {
	request   model.SignedChannelLeaveRequest
	canonical model.JSON
}

type leaveRequestWire struct {
	Request json.RawMessage `json:"request"`
}

func NewLeaveRequest(request model.SignedChannelLeaveRequest) (LeaveRequest, error) {
	if request.IsZero() || len(request.WireJSON().Bytes()) > model.MaxChannelRecordBytes {
		return LeaveRequest{}, channelFrameError("LeaveRequest requires bounded signed member evidence", nil)
	}
	canonical, err := model.JSONFrom(leaveRequestWire{Request: request.WireJSON().Bytes()})
	if err != nil {
		return LeaveRequest{}, channelFrameError("encode LeaveRequest", err)
	}
	return LeaveRequest{request: request, canonical: canonical}, nil
}

func parseLeaveRequest(raw []byte) (LeaveRequest, error) {
	var wire leaveRequestWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return LeaveRequest{}, err
	}
	request, err := model.ParseSignedChannelLeaveRequest(wire.Request)
	if err != nil {
		return LeaveRequest{}, channelFrameError("invalid LeaveRequest member evidence", err)
	}
	payload, err := NewLeaveRequest(request)
	if err != nil {
		return LeaveRequest{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return LeaveRequest{}, channelFrameError("LeaveRequest bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload LeaveRequest) SignedRequest() model.SignedChannelLeaveRequest { return payload.request }
func (payload LeaveRequest) CanonicalJSON() model.JSON                      { return payload.canonical }
func (payload LeaveRequest) IsZero() bool {
	return payload.request.IsZero() || payload.canonical.IsZero()
}
func (LeaveRequest) channelFrameType() ChannelFrameType { return ChannelFrameLeaveRequest }

// LeaveReceipt is the owner-signed acknowledgement and complete bounded roster
// suffix needed to settle the request without a second source of authority.
type LeaveReceipt struct {
	receipt   model.SignedChannelLeaveReceipt
	canonical model.JSON
}

type leaveReceiptWire struct {
	Receipt json.RawMessage `json:"receipt"`
}

func NewLeaveReceipt(receipt model.SignedChannelLeaveReceipt) (LeaveReceipt, error) {
	if receipt.IsZero() || len(receipt.WireJSON().Bytes()) > model.MaxCanonicalJSONBytes {
		return LeaveReceipt{}, channelFrameError("LeaveReceipt requires bounded signed owner evidence", nil)
	}
	canonical, err := model.JSONFrom(leaveReceiptWire{Receipt: receipt.WireJSON().Bytes()})
	if err != nil {
		return LeaveReceipt{}, channelFrameError("encode LeaveReceipt", err)
	}
	return LeaveReceipt{receipt: receipt, canonical: canonical}, nil
}

func parseLeaveReceipt(raw []byte) (LeaveReceipt, error) {
	var wire leaveReceiptWire
	if err := decodeExactFrameJSON(raw, &wire); err != nil {
		return LeaveReceipt{}, err
	}
	receipt, err := model.ParseSignedChannelLeaveReceipt(wire.Receipt)
	if err != nil {
		return LeaveReceipt{}, channelFrameError("invalid LeaveReceipt owner evidence", err)
	}
	payload, err := NewLeaveReceipt(receipt)
	if err != nil {
		return LeaveReceipt{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return LeaveReceipt{}, channelFrameError("LeaveReceipt bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload LeaveReceipt) SignedReceipt() model.SignedChannelLeaveReceipt { return payload.receipt }
func (payload LeaveReceipt) CanonicalJSON() model.JSON                      { return payload.canonical }
func (payload LeaveReceipt) IsZero() bool {
	return payload.receipt.IsZero() || payload.canonical.IsZero()
}
func (LeaveReceipt) channelFrameType() ChannelFrameType { return ChannelFrameLeaveReceipt }
