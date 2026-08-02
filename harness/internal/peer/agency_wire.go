package peer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const agencyFrameLengthBytes = 4

func canonicalAgencyJSON(value any, maximum int) (model.JSON, error) {
	if maximum <= 0 {
		return model.JSON{}, agencyFrameError("invalid canonical JSON bound", nil)
	}
	canonical, err := model.JSONFrom(value)
	if err != nil {
		return model.JSON{}, agencyFrameError("encode canonical JSON", err)
	}
	if len(canonical.Bytes()) > maximum {
		return model.JSON{}, agencyFrameError("canonical JSON exceeds its wire bound", nil)
	}
	return canonical, nil
}

func decodeExactAgencyJSON(raw []byte, maximum int, destination any) error {
	if len(raw) == 0 || len(raw) > maximum || destination == nil {
		return agencyFrameError("empty, oversized, or unbound canonical JSON", nil)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return agencyFrameError("wire value must be exact canonical JSON", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return agencyFrameError("decode exact wire value", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return agencyFrameError("wire value contains a trailing value", err)
	}
	return nil
}

func validateAgencyDeliveryID(value string) bool {
	return validateAgencyHexValue(value, "delivery:")
}

func validateAgencyDigest(value string) bool {
	return validateAgencyHexValue(value, "sha256:")
}

func validateAgencyHexValue(value, prefix string) bool {
	if len(value) != len(prefix)+64 || value[:len(prefix)] != prefix {
		return false
	}
	for _, character := range value[len(prefix):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func readAgencyJSON(reader io.Reader, scope network.ResourceScope,
	maximum int,
) ([]byte, func(), error) {
	if reader == nil || maximum <= 0 {
		return nil, nil, agencyFrameError("reader and positive frame bound are required", nil)
	}
	var prefix [agencyFrameLengthBytes]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return nil, nil, agencyFrameError("read frame length", err)
	}
	length := uint64(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 || length > uint64(maximum) {
		return nil, nil, agencyFrameError("declared frame length exceeds its wire bound", nil)
	}
	reserved := int(length)
	var release func()
	if scope != nil {
		if err := scope.ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
			return nil, nil, agencyFrameError("reserve frame memory", err)
		}
		var once sync.Once
		release = func() { once.Do(func() { scope.ReleaseMemory(reserved) }) }
	}
	raw := make([]byte, reserved)
	if _, err := io.ReadFull(reader, raw); err != nil {
		if release != nil {
			release()
		}
		return nil, nil, agencyFrameError("read declared frame", err)
	}
	return raw, release, nil
}

func writeAgencyJSON(writer io.Writer, canonical model.JSON, maximum int) error {
	if writer == nil || canonical.IsZero() || maximum <= 0 {
		return agencyFrameError("writer and complete frame are required", nil)
	}
	raw := canonical.Bytes()
	if len(raw) == 0 || len(raw) > maximum || len(raw) > int(^uint32(0)) {
		return agencyFrameError("canonical frame exceeds its wire bound", nil)
	}
	var prefix [agencyFrameLengthBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if err := writeFullAgencyBytes(writer, prefix[:]); err != nil {
		return agencyFrameError("write frame length", err)
	}
	if err := writeFullAgencyBytes(writer, raw); err != nil {
		return agencyFrameError("write canonical frame", err)
	}
	return nil
}

func writeFullAgencyBytes(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if written < 0 || written > len(value) {
			return io.ErrShortWrite
		}
		value = value[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func reserveAgencyWrite(stream network.Stream, bytes int) (func(), error) {
	if stream == nil || stream.Scope() == nil || bytes <= 0 ||
		bytes > HermeticLimits().DirectFrameBytes {
		return nil, agencyFrameError("live stream and bounded write are required", nil)
	}
	if err := stream.Scope().ReserveMemory(bytes, network.ReservationPriorityAlways); err != nil {
		return nil, agencyFrameError("reserve write memory", err)
	}
	var once sync.Once
	return func() { once.Do(func() { stream.Scope().ReleaseMemory(bytes) }) }, nil
}

func agencyFrameError(detail string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrAgencyFrame, detail)
	}
	return fmt.Errorf("%w: %s: %w", ErrAgencyFrame, detail, cause)
}
