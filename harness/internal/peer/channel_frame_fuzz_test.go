package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func FuzzReadChannelFrame(f *testing.F) {
	requestID, err := ParseChannelRequestID(
		"channel-request-303132333435363738393a3b3c3d3e3f")
	if err != nil {
		f.Fatal(err)
	}
	proof, err := NewEnrollProof(model.Sum([]byte("channel frame fuzz proof")))
	if err != nil {
		f.Fatal(err)
	}
	frame, err := NewChannelFrame(requestID, proof)
	if err != nil {
		f.Fatal(err)
	}
	var valid bytes.Buffer
	if err := WriteChannelFrame(&valid, frame); err != nil {
		f.Fatal(err)
	}
	validBytes := valid.Bytes()

	var zeroPrefix [channelFrameLengthBytes]byte
	var oversizedPrefix [channelFrameLengthBytes]byte
	binary.BigEndian.PutUint32(oversizedPrefix[:], uint32(maxChannelFrameBytes()+1))
	declaredTooLong := append([]byte(nil), validBytes...)
	binary.BigEndian.PutUint32(declaredTooLong[:channelFrameLengthBytes],
		uint32(len(validBytes)-channelFrameLengthBytes+1))
	noncanonical := append([]byte(nil), validBytes...)
	noncanonicalBody := append([]byte{' '}, noncanonical[channelFrameLengthBytes:]...)
	binary.BigEndian.PutUint32(noncanonical[:channelFrameLengthBytes],
		uint32(len(noncanonicalBody)))
	noncanonical = append(noncanonical[:channelFrameLengthBytes], noncanonicalBody...)

	for _, raw := range [][]byte{
		nil,
		{},
		{0, 0},
		zeroPrefix[:],
		oversizedPrefix[:],
		declaredTooLong,
		noncanonical,
		append(append([]byte(nil), validBytes...), 0xff),
	} {
		f.Add(raw)
	}
	for _, cut := range []int{1, channelFrameLengthBytes - 1,
		channelFrameLengthBytes, len(validBytes) - 1} {
		f.Add(append([]byte(nil), validBytes[:cut]...))
	}
	f.Add(append([]byte(nil), validBytes...))

	f.Fuzz(func(t *testing.T, raw []byte) {
		reader := bytes.NewReader(raw)
		frame, err := ReadChannelFrame(reader)
		if err != nil {
			if !errors.Is(err, ErrChannelFrame) {
				t.Fatalf("ReadChannelFrame error = %v, want ErrChannelFrame", err)
			}
			return
		}
		if frame.IsZero() {
			t.Fatal("ReadChannelFrame returned a zero frame")
		}

		consumed := len(raw) - reader.Len()
		if consumed <= channelFrameLengthBytes || consumed > len(raw) {
			t.Fatalf("ReadChannelFrame consumed %d of %d bytes", consumed, len(raw))
		}
		declared := int(binary.BigEndian.Uint32(raw[:channelFrameLengthBytes]))
		if declared != consumed-channelFrameLengthBytes {
			t.Fatalf("declared frame length %d, consumed %d", declared,
				consumed-channelFrameLengthBytes)
		}
		if !bytes.Equal(frame.CanonicalJSON().Bytes(),
			raw[channelFrameLengthBytes:consumed]) {
			t.Fatal("parsed frame did not retain the exact canonical envelope")
		}

		var encoded bytes.Buffer
		if err := WriteChannelFrame(&encoded, frame); err != nil {
			t.Fatalf("WriteChannelFrame(parsed): %v", err)
		}
		if !bytes.Equal(encoded.Bytes(), raw[:consumed]) {
			t.Fatal("parsed frame did not round-trip through WriteChannelFrame")
		}

		roundTripReader := bytes.NewReader(encoded.Bytes())
		roundTrip, err := ReadChannelFrame(roundTripReader)
		if err != nil {
			t.Fatalf("read re-encoded frame: %v", err)
		}
		if roundTripReader.Len() != 0 ||
			roundTrip.Type() != frame.Type() ||
			roundTrip.RequestID() != frame.RequestID() ||
			!bytes.Equal(roundTrip.CanonicalJSON().Bytes(), frame.CanonicalJSON().Bytes()) ||
			!bytes.Equal(roundTrip.Payload().CanonicalJSON().Bytes(),
				frame.Payload().CanonicalJSON().Bytes()) {
			t.Fatal("channel frame round-trip changed typed canonical identity")
		}
	})
}
