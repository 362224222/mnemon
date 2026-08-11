package agencyclient

import (
	"bytes"
	"testing"
)

func TestCurrentOperationEncodingHasCanonicalTokenTail(t *testing.T) {
	// Exhaust the byte that determines the final raw-base64url character.
	// With the former 24-byte input, values such as 0xff ended in '_' and
	// made roughly one in 32 randomly generated operations invalid.
	entropy := bytes.Repeat([]byte{0xff}, currentOperationEntropy)
	for tail := 0; tail < 256; tail++ {
		entropy[len(entropy)-1] = byte(tail)
		if _, err := newCurrentOperation(bytes.NewReader(entropy)); err != nil {
			t.Fatalf("newCurrentOperation(tail=%#02x) error = %v", tail, err)
		}
	}
}
