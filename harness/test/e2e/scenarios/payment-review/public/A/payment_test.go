package payment

import (
	"errors"
	"testing"
)

func TestSequentialRetryReusesCharge(t *testing.T) {
	processor := NewProcessor()
	first, err := processor.Charge("checkout-42", 1250)
	if err != nil {
		t.Fatal(err)
	}
	second, err := processor.Charge("checkout-42", 1250)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || processor.Count() != 1 {
		t.Fatalf("retry produced a second charge: first=%+v second=%+v count=%d", first, second, processor.Count())
	}
}

func TestRejectsKeyReuseWithDifferentAmount(t *testing.T) {
	processor := NewProcessor()
	if _, err := processor.Charge("checkout-42", 1250); err != nil {
		t.Fatal(err)
	}
	if _, err := processor.Charge("checkout-42", 1300); !errors.Is(err, ErrInvalidCharge) {
		t.Fatalf("got %v, want ErrInvalidCharge", err)
	}
}
