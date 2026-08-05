package world

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMonitorProbeRunsOneServerNamedCheckout(t *testing.T) {
	ledger := httptest.NewServer(NewLedger().Handler())
	t.Cleanup(ledger.Close)
	callback := httptest.NewServer(NewCallback(0, ledger.URL).Handler())
	t.Cleanup(callback.Close)
	payment := httptest.NewServer(NewPayment(PaymentConfig{
		TimeoutMillis: 500,
		StableKeys:    true,
		Retries:       1,
	}, callback.URL).Handler())
	t.Cleanup(payment.Close)
	gateway := httptest.NewServer(NewGateway("east", payment.URL, payment.URL).Handler())
	t.Cleanup(gateway.Close)
	monitor := httptest.NewServer(NewMonitor(gateway.URL, ledger.URL).Handler())
	t.Cleanup(monitor.Close)

	for index, wantID := range []string{"synthetic-001", "synthetic-002"} {
		var result MonitorProbeResult
		if err := PostJSON(context.Background(), DefaultClient(3*time.Second),
			monitor.URL+"/probe", struct{}{}, &result); err != nil {
			t.Fatalf("probe %d: %v", index+1, err)
		}
		if result.Receipt.BusinessID != wantID ||
			result.Receipt.Status != GatewayReceiptSucceeded ||
			result.Receipt.CaptureID <= 0 ||
			result.Ledger != (LedgerStatus{Charges: 1, ActiveCharges: 1,
				UniqueBusinesses: 1}) {
			t.Fatalf("probe %d result = %+v", index+1, result)
		}
	}
}

func TestMonitorProbeRejectsCallerParametersAndExhaustedBudget(t *testing.T) {
	monitor := NewMonitor("http://gateway.invalid", "http://ledger.invalid")
	server := httptest.NewServer(monitor.Handler())
	t.Cleanup(server.Close)

	response, err := http.Post(server.URL+"/probe", "application/json",
		bytes.NewBufferString(`{"count":2}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("parameterized probe status = %d", response.StatusCode)
	}

	monitor.probeMu.Lock()
	monitor.probes = MonitorProbeLimit
	monitor.probeMu.Unlock()
	response, err = http.Post(server.URL+"/probe", "application/json",
		bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("exhausted probe status = %d", response.StatusCode)
	}
}
