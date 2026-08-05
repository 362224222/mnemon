package domainops_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/testdata/r7/domain-ops/world"
)

func TestMonitorProbeRunsOneServerNamedCheckout(t *testing.T) {
	ledger := httptest.NewServer(world.NewLedger().Handler())
	t.Cleanup(ledger.Close)
	callback := httptest.NewServer(world.NewCallback(0, ledger.URL).Handler())
	t.Cleanup(callback.Close)
	payment := httptest.NewServer(world.NewPayment(world.PaymentConfig{
		TimeoutMillis: 500,
		StableKeys:    true,
		Retries:       1,
	}, callback.URL).Handler())
	t.Cleanup(payment.Close)
	gateway := httptest.NewServer(world.NewGateway("east", payment.URL, payment.URL).Handler())
	t.Cleanup(gateway.Close)
	monitor := httptest.NewServer(world.NewMonitor(gateway.URL, ledger.URL).Handler())
	t.Cleanup(monitor.Close)

	for index := 1; index <= world.MonitorProbeLimit; index++ {
		var result world.MonitorProbeResult
		if err := world.PostJSON(context.Background(), world.DefaultClient(3*time.Second),
			monitor.URL+"/probe", struct{}{}, &result); err != nil {
			t.Fatalf("probe %d: %v", index, err)
		}
		wantID := fmt.Sprintf("synthetic-%03d", index)
		if result.Receipt.BusinessID != wantID ||
			result.Receipt.Status != world.GatewayReceiptSucceeded ||
			result.Receipt.CaptureID <= 0 ||
			result.Ledger != (world.LedgerStatus{Charges: 1, ActiveCharges: 1,
				UniqueBusinesses: 1}) {
			t.Fatalf("probe %d result = %+v", index, result)
		}
	}
	var exhausted world.MonitorProbeResult
	if err := world.PostJSON(context.Background(), world.DefaultClient(time.Second),
		monitor.URL+"/probe", struct{}{}, &exhausted); err == nil {
		t.Fatal("monitor accepted a probe beyond its global bound")
	}
}

func TestMonitorProbeRejectsCallerParameters(t *testing.T) {
	monitor := httptest.NewServer(world.NewMonitor(
		"http://gateway.invalid", "http://ledger.invalid").Handler())
	t.Cleanup(monitor.Close)

	response, err := http.Post(monitor.URL+"/probe", "application/json",
		bytes.NewBufferString(`{"count":2}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("parameterized probe status = %d", response.StatusCode)
	}
}
