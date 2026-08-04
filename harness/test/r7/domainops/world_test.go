package domainops_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	world "github.com/mnemon-dev/mnemon/harness/testdata/r7/domain-ops/world"
)

const (
	requestTimeout = 2 * time.Second
	pollTimeout    = 3 * time.Second
)

type serviceWorld struct {
	gatewayURL     string
	monitorURL     string
	ledgerURL      string
	eastPaymentURL string
}

func TestDefaultEastFaultReturnsSuccessAndDuplicatesCharge(t *testing.T) {
	services := newServiceWorld(t)
	prefix := "default-fault"

	result, err := checkout(services.gatewayURL, prefix+"-order-1")
	if err != nil {
		t.Fatalf("default East checkout failed: %v", err)
	}
	if result.CaptureID <= 0 {
		t.Fatalf("capture ID = %d, want a durable customer receipt", result.CaptureID)
	}

	status := waitForMonitor(t, services.monitorURL, prefix, func(status world.MonitorStatus) bool {
		return status.Gateway.Succeeded == 1 && status.Ledger.DuplicateBusinesses == 1
	})
	if status.Gateway.Route != "east" {
		t.Fatalf("gateway route = %q, want east", status.Gateway.Route)
	}
	if status.Gateway.Failed != 0 {
		t.Fatalf("gateway failures = %d, want 0", status.Gateway.Failed)
	}
	if status.Ledger.ActiveCharges != 2 || status.Ledger.UniqueBusinesses != 1 {
		t.Fatalf("ledger status = %+v, want two charges for one business", status.Ledger)
	}
}

func TestDistinctRemediationsSatisfySameOutcomeOracle(t *testing.T) {
	remediations := []struct {
		name  string
		apply func(*testing.T, serviceWorld)
	}{
		{
			name: "route_new_traffic_to_healthy_region",
			apply: func(t *testing.T, services serviceWorld) {
				postAccepted(t, services.gatewayURL+"/admin/route", map[string]string{
					"route": "west",
				})
			},
		},
		{
			name: "stabilize_payment_retry_configuration",
			apply: func(t *testing.T, services serviceWorld) {
				postAccepted(t, services.eastPaymentURL+"/admin/config", world.PaymentConfig{
					TimeoutMillis: 500,
					StableKeys:    true,
					Retries:       2,
				})
			},
		},
	}

	for _, remediation := range remediations {
		remediation := remediation
		t.Run(remediation.name, func(t *testing.T) {
			services := newServiceWorld(t)
			prefix := "incident-" + remediation.name

			customerCapture := induceIncident(t, services, prefix)
			remediation.apply(t, services)
			voidDuplicateCharges(t, services.ledgerURL, prefix, customerCapture)

			assertRecoveredOutcome(t, services, prefix, 3)
		})
	}
}

func newServiceWorld(t *testing.T) serviceWorld {
	t.Helper()

	ledger := httptest.NewServer(world.NewLedger().Handler())
	t.Cleanup(ledger.Close)

	eastCallback := httptest.NewServer(world.NewCallback(150*time.Millisecond, ledger.URL).Handler())
	t.Cleanup(eastCallback.Close)
	westCallback := httptest.NewServer(world.NewCallback(5*time.Millisecond, ledger.URL).Handler())
	t.Cleanup(westCallback.Close)

	eastPayment := httptest.NewServer(world.NewPayment(world.PaymentConfig{
		TimeoutMillis: 50,
		StableKeys:    false,
		Retries:       2,
	}, eastCallback.URL).Handler())
	t.Cleanup(eastPayment.Close)
	westPayment := httptest.NewServer(world.NewPayment(world.PaymentConfig{
		TimeoutMillis: 500,
		StableKeys:    true,
		Retries:       2,
	}, westCallback.URL).Handler())
	t.Cleanup(westPayment.Close)

	gateway := httptest.NewServer(world.NewGateway("east", eastPayment.URL, westPayment.URL).Handler())
	t.Cleanup(gateway.Close)
	monitor := httptest.NewServer(world.NewMonitor(gateway.URL, ledger.URL).Handler())
	t.Cleanup(monitor.Close)

	return serviceWorld{
		gatewayURL:     gateway.URL,
		monitorURL:     monitor.URL,
		ledgerURL:      ledger.URL,
		eastPaymentURL: eastPayment.URL,
	}
}

func induceIncident(t *testing.T, services serviceWorld, prefix string) int64 {
	t.Helper()
	result, err := checkout(services.gatewayURL, prefix+"-original")
	if err != nil {
		t.Fatalf("incident checkout failed before remediation: %v", err)
	}
	waitForMonitor(t, services.monitorURL, prefix, func(status world.MonitorStatus) bool {
		return status.Gateway.Succeeded == 1 && status.Ledger.DuplicateBusinesses == 1
	})
	return result.CaptureID
}

func voidDuplicateCharges(t *testing.T, ledgerURL, prefix string, preserve int64) {
	t.Helper()
	charges := getCharges(t, ledgerURL, prefix)
	voided := 0
	for _, charge := range charges {
		if charge.State == world.ChargeActive && charge.Sequence != preserve {
			postAccepted(t, ledgerURL+"/admin/void", world.VoidRequest{
				Sequence: charge.Sequence,
				Reason:   "duplicate capture confirmed by incident reconciliation",
			})
			voided++
		}
	}
	if voided != 1 {
		t.Fatalf("voided charges = %d, want one explicit duplicate", voided)
	}
	status := getLedgerStatus(t, ledgerURL, prefix)
	if status.DuplicateBusinesses != 0 || status.VoidedCharges != 1 {
		t.Fatalf("ledger status after reconciliation = %+v, want one retained void", status)
	}
}

// assertRecoveredOutcome is intentionally shared by every remediation. It
// observes user traffic and durable ledger state, not which component changed.
func assertRecoveredOutcome(t *testing.T, services serviceWorld, prefix string, evaluations int) {
	t.Helper()
	for index := 1; index <= evaluations; index++ {
		businessID := fmt.Sprintf("%s-evaluation-%d", prefix, index)
		if _, err := checkout(services.gatewayURL, businessID); err != nil {
			t.Fatalf("evaluation checkout %q failed: %v", businessID, err)
		}
	}

	status := waitForMonitor(t, services.monitorURL, prefix, func(status world.MonitorStatus) bool {
		return status.Gateway.Succeeded == int64(evaluations+1) &&
			status.Ledger.DuplicateBusinesses == 0 &&
			status.Ledger.UniqueBusinesses == evaluations+1 &&
			status.Ledger.ActiveCharges == evaluations+1 &&
			status.Ledger.VoidedCharges == 1
	})
	if status.Gateway.Failed != 0 {
		t.Fatalf("gateway failures = %d, want all customer requests to return", status.Gateway.Failed)
	}
	if status.Ledger.Charges != evaluations+2 {
		t.Fatalf("ledger charges = %d, want audit history retained", status.Ledger.Charges)
	}
}

func checkout(gatewayURL, businessID string) (world.CheckoutResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var result world.CheckoutResponse
	err := world.PostJSON(ctx, world.DefaultClient(requestTimeout), gatewayURL+"/checkout",
		world.PayRequest{BusinessID: businessID}, &result)
	return result, err
}

func postAccepted(t *testing.T, target string, input any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var result map[string]any
	if err := world.PostJSON(ctx, world.DefaultClient(requestTimeout), target, input, &result); err != nil {
		t.Fatalf("POST %s failed: %v", target, err)
	}
}

func getLedgerStatus(t *testing.T, ledgerURL, prefix string) world.LedgerStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var status world.LedgerStatus
	target := ledgerURL + "/status?prefix=" + url.QueryEscape(prefix)
	if err := world.GetJSON(ctx, world.DefaultClient(requestTimeout), target, &status); err != nil {
		t.Fatalf("GET ledger status failed: %v", err)
	}
	return status
}

func getCharges(t *testing.T, ledgerURL, prefix string) []world.Charge {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var charges []world.Charge
	target := ledgerURL + "/charges?prefix=" + url.QueryEscape(prefix)
	if err := world.GetJSON(ctx, world.DefaultClient(requestTimeout), target, &charges); err != nil {
		t.Fatalf("GET ledger charges failed: %v", err)
	}
	return charges
}

func waitForMonitor(
	t *testing.T,
	monitorURL string,
	prefix string,
	ready func(world.MonitorStatus) bool,
) world.MonitorStatus {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var last world.MonitorStatus
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		target := monitorURL + "/status?prefix=" + url.QueryEscape(prefix)
		lastErr = world.GetJSON(ctx, world.DefaultClient(250*time.Millisecond), target, &last)
		cancel()
		if lastErr == nil && ready(last) {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("monitor condition was not met: last=%+v error=%v", last, lastErr)
	return world.MonitorStatus{}
}
