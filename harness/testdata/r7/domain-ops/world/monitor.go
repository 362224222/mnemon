package world

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	MonitorProbeLimit  = 16
	monitorProbeSettle = 500 * time.Millisecond
)

type MonitorStatus struct {
	Gateway GatewayStatus `json:"gateway"`
	Ledger  LedgerStatus  `json:"ledger"`
}

// MonitorProbeResult is one bounded customer-like observation. The monitor,
// not the caller, chooses the identity and cardinality. Receipt is copied from
// the public gateway boundary; Ledger is an aggregate observation for that
// exact synthetic identity.
type MonitorProbeResult struct {
	Receipt GatewayReceipt `json:"receipt"`
	Ledger  LedgerStatus   `json:"ledger"`
}

type Monitor struct {
	gatewayURL string
	ledgerURL  string
	client     *http.Client
	probeMu    sync.Mutex
	probes     int
}

func NewMonitor(gatewayURL, ledgerURL string) *Monitor {
	return &Monitor{gatewayURL: gatewayURL, ledgerURL: ledgerURL,
		client: DefaultClient(2 * time.Second)}
}

func (monitor *Monitor) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		WriteJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /status", monitor.status)
	mux.HandleFunc("POST /probe", monitor.probe)
	return mux
}

func (monitor *Monitor) status(writer http.ResponseWriter, request *http.Request) {
	prefix := request.URL.Query().Get("prefix")
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	var gateway GatewayStatus
	if err := GetJSON(ctx, monitor.client, monitor.gatewayURL+"/status", &gateway); err != nil {
		WriteJSON(writer, http.StatusBadGateway, map[string]string{"error": "gateway unavailable"})
		return
	}
	var ledger LedgerStatus
	ledgerTarget := monitor.ledgerURL + "/status?prefix=" + url.QueryEscape(prefix)
	if err := GetJSON(ctx, monitor.client, ledgerTarget, &ledger); err != nil {
		WriteJSON(writer, http.StatusBadGateway, map[string]string{"error": "ledger unavailable"})
		return
	}
	WriteJSON(writer, http.StatusOK, MonitorStatus{Gateway: gateway, Ledger: ledger})
}

func (monitor *Monitor) probe(writer http.ResponseWriter, request *http.Request) {
	var input struct{}
	if err := DecodeJSON(request, &input); err != nil {
		WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid probe"})
		return
	}

	// Keep the full real-world effect serialized. A queued caller cannot choose
	// an identity, fan out, or race the global bound.
	monitor.probeMu.Lock()
	defer monitor.probeMu.Unlock()
	if monitor.probes >= MonitorProbeLimit {
		WriteJSON(writer, http.StatusTooManyRequests,
			map[string]string{"error": "probe limit reached"})
		return
	}
	monitor.probes++
	businessID := fmt.Sprintf("synthetic-%03d", monitor.probes)

	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	var checkout CheckoutResponse
	// A failed public response remains useful evidence. The exact gateway
	// receipt below, rather than this transport result, records what happened at
	// the public boundary.
	_ = PostJSON(ctx, monitor.client, monitor.gatewayURL+"/checkout",
		PayRequest{BusinessID: businessID}, &checkout)
	timer := time.NewTimer(monitorProbeSettle)
	select {
	case <-ctx.Done():
		timer.Stop()
		WriteJSON(writer, http.StatusGatewayTimeout,
			map[string]string{"error": "probe observation timed out"})
		return
	case <-timer.C:
	}

	var history GatewayHistory
	historyTarget := monitor.gatewayURL + "/history?prefix=" + url.QueryEscape(businessID)
	if err := GetJSON(ctx, monitor.client, historyTarget, &history); err != nil ||
		len(history.Entries) != 1 || history.Entries[0].BusinessID != businessID {
		WriteJSON(writer, http.StatusBadGateway,
			map[string]string{"error": "probe receipt unavailable"})
		return
	}
	var ledger LedgerStatus
	ledgerTarget := monitor.ledgerURL + "/status?prefix=" + url.QueryEscape(businessID)
	if err := GetJSON(ctx, monitor.client, ledgerTarget, &ledger); err != nil {
		WriteJSON(writer, http.StatusBadGateway,
			map[string]string{"error": "probe ledger observation unavailable"})
		return
	}
	WriteJSON(writer, http.StatusOK,
		MonitorProbeResult{Receipt: history.Entries[0], Ledger: ledger})
}
