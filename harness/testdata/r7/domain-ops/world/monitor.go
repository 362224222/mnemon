package world

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

type MonitorStatus struct {
	Gateway GatewayStatus `json:"gateway"`
	Ledger  LedgerStatus  `json:"ledger"`
}

type Monitor struct {
	gatewayURL string
	ledgerURL  string
	client     *http.Client
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
