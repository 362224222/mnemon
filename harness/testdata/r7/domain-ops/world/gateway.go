package world

import (
	"context"
	"net/http"
	"sync"
	"time"
)

type GatewayStatus struct {
	Route     string `json:"route"`
	Requests  int64  `json:"requests"`
	Succeeded int64  `json:"succeeded"`
	Failed    int64  `json:"failed"`
}

type Gateway struct {
	mu        sync.Mutex
	route     string
	eastURL   string
	westURL   string
	client    *http.Client
	requests  int64
	succeeded int64
	failed    int64
}

func NewGateway(route, eastURL, westURL string) *Gateway {
	return &Gateway{route: route, eastURL: eastURL, westURL: westURL,
		client: DefaultClient(4 * time.Second)}
}

func (gateway *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		WriteJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /checkout", gateway.checkout)
	mux.HandleFunc("GET /status", gateway.status)
	mux.HandleFunc("POST /admin/route", gateway.configureRoute)
	return mux
}

func (gateway *Gateway) checkout(writer http.ResponseWriter, request *http.Request) {
	var input PayRequest
	if err := DecodeJSON(request, &input); err != nil || !ValidToken(input.BusinessID) {
		WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid checkout"})
		return
	}
	gateway.mu.Lock()
	gateway.requests++
	route := gateway.route
	target := gateway.eastURL
	if route == "west" {
		target = gateway.westURL
	}
	gateway.mu.Unlock()
	var result PayResponse
	err := PostJSON(context.Background(), gateway.client, target+"/pay", input, &result)
	gateway.mu.Lock()
	if err == nil {
		gateway.succeeded++
	} else {
		gateway.failed++
	}
	gateway.mu.Unlock()
	if err != nil {
		WriteJSON(writer, http.StatusBadGateway, map[string]any{"accepted": false, "route": route})
		return
	}
	WriteJSON(writer, http.StatusOK, CheckoutResponse{Accepted: true, Route: route,
		CaptureID: result.CaptureID})
}

func (gateway *Gateway) status(writer http.ResponseWriter, _ *http.Request) {
	gateway.mu.Lock()
	status := gateway.statusLocked()
	gateway.mu.Unlock()
	WriteJSON(writer, http.StatusOK, status)
}

func (gateway *Gateway) configureRoute(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Route string `json:"route"`
	}
	if err := DecodeJSON(request, &input); err != nil || (input.Route != "east" && input.Route != "west") {
		WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid route"})
		return
	}
	gateway.mu.Lock()
	gateway.route = input.Route
	status := gateway.statusLocked()
	gateway.mu.Unlock()
	WriteJSON(writer, http.StatusOK, status)
}

func (gateway *Gateway) statusLocked() GatewayStatus {
	return GatewayStatus{Route: gateway.route, Requests: gateway.requests,
		Succeeded: gateway.succeeded, Failed: gateway.failed}
}
