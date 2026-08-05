// Package world implements the test-only service world for the R7 federated
// domain-operations case. It contains no Agent or mnemond policy.
package world

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"
)

const maxBodyBytes = 16 << 10

var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$`)

type ChargeRequest struct {
	BusinessID string `json:"business_id"`
	AttemptKey string `json:"attempt_key"`
}

type ChargeResponse struct {
	Accepted bool  `json:"accepted"`
	Replayed bool  `json:"replayed"`
	Sequence int64 `json:"sequence"`
}

type PayRequest struct {
	BusinessID string `json:"business_id"`
}

type PayResponse struct {
	Paid      bool  `json:"paid"`
	Attempts  int   `json:"attempts"`
	CaptureID int64 `json:"capture_id"`
}

type CheckoutResponse struct {
	Accepted  bool   `json:"accepted"`
	Route     string `json:"route"`
	CaptureID int64  `json:"capture_id"`
}

type VoidRequest struct {
	Sequence int64  `json:"sequence"`
	Reason   string `json:"reason"`
}

func ValidToken(value string) bool { return tokenPattern.MatchString(value) }

func DecodeJSON(request *http.Request, destination any) error {
	if request == nil || request.Body == nil || destination == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxBodyBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request contains a trailing JSON value")
		}
		return fmt.Errorf("decode request trailer: %w", err)
	}
	return nil
}

func WriteJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func PostJSON(ctx context.Context, client *http.Client, target string, input, output any) error {
	if ctx == nil || client == nil || target == "" {
		return errors.New("post dependencies are required")
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("post request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxBodyBytes))
		return fmt.Errorf("post response status %d", response.StatusCode)
	}
	if output == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxBodyBytes))
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxBodyBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func GetJSON(ctx context.Context, client *http.Client, target string, output any) error {
	if ctx == nil || client == nil || target == "" || output == nil {
		return errors.New("get dependencies are required")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("get request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("get response status %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxBodyBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func DefaultClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout}
}
