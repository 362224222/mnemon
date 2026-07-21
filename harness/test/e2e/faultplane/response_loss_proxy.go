// Command r5-response-loss-proxy performs one domain-opaque Unix-socket fault.
// It forwards a client request, waits until the upstream emits its first
// response byte, then closes both connections without forwarding response
// bytes to the client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"
)

const (
	receiptSchemaVersion = 1
	maximumSocketPath    = 100
	maximumTimeout       = 5 * time.Minute
	pollInterval         = 100 * time.Millisecond
)

var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$`)

type proxyConfig struct {
	listenPath   string
	upstreamPath string
	receiptPath  string
	readyPath    string
	startedPath  string
	token        string
	timeout      time.Duration
}

type startedReceipt struct {
	SchemaVersion int    `json:"schema_version"`
	Token         string `json:"token"`
	Status        string `json:"status"`
	PID           int    `json:"pid"`
	StartedAt     string `json:"started_at"`
}

type readyReceipt struct {
	SchemaVersion int    `json:"schema_version"`
	Token         string `json:"token"`
	Status        string `json:"status"`
	PID           int    `json:"pid"`
	ReadyAt       string `json:"ready_at"`
}

type lossReceipt struct {
	SchemaVersion             int    `json:"schema_version"`
	Token                     string `json:"token"`
	Outcome                   string `json:"outcome"`
	StartedAt                 string `json:"started_at"`
	AcceptedAt                string `json:"accepted_at"`
	FirstResponseByteAt       string `json:"first_response_byte_at"`
	FinishedAt                string `json:"finished_at"`
	RequestBytesForwarded     int64  `json:"request_bytes_forwarded"`
	ResponseBytesForwarded    int64  `json:"response_bytes_forwarded"`
	FirstResponseByteObserved bool   `json:"first_response_byte_observed"`
}

type copyResult struct {
	bytes int64
	err   error
}

func main() {
	config := proxyConfig{}
	flag.StringVar(&config.listenPath, "listen", "", "Unix socket path exposed to the client")
	flag.StringVar(&config.upstreamPath, "upstream", "", "relocated upstream Unix socket path")
	flag.StringVar(&config.receiptPath, "receipt", "", "exclusive success receipt path")
	flag.StringVar(&config.readyPath, "ready", "", "exclusive readiness receipt path")
	flag.StringVar(&config.startedPath, "started", "", "exclusive process-start receipt path")
	flag.StringVar(&config.token, "token", "", "bounded fault action identity")
	flag.DurationVar(&config.timeout, "timeout", 30*time.Second, "one-shot accept/response deadline")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "r5-response-loss-proxy: positional arguments are not supported")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	if err := runProxy(ctx, config); err != nil {
		fmt.Fprintf(os.Stderr, "r5-response-loss-proxy: %v\n", err)
		os.Exit(1)
	}
}

func runProxy(ctx context.Context, config proxyConfig) error {
	if err := validateProxyConfig(config); err != nil {
		return err
	}
	started := time.Now().UTC()
	if err := writeExclusiveJSON(config.startedPath, startedReceipt{
		SchemaVersion: receiptSchemaVersion,
		Token:         config.token,
		Status:        "started",
		PID:           os.Getpid(),
		StartedAt:     started.Format(time.RFC3339Nano),
	}); err != nil {
		return fmt.Errorf("write start receipt: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.listenPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	defer os.Remove(config.listenPath)
	if err := os.Chmod(config.listenPath, 0o600); err != nil {
		return fmt.Errorf("restrict listener: %w", err)
	}
	if err := writeExclusiveJSON(config.readyPath, readyReceipt{
		SchemaVersion: receiptSchemaVersion,
		Token:         config.token,
		Status:        "ready",
		PID:           os.Getpid(),
		ReadyAt:       time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return fmt.Errorf("write ready receipt: %w", err)
	}

	acceptDeadline := time.Now().Add(config.timeout)
	client, err := acceptUntil(ctx, listener, acceptDeadline)
	if err != nil {
		return err
	}
	defer client.Close()
	accepted := time.Now().UTC()

	dialer := net.Dialer{}
	upstreamConnection, err := dialer.DialContext(ctx, "unix", config.upstreamPath)
	if err != nil {
		return fmt.Errorf("connect upstream: %w", err)
	}
	upstream := upstreamConnection.(*net.UnixConn)
	defer upstream.Close()

	requestDone := make(chan copyResult, 1)
	go func() {
		count, copyErr := io.Copy(upstream, client)
		if closeErr := upstream.CloseWrite(); copyErr == nil {
			copyErr = closeErr
		}
		requestDone <- copyResult{bytes: count, err: copyErr}
	}()

	responseDeadline := accepted.Add(config.timeout)
	firstByteAt, err := awaitFirstResponseByte(ctx, upstream, responseDeadline)
	_ = client.Close()
	_ = upstream.Close()
	request := <-requestDone
	if err != nil {
		return err
	}
	if request.bytes == 0 {
		return errors.New("upstream responded before any client request byte was forwarded")
	}
	if request.err != nil && !errors.Is(request.err, net.ErrClosed) {
		var netError *net.OpError
		if !errors.As(request.err, &netError) {
			return fmt.Errorf("forward request: %w", request.err)
		}
	}

	receipt := lossReceipt{
		SchemaVersion:             receiptSchemaVersion,
		Token:                     config.token,
		Outcome:                   "response_dropped_after_first_byte",
		StartedAt:                 started.Format(time.RFC3339Nano),
		AcceptedAt:                accepted.Format(time.RFC3339Nano),
		FirstResponseByteAt:       firstByteAt.Format(time.RFC3339Nano),
		FinishedAt:                time.Now().UTC().Format(time.RFC3339Nano),
		RequestBytesForwarded:     request.bytes,
		ResponseBytesForwarded:    0,
		FirstResponseByteObserved: true,
	}
	if err := writeExclusiveJSON(config.receiptPath, receipt); err != nil {
		return fmt.Errorf("write loss receipt: %w", err)
	}
	return nil
}

func validateProxyConfig(config proxyConfig) error {
	if !tokenPattern.MatchString(config.token) {
		return errors.New("token is empty, unbounded, or contains unsupported characters")
	}
	if config.timeout <= 0 || config.timeout > maximumTimeout {
		return errors.New("timeout is outside the bounded range")
	}
	paths := []string{
		config.listenPath,
		config.upstreamPath,
		config.receiptPath,
		config.readyPath,
		config.startedPath,
	}
	unique := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if !filepath.IsAbs(path) || len(path) > maximumSocketPath {
			return errors.New("all paths must be absolute and at most 100 bytes")
		}
		if _, exists := unique[path]; exists {
			return errors.New("listen, upstream, and receipt paths must all differ")
		}
		unique[path] = struct{}{}
	}
	if _, err := os.Lstat(config.listenPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("listen path must not already exist")
	}
	upstream, err := os.Lstat(config.upstreamPath)
	if err != nil {
		return fmt.Errorf("inspect upstream: %w", err)
	}
	if upstream.Mode()&os.ModeSocket == 0 {
		return errors.New("upstream path is not a Unix socket")
	}
	for _, output := range []string{config.receiptPath, config.readyPath, config.startedPath} {
		if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
			return errors.New("receipt paths must not already exist")
		}
		parent, err := os.Lstat(filepath.Dir(output))
		if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
			return errors.New("receipt parent must be an existing non-symlink directory")
		}
	}
	return nil
}

func acceptUntil(ctx context.Context, listener *net.UnixListener, deadline time.Time) (*net.UnixConn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("accept canceled: %w", err)
		}
		now := time.Now()
		if !now.Before(deadline) {
			return nil, errors.New("timed out waiting for the one client")
		}
		_ = listener.SetDeadline(minTime(deadline, now.Add(pollInterval)))
		client, err := listener.AcceptUnix()
		if err == nil {
			return client, nil
		}
		var netError net.Error
		if errors.As(err, &netError) && netError.Timeout() {
			continue
		}
		return nil, fmt.Errorf("accept client: %w", err)
	}
}

func awaitFirstResponseByte(ctx context.Context, upstream *net.UnixConn, deadline time.Time) (time.Time, error) {
	buffer := []byte{0}
	for {
		if err := ctx.Err(); err != nil {
			return time.Time{}, fmt.Errorf("response wait canceled: %w", err)
		}
		now := time.Now()
		if !now.Before(deadline) {
			return time.Time{}, errors.New("timed out before the upstream response began")
		}
		_ = upstream.SetReadDeadline(minTime(deadline, now.Add(pollInterval)))
		count, err := upstream.Read(buffer)
		if count == 1 {
			return time.Now().UTC(), nil
		}
		var netError net.Error
		if errors.As(err, &netError) && netError.Timeout() {
			continue
		}
		if err != nil {
			return time.Time{}, fmt.Errorf("read first upstream response byte: %w", err)
		}
	}
}

func writeExclusiveJSON(path string, value any) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
