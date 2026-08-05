package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

const (
	networkPath       = "/sample"
	controlReadyPath  = "/ready"
	controlStatusPath = "/status"
	controlRoundPath  = "/round"
	requestBudget     = 3 * time.Second
	shutdownBudget    = 3 * time.Second
	maxConcurrentHTTP = 8
)

type peerService struct {
	config        runtimeConfig
	self          peerRuntime
	private       ed25519.PrivateKey
	store         *selector.Store
	networkServer *http.Server
	controlServer *http.Server
	controlSocket string
	attempts      *attemptLedger
	handlers      *handlerTracker
}

func runServe(ctx context.Context, args []string) (err error) {
	options, err := parseCommon("serve", args, func(flags *flag.FlagSet, options *commonOptions) {
		flags.StringVar(&options.stateDir, "state-dir", "", "private selector state directory")
		flags.StringVar(&options.config, "config", "", "frozen selector config")
		flags.StringVar(&options.self, "id", "", "local participant ID")
		flags.StringVar(&options.listen, "listen", "", "bounded sample listen address")
		flags.StringVar(&options.control, "control", "", "owner-only Unix control socket")
	})
	if err != nil {
		return err
	}
	if err := requireValues(options.stateDir, options.config, options.self,
		options.listen, options.control); err != nil {
		return err
	}
	service, err := openPeerService(ctx, options)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, service.close()) }()
	return service.serve(ctx, options.listen, options.stateDir)
}

func openPeerService(ctx context.Context, options commonOptions) (*peerService, error) {
	config, err := loadConfig(options.config)
	if err != nil {
		return nil, err
	}
	self, err := requireSelfIdentity(config, options.self, options.stateDir)
	if err != nil {
		return nil, err
	}
	private, err := loadPrivateKey(options.stateDir)
	if err != nil {
		return nil, err
	}
	store, err := selector.OpenStore(ctx, filepath.Join(options.stateDir, databaseName))
	if err != nil {
		return nil, err
	}
	if _, err := store.Selection(ctx, config.descriptor.ID()); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("load configured selection: %w", err)
	}
	attempts, err := openAttemptLedger(options.stateDir, len(config.peers),
		config.descriptor.Profile().MaxRounds())
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	service := &peerService{config: config, self: self, private: private, store: store,
		controlSocket: options.control, attempts: attempts, handlers: newHandlerTracker()}
	service.networkServer = boundedServer(service.handlers.wrap(service.networkMux()))
	service.controlServer = boundedServer(service.handlers.wrap(service.controlMux()))
	return service, nil
}

func boundedServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler, ReadHeaderTimeout: requestBudget,
		ReadTimeout: requestBudget, WriteTimeout: requestBudget,
		IdleTimeout: requestBudget, MaxHeaderBytes: 1024}
}

func (service *peerService) serve(ctx context.Context, address, stateDirectory string) error {
	serviceContext, cancelService := context.WithCancel(ctx)
	defer cancelService()
	networkSocket, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen for sample queries: %w", err)
	}
	networkListener := newBoundedListener(networkSocket, maxConcurrentHTTP)
	controlSocket, err := listenControlSocket(service.controlSocket, stateDirectory)
	if err != nil {
		_ = networkListener.Close()
		return err
	}
	controlListener := newBoundedListener(controlSocket, 1)
	service.networkServer.BaseContext = func(net.Listener) context.Context { return serviceContext }
	service.controlServer.BaseContext = func(net.Listener) context.Context { return serviceContext }
	results := make(chan error, 2)
	go func() { results <- service.networkServer.Serve(networkListener) }()
	go func() { results <- service.controlServer.Serve(controlListener) }()
	received := 0
	var result error
	select {
	case <-ctx.Done():
		result = ctx.Err()
	case result = <-results:
		received++
	}
	service.handlers.stop()
	cancelService()
	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	shutdownErr := errors.Join(service.networkServer.Shutdown(shutdownContext),
		service.controlServer.Shutdown(shutdownContext))
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, service.networkServer.Close(),
			service.controlServer.Close())
	}
	for received < 2 {
		serveErr := <-results
		if !errors.Is(serveErr, http.ErrServerClosed) {
			result = errors.Join(result, serveErr)
		}
		received++
	}
	drainErr := service.handlers.wait(shutdownContext)
	cancel()
	if errors.Is(result, http.ErrServerClosed) || errors.Is(result, context.Canceled) {
		result = nil
	}
	return errors.Join(result, shutdownErr, drainErr)
}

func listenControlSocket(path, stateDirectory string) (net.Listener, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) != "control.sock" ||
		filepath.Dir(path) != stateDirectory {
		return nil, errors.New("control socket must be the state directory's absolute control.sock")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("control path exists and is not a socket")
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on control socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("protect control socket: %w", err)
	}
	return listener, nil
}

func (service *peerService) close() error {
	if service == nil {
		return nil
	}
	removeErr := os.Remove(service.controlSocket)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if !service.handlers.idle() {
		return errors.Join(removeErr, errors.New("refusing to close selector store with active handlers"))
	}
	return errors.Join(service.store.Close(), removeErr)
}
