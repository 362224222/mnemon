package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
)

func runControl(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("control", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	socket := flags.String("socket", "", "owner-only Unix control socket")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *socket == "" || flags.NArg() != 1 {
		return errors.New("control requires --socket and exactly one of status|round")
	}
	action := flags.Arg(0)
	method, path := http.MethodGet, controlStatusPath
	switch action {
	case "status":
	case "round":
		method, path = http.MethodPost, controlRoundPath
	default:
		return fmt.Errorf("unsupported control action %q", action)
	}
	return invokeControl(ctx, *socket, method, path)
}

func invokeControl(ctx context.Context, socket, method, path string) error {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: requestBudget}).DialContext(ctx, "unix", socket)
	}, DisableKeepAlives: true}
	client := &http.Client{Timeout: requestBudget, Transport: transport}
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, method, "http://unix"+path, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, readErr := readBounded(response.Body, 16<<10)
	if readErr != nil {
		return readErr
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("control returned %s: %s", response.Status, boundedError(errors.New(string(body))))
	}
	_, err = os.Stdout.Write(append(body, '\n'))
	return err
}

func parseProbeOptions(args []string) (commonOptions, string, string, error) {
	options := commonOptions{}
	flags := flag.NewFlagSet("probe", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&options.stateDir, "state-dir", "", "private selector state directory")
	flags.StringVar(&options.config, "config", "", "frozen selector config")
	flags.StringVar(&options.self, "id", "", "local participant ID")
	target := flags.String("target", "", "frozen target participant")
	mode := flags.String("mode", "", "no-vote or identity-mismatch")
	if err := flags.Parse(args); err != nil {
		return commonOptions{}, "", "", err
	}
	if flags.NArg() != 0 {
		return commonOptions{}, "", "", errors.New("probe accepts no positional arguments")
	}
	if err := requireValues(options.stateDir, options.config, options.self, *target, *mode); err != nil {
		return commonOptions{}, "", "", err
	}
	if *mode != "no-vote" && *mode != "identity-mismatch" {
		return commonOptions{}, "", "", errors.New("probe mode must be no-vote or identity-mismatch")
	}
	return options, *target, *mode, nil
}
