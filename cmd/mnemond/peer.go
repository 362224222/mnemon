package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mnemon-dev/mnemon/internal/daemon"
)

const maxPeerCardInputBytes = 1025

type peerDependencies struct {
	workingDirectory func() (string, error)
	resolveState     func(string) (string, string, error)
	provision        func(context.Context, string) (daemon.ProvisionResult, error)
	configure        func(context.Context, string, string, string) (daemon.PeerCard, error)
	parseCard        func([]byte) (daemon.PeerCard, error)
	enroll           func(context.Context, string, string, daemon.PeerCard) (
		daemon.PeerEnrollment, error)
}

func productionPeerDependencies() peerDependencies {
	return peerDependencies{workingDirectory: os.Getwd, resolveState: daemon.ResolveProjectState,
		provision: daemon.Provision, configure: daemon.ConfigureExchange,
		parseCard: daemon.ParsePeerCardCanonicalJSON, enroll: daemon.EnrollPeer}
}

func runPeer(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer,
	deps peerDependencies,
) int {
	if ctx == nil || stdin == nil || stdout == nil || stderr == nil || !deps.available() {
		return 1
	}
	if len(args) == 0 {
		return writePeerUsage(stderr, "a subcommand is required")
	}
	switch args[0] {
	case "prepare":
		return runPeerPrepare(ctx, args[1:], stdout, stderr, deps)
	case "enroll":
		return runPeerEnroll(ctx, args[1:], stdin, stdout, stderr, deps)
	default:
		return writePeerUsage(stderr, fmt.Sprintf("unsupported subcommand %q", args[0]))
	}
}

func runPeerPrepare(ctx context.Context, args []string, stdout, stderr io.Writer,
	deps peerDependencies,
) int {
	options, err := parsePeerPrepareOptions(args)
	if err != nil {
		return writePeerUsage(stderr, err.Error())
	}
	root, err := resolvePeerProjectRoot(options.projectRoot, deps)
	if err != nil {
		return writePeerFailure(stderr, err)
	}
	provisioned, err := deps.provision(ctx, root)
	if err != nil {
		return writePeerFailure(stderr, err)
	}
	card, err := deps.configure(ctx, provisioned.StateDirectory(), options.listenAddress,
		options.advertisedAddress)
	if err != nil {
		return writePeerFailure(stderr, err)
	}
	if _, err := stdout.Write(append(card.CanonicalJSON(), '\n')); err != nil {
		return 1
	}
	return 0
}

func runPeerEnroll(ctx context.Context, args []string, stdin io.Reader,
	stdout, stderr io.Writer, deps peerDependencies,
) int {
	options, err := parsePeerEnrollOptions(args)
	if err != nil {
		return writePeerUsage(stderr, err.Error())
	}
	root, err := resolvePeerProjectRoot(options.projectRoot, deps)
	if err != nil {
		return writePeerFailure(stderr, err)
	}
	_, stateDirectory, err := deps.resolveState(root)
	if err != nil {
		return writePeerFailure(stderr, err)
	}
	card, err := readPeerCard(stdin, deps.parseCard)
	if err != nil {
		return writePeerUsage(stderr, err.Error())
	}
	result, err := deps.enroll(ctx, stateDirectory, options.alias, card)
	if err != nil {
		return writePeerFailure(stderr, err)
	}
	if _, err := stdout.Write(append(result.CanonicalJSON(), '\n')); err != nil {
		return 1
	}
	return 0
}

func (deps peerDependencies) available() bool {
	return deps.workingDirectory != nil && deps.resolveState != nil && deps.provision != nil &&
		deps.configure != nil && deps.parseCard != nil && deps.enroll != nil
}

type peerPrepareOptions struct {
	projectRoot       string
	listenAddress     string
	advertisedAddress string
}

func parsePeerPrepareOptions(args []string) (peerPrepareOptions, error) {
	var options peerPrepareOptions
	seen := make(map[string]bool, 3)
	for index := 0; index < len(args); index++ {
		flag := args[index]
		if seen[flag] || index+1 >= len(args) {
			return peerPrepareOptions{}, fmt.Errorf("%s requires one value", flag)
		}
		seen[flag] = true
		index++
		switch flag {
		case "--project-root":
			options.projectRoot = args[index]
		case "--listen":
			options.listenAddress = args[index]
		case "--advertise":
			options.advertisedAddress = args[index]
		default:
			return peerPrepareOptions{}, fmt.Errorf("unsupported argument %q", flag)
		}
	}
	if options.listenAddress == "" || options.advertisedAddress == "" {
		return peerPrepareOptions{}, errors.New("prepare requires --listen and --advertise")
	}
	if seen["--project-root"] && options.projectRoot == "" {
		return peerPrepareOptions{}, errors.New("--project-root must not be empty")
	}
	return options, nil
}

type peerEnrollOptions struct {
	projectRoot string
	alias       string
}

func parsePeerEnrollOptions(args []string) (peerEnrollOptions, error) {
	var options peerEnrollOptions
	seen := make(map[string]bool, 2)
	for index := 0; index < len(args); index++ {
		flag := args[index]
		if seen[flag] || index+1 >= len(args) {
			return peerEnrollOptions{}, fmt.Errorf("%s requires one value", flag)
		}
		seen[flag] = true
		index++
		switch flag {
		case "--project-root":
			options.projectRoot = args[index]
		case "--alias":
			options.alias = args[index]
		default:
			return peerEnrollOptions{}, fmt.Errorf("unsupported argument %q", flag)
		}
	}
	if options.alias == "" {
		return peerEnrollOptions{}, errors.New("enroll requires --alias")
	}
	if seen["--project-root"] && options.projectRoot == "" {
		return peerEnrollOptions{}, errors.New("--project-root must not be empty")
	}
	return options, nil
}

func resolvePeerProjectRoot(requested string, deps peerDependencies) (string, error) {
	if requested == "" {
		var err error
		requested, err = deps.workingDirectory()
		if err != nil {
			return "", err
		}
	}
	root, _, err := deps.resolveState(requested)
	return root, err
}

func readPeerCard(input io.Reader, parse func([]byte) (daemon.PeerCard, error)) (
	daemon.PeerCard, error,
) {
	raw, err := io.ReadAll(io.LimitReader(input, maxPeerCardInputBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxPeerCardInputBytes {
		return daemon.PeerCard{}, errors.New("canonical Peer Card on stdin is required")
	}
	raw = bytes.TrimSuffix(raw, []byte{'\n'})
	return parse(raw)
}

func writePeerUsage(stderr io.Writer, message string) int {
	_, _ = fmt.Fprintf(stderr, "mnemond peer: %s\n", message)
	return 2
}

func writePeerFailure(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintf(stderr, "mnemond peer: %v\n", err)
	return 1
}
