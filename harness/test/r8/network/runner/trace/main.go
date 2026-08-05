// Command trace converts bounded evidence from the real R8 Docker proof into
// the protocol-neutral mnemon.test.trace format. It never reads or writes a
// selector store and never reconstructs facts that the running adapter did not
// report directly.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

type options struct {
	inputDir   string
	outputPath string
	runID      string
	candidate  string
	startedAt  time.Time
	finishedAt time.Time
}

func main() {
	parsed, err := parseOptions(os.Args[1:])
	if err != nil {
		fatal(err)
	}
	proof, err := loadEvidence(parsed.inputDir)
	if err != nil {
		fatal(err)
	}
	if err := writeAtomic(parsed.outputPath, func(destination io.Writer) error {
		return writeTrace(destination, parsed, proof)
	}); err != nil {
		fatal(err)
	}
}

func parseOptions(arguments []string) (options, error) {
	set := flag.NewFlagSet("r8-network-trace", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var input, output, runID, candidate, startedAt, finishedAt string
	set.StringVar(&input, "input", "", "directory containing bounded R8 evidence")
	set.StringVar(&output, "output", "", "trace output path")
	set.StringVar(&runID, "run-id", "", "bounded run identity")
	set.StringVar(&candidate, "candidate", "", "candidate image digest")
	set.StringVar(&startedAt, "started-at", "", "run start in RFC3339Nano")
	set.StringVar(&finishedAt, "finished-at", "", "run finish in RFC3339Nano")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 || input == "" || output == "" ||
		runID == "" || candidate == "" || startedAt == "" || finishedAt == "" {
		return options{}, errors.New("input, output, run-id, candidate, started-at, and finished-at are required")
	}
	started, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		return options{}, fmt.Errorf("parse started-at: %w", err)
	}
	finished, err := time.Parse(time.RFC3339Nano, finishedAt)
	if err != nil || finished.Before(started) {
		return options{}, errors.New("finished-at must be valid and not precede started-at")
	}
	return options{inputDir: input, outputPath: output, runID: runID,
		candidate: candidate, startedAt: started, finishedAt: finished}, nil
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "r8 network trace: %v\n", err)
	os.Exit(1)
}
