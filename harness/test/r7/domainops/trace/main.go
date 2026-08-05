// Command trace converts the sanitized domain-operations live report and the
// stopped R7 authority stores into a protocol-neutral mnemon.test.trace file.
// It never reads a prompt, provider stream, transcript, or live daemon.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type options struct {
	reportPath     string
	failurePath    string
	authorityRoot  string
	outputPath     string
	scenarioRoot   string
	binaryManifest string
}

func main() {
	config, err := parseOptions(os.Args[1:])
	if err != nil {
		fatal(err)
	}
	scenario, err := loadScenarioEvidence(config.scenarioRoot, config.binaryManifest)
	if err != nil {
		fatal(err)
	}
	if err := writeAtomic(config.outputPath, func(destination io.Writer) error {
		if config.failurePath != "" {
			report, err := loadFailureReport(config.failurePath)
			if err != nil {
				return err
			}
			nodes, err := loadAuthorityNodes(config.authorityRoot)
			if err != nil {
				return err
			}
			return writeFailureTrace(destination, report, scenario, nodes)
		}
		proof, err := loadEvidence(config.reportPath, config.authorityRoot)
		if err != nil {
			return err
		}
		proof.Scenario = scenario
		return writeTrace(destination, proof)
	}); err != nil {
		fatal(err)
	}
}

func parseOptions(arguments []string) (options, error) {
	set := flag.NewFlagSet("r7-domain-ops-trace", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var report, failure, authority, output, scenario, binaries string
	set.StringVar(&report, "report", "", "sanitized live report")
	set.StringVar(&failure, "failure-report", "", "sanitized failed-run report")
	set.StringVar(&authority, "authority", "", "stopped per-role authority directories")
	set.StringVar(&output, "output", "", "trace output path")
	set.StringVar(&scenario, "scenario-root", "", "domain-ops scenario fixture root")
	set.StringVar(&binaries, "candidate-binaries", "", "candidate sha256sum manifest")
	if err := set.Parse(arguments); err != nil || set.NArg() != 0 ||
		(report == "") == (failure == "") || authority == "" || output == "" ||
		scenario == "" || binaries == "" {
		return options{}, errors.New("exactly one report type plus authority, output, scenario-root, and candidate-binaries is required")
	}
	for label, path := range map[string]string{"report": report, "authority": authority,
		"failure-report": failure, "output": output, "scenario-root": scenario,
		"candidate-binaries": binaries} {
		if path == "" {
			continue
		}
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return options{}, fmt.Errorf("%s path must be absolute and clean", label)
		}
	}
	return options{reportPath: report, failurePath: failure, authorityRoot: authority,
		outputPath: output, scenarioRoot: scenario, binaryManifest: binaries}, nil
}

func writeAtomic(path string, write func(io.Writer) error) error {
	if write == nil {
		return errors.New("trace writer callback is required")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create trace directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".r7-domain-ops-trace-*")
	if err != nil {
		return fmt.Errorf("create temporary trace: %w", err)
	}
	return renderAndPublish(temporary, path, write)
}

func renderAndPublish(temporary *os.File, path string, render func(io.Writer) error) error {
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := render(temporary); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect temporary trace: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary trace: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish trace: %w", err)
	}
	return nil
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "r7 domain ops trace: %v\n", err)
	os.Exit(1)
}
