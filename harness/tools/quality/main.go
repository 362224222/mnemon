package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "harness-quality:", err)
		os.Exit(2)
	}
}

func execute(arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 0 {
		return fmt.Errorf("expected measure or check subcommand")
	}
	switch arguments[0] {
	case "measure":
		return executeMeasure(arguments[1:], stdout, stderr)
	case "check":
		return executeCheck(arguments[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown subcommand %q; expected measure or check", arguments[0])
	}
}

func executeMeasure(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("measure", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", "", "source tree root")
	sourceCommit := flags.String("source-commit", "", "full source commit hash")
	output := flags.String("output", "", "baseline JSON output path")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("measure does not accept positional arguments")
	}
	if *root == "" || *sourceCommit == "" || *output == "" {
		return fmt.Errorf("measure requires --root, --source-commit, and --output")
	}
	manifest, err := measureTree(*root, *sourceCommit)
	if err != nil {
		return err
	}
	if err := writeCanonicalJSON(*output, manifest); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "measured %d quality violations with %s\n", len(manifest.Entries), qualityToolVersion)
	return err
}

func executeCheck(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", "", "repository root")
	baseReference := flags.String("base-ref", "", "optional Git base reference")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("check does not accept positional arguments")
	}
	if *root == "" {
		return fmt.Errorf("check requires --root")
	}
	if err := checkRepository(*root, *baseReference); err != nil {
		return err
	}
	_, err := fmt.Fprintln(stdout, "harness quality check passed")
	return err
}
