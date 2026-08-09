package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/mnemon-dev/mnemon/cmd"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exitCode := cmd.Execute(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
