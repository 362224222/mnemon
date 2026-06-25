// Package driver is the co-hosted Background Driver: it runs INSIDE the Local Runtime process (holding
// the same single store-writer lock — never a second opener) and periodically drives the governed
// Tick, drains projection invalidations, and invokes the caller-supplied side effect for explicit
// workers such as tests. Runtime serving paths do not write host projection files.
package driver

import (
	"context"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// Driver drives one runtime's background duties. reproject is invoked only when a Tick actually
// drained an invalidation (it is nil for a runtime with no host projection).
type Driver struct {
	rt        *runtime.Runtime
	reproject func(refs []contract.ResourceRef) error
	interval  time.Duration
}

// New builds a Driver over rt with an injected re-projection callback (the host-free seam used by
// tests). interval <= 0 defaults to one second.
func New(rt *runtime.Runtime, reproject func(refs []contract.ResourceRef) error, interval time.Duration) *Driver {
	return &Driver{rt: rt, reproject: reproject, interval: interval}
}

// Tick runs one background cycle: advance the governed Tick, drain any projection invalidations, and —
// only if something was invalidated — call the injected side effect. It uses the runtime's own store
// (no second opener).
func (d *Driver) Tick(ctx context.Context) error {
	if _, err := d.rt.Tick(); err != nil {
		return err
	}
	refs, drained, err := d.rt.DrainOutbox()
	if err != nil {
		return err
	}
	if drained > 0 && d.reproject != nil {
		return d.reproject(refs)
	}
	return nil
}

// Run loops Tick on the interval until ctx is cancelled (clean shutdown). It returns ctx.Err() on
// cancellation, or the first Tick error.
func (d *Driver) Run(ctx context.Context) error {
	interval := d.interval
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := d.Tick(ctx); err != nil {
				return err
			}
		}
	}
}
