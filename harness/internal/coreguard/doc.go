// Package coreguard holds architectural guard tests that keep the collaboration-channel core
// generic. The load-bearing invariant: the core — contract, channel, kernel, store, eventview,
// rule, reconcile, runtime — contains ONLY the generic governed-event mechanism. It imports no
// application/host/optional ring, and hardcodes no business kind vocabulary. The core is what makes
// mnemon a protocol; everything specific (capabilities, hosts, the optional autopilot, demos) is an
// add-on OUTSIDE it. These tests fail the build the moment that line is crossed.
package coreguard
