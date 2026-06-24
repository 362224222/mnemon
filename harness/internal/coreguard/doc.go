// Package coreguard holds architectural guard tests for the harness package topology. The
// load-bearing invariant is that implementation packages remain owned by the main axes:
// hostagent, mnemond, mnemonhub, and event. Residual words such as capability, channel, rule,
// kernel, sync, render, and eventview may exist only as implementation details with an explicit
// main-axis owner; they must not grow into new system-level concepts.
package coreguard
