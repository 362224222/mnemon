// Package agency defines the protocol-neutral R7 domain vocabulary.
//
// AgentIntent contains only bounded semantic input and a selection from the
// closed consequence set. BoundIntent adds machine-owned authority. The
// package validates and canonically represents those values, but deliberately
// owns no admission policy, persistence, transport, CLI, or collaboration
// pattern.
package agency
