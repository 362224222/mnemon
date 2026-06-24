// Package render builds read-only hostagent event views from scoped projections.
//
// The package is the hot-content side of mnemond: it derives AgentEvent values
// for a principal, then presents them as the current hook/skill text format.
// Host-specific mechanics remain in hostsurface; governed writes remain behind
// observe/rule/kernel; mnemonhub sync remains an accepted-event transport.
package render
