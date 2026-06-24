// Package presentation builds read-only hostagent presentations from scoped event views.
//
// The package is the hot-content side of mnemond: it derives EventEnvelope values
// for a principal, then presents them as the current hook/skill text format.
// Host-specific mechanics remain in hostsurface; governed writes remain behind
// observe/admission/state; mnemonhub exchange remains an accepted-event transport.
package presentation
