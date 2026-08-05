// Package selector implements the optional R8 binary preference selector.
//
// The package is deliberately independent of R7 domain state. It validates a
// canonical selection scope and computes bounded, single-round state changes.
// Its optional private Store durably freezes those inputs and observations in
// selector.db, but it cannot create Events, mutate References, or complete
// Handlings. Network I/O and R7 admission remain outside this package.
package selector
