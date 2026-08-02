// Package selector implements the optional R8 binary preference selector.
//
// The package is deliberately independent of R7 domain state. It validates a
// canonical selection scope and computes bounded, single-round state changes;
// it performs no I/O and cannot create Events, mutate References, or complete
// Handlings.
package selector
