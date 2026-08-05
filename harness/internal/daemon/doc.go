// Package daemon composes one R7 local authority, immutable CAS, and its
// owner-only Unix control boundary.
//
// It owns process mechanics only. Semantic Event kinds remain opaque, peer
// exchange is composed separately, and setup must provision durable state
// before Open is called.
package daemon
