// Package daemon composes one local authority, immutable Artifact store, and its
// owner-only Unix control boundary.
//
// It owns process mechanics only. Semantic Event kinds remain opaque, peer
// exchange is composed separately, and setup must provision durable state
// before Open is called.
package daemon
