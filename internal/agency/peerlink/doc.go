// Package peerlink owns the replaceable R7 peer transport. It authenticates
// enrolled Ed25519 keys, bounds one request and one response per TCP
// connection, and moves opaque agency candidates and Artifact bytes without owning
// routes, admission, settlement, or domain state.
package peerlink
