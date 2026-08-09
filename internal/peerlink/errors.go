package peerlink

import "errors"

var (
	ErrInput          = errors.New("peerlink: invalid input")
	ErrAuthentication = errors.New("peerlink: authentication failed")
	ErrFrame          = errors.New("peerlink: invalid frame")
	ErrTransport      = errors.New("peerlink: transport unavailable")
)
