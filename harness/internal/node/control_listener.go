package node

import (
	"errors"
	"net"
	"sync"
)

const controllerHTTPConnectionLimit uint32 = 32

// connectionAdmissionListener acquires capacity before calling the underlying
// Accept. This bounds the number of connections handed to net/http, rather than
// merely blocking handlers after net/http has already created goroutines.
type connectionAdmissionListener struct {
	net.Listener
	permits   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newConnectionAdmissionListener(listener net.Listener,
	limit uint32,
) (*connectionAdmissionListener, error) {
	if listener == nil || limit == 0 {
		return nil, errors.New("controller listener requires a finite connection limit")
	}
	return &connectionAdmissionListener{Listener: listener, permits: make(chan struct{}, limit),
		closed: make(chan struct{})}, nil
}

func (listener *connectionAdmissionListener) Accept() (net.Conn, error) {
	if listener == nil || listener.Listener == nil {
		return nil, net.ErrClosed
	}
	select {
	case listener.permits <- struct{}{}:
	case <-listener.closed:
		return nil, net.ErrClosed
	}
	connection, err := listener.Listener.Accept()
	if err != nil {
		listener.release()
		return nil, err
	}
	return &admittedConnection{Conn: connection, release: listener.release}, nil
}

func (listener *connectionAdmissionListener) Close() error {
	if listener == nil || listener.Listener == nil {
		return nil
	}
	listener.closeOnce.Do(func() {
		close(listener.closed)
		listener.closeErr = listener.Listener.Close()
	})
	return listener.closeErr
}

func (listener *connectionAdmissionListener) release() {
	<-listener.permits
}

type admittedConnection struct {
	net.Conn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func (connection *admittedConnection) Close() error {
	if connection == nil || connection.Conn == nil {
		return nil
	}
	connection.closeOnce.Do(func() {
		connection.closeErr = connection.Conn.Close()
		connection.release()
	})
	return connection.closeErr
}
