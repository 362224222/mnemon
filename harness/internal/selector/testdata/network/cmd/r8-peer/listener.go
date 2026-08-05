package main

import (
	"net"
	"sync"
)

// boundedListener acquires capacity before accepting a connection. The
// kernel's listen backlog may queue callers, but the process owns at most the
// configured number of accepted connections and net/http goroutines.
type boundedListener struct {
	net.Listener
	slots     chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newBoundedListener(listener net.Listener, maximum int) *boundedListener {
	return &boundedListener{Listener: listener, slots: make(chan struct{}, maximum),
		done: make(chan struct{})}
}

func (listener *boundedListener) Accept() (net.Conn, error) {
	select {
	case listener.slots <- struct{}{}:
	case <-listener.done:
		return nil, net.ErrClosed
	}
	connection, err := listener.Listener.Accept()
	if err != nil {
		<-listener.slots
		return nil, err
	}
	return &boundedConnection{Conn: connection, release: func() { <-listener.slots }}, nil
}

func (listener *boundedListener) Close() error {
	listener.closeOnce.Do(func() {
		close(listener.done)
		listener.closeErr = listener.Listener.Close()
	})
	return listener.closeErr
}

type boundedConnection struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
}

func (connection *boundedConnection) Close() error {
	err := connection.Conn.Close()
	connection.releaseOnce.Do(connection.release)
	return err
}
