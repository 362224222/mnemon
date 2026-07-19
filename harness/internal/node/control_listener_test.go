package node

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConnectionAdmissionListenerBoundsAcceptBeforeNetHTTP(t *testing.T) {
	t.Parallel()
	raw := newScriptedListener()
	listener, err := newConnectionAdmissionListener(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	serverEnds := make([]net.Conn, 3)
	clientEnds := make([]net.Conn, 3)
	for index := range serverEnds {
		serverEnds[index], clientEnds[index] = net.Pipe()
		raw.accepts <- serverEnds[index]
		defer clientEnds[index].Close()
	}
	first := mustAcceptConnection(t, listener)
	<-raw.called
	second := mustAcceptConnection(t, listener)
	<-raw.called
	thirdStarted := make(chan struct{})
	thirdResult := make(chan net.Conn, 1)
	thirdError := make(chan error, 1)
	go func() {
		close(thirdStarted)
		connection, acceptErr := listener.Accept()
		thirdResult <- connection
		thirdError <- acceptErr
	}()
	<-thirdStarted
	select {
	case <-raw.called:
		t.Fatal("underlying Accept ran while the connection budget was full")
	case <-time.After(25 * time.Millisecond):
	}
	mustCloseConnection(t, first)
	select {
	case <-raw.called:
	case <-time.After(time.Second):
		t.Fatal("releasing a connection did not resume underlying Accept")
	}
	var third net.Conn
	select {
	case third = <-thirdResult:
		if err := <-thirdError; err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("third connection was not admitted after capacity returned")
	}
	mustCloseConnection(t, second)
	mustCloseConnection(t, third)
	mustCloseConnection(t, listener)
}

func TestConnectionAdmissionListenerCloseUnblocksCapacityWaiter(t *testing.T) {
	t.Parallel()
	raw := newScriptedListener()
	listener, err := newConnectionAdmissionListener(raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	raw.accepts <- server
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	<-raw.called
	waiting := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(waiting)
		_, acceptErr := listener.Accept()
		done <- acceptErr
	}()
	<-waiting
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("blocked Accept error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock capacity-limited Accept")
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("repeated Close() = %v", err)
	}
}

func TestConnectionAdmissionListenerRequiresFiniteAuthority(t *testing.T) {
	t.Parallel()
	if _, err := newConnectionAdmissionListener(nil, 1); err == nil {
		t.Fatal("nil listener was accepted")
	}
	raw := newScriptedListener()
	if _, err := newConnectionAdmissionListener(raw, 0); err == nil {
		t.Fatal("zero connection limit was accepted")
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

type scriptedListener struct {
	accepts   chan net.Conn
	called    chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newScriptedListener() *scriptedListener {
	return &scriptedListener{accepts: make(chan net.Conn, 8), called: make(chan struct{}, 8),
		closed: make(chan struct{})}
}

func (listener *scriptedListener) Accept() (net.Conn, error) {
	listener.called <- struct{}{}
	select {
	case connection := <-listener.accepts:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *scriptedListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (*scriptedListener) Addr() net.Addr { return scriptedAddress("controller-test") }

func mustAcceptConnection(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func mustCloseConnection(t *testing.T, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

type scriptedAddress string

func (address scriptedAddress) Network() string { return "scripted" }

func (address scriptedAddress) String() string { return string(address) }
