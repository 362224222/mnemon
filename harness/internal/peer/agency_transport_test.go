package peer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAgencyClientServerDispatchesOpaqueDeliveryAndScopedObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	localIdentity := testAuthorityPeer(t, "agency-transport-local")
	remoteIdentity := testAuthorityPeer(t, "agency-transport-remote")
	localHost := newBarePeerHost(t, localIdentity)
	remoteHost := newBarePeerHost(t, remoteIdentity)
	t.Cleanup(func() { _ = localHost.Close() })
	t.Cleanup(func() { _ = remoteHost.Close() })

	_, delivery, _ := agencyFrameDomainFixture(t)
	offer, _ := NewAgencyDeliveryOffer(AgencyDeliveryOfferSpec{
		DeliveryID: delivery.ID().String(), EnvelopeDigest: delivery.EnvelopeDigest().String(),
		CanonicalDelivery: delivery.CanonicalJSON(), Signature: bytes.Repeat([]byte{3}, ed25519.SignatureSize),
	})
	objectBytes := []byte("opaque artifact bytes")
	request, _ := NewAgencyObjectRequest(offer.DeliveryID(), offer.EnvelopeDigest(),
		agencyObjectDigest(objectBytes))

	var callbackMu sync.Mutex
	var deliveryPeer, objectPeer model.PeerID
	server, err := NewAgencyServer(ctx, AgencyServerOptions{Host: remoteHost,
		Delivery: AgencyDeliveryHandlerFunc(func(_ context.Context, source model.PeerID,
			got AgencyDeliveryOffer,
		) (AgencyDeliveryReply, error) {
			callbackMu.Lock()
			deliveryPeer = source
			callbackMu.Unlock()
			if got.DeliveryID() != offer.DeliveryID() ||
				!bytes.Equal(got.CanonicalDelivery(), offer.CanonicalDelivery()) {
				return nil, errors.New("Delivery callback changed opaque input")
			}
			return NewAgencyTransportAck(got.DeliveryID(), got.EnvelopeDigest())
		}),
		Object: AgencyObjectHandlerFunc(func(_ context.Context, source model.PeerID,
			got AgencyObjectRequest,
		) (AgencyObjectReply, error) {
			callbackMu.Lock()
			objectPeer = source
			callbackMu.Unlock()
			return NewAgencyObjectResponse(AgencyObjectResponseSpec{DeliveryID: got.DeliveryID(),
				EnvelopeDigest: got.EnvelopeDigest(), ObjectDigest: got.ObjectDigest(), Bytes: objectBytes})
		})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	connectAgencyTestHosts(t, ctx, localHost, remoteHost)
	client, err := NewAgencyClient(AgencyClientOptions{Host: localHost})
	if err != nil {
		t.Fatal(err)
	}

	reply, err := client.SendDelivery(ctx, remoteIdentity.modelID, offer)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reply.(AgencyTransportAck); !ok {
		t.Fatalf("Delivery reply = %T, want transport-only ACK", reply)
	}
	object, err := client.FetchObject(ctx, remoteIdentity.modelID, request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(object.Bytes(), objectBytes) {
		t.Fatal("Object client changed opaque bytes")
	}
	callbackMu.Lock()
	defer callbackMu.Unlock()
	if deliveryPeer != localIdentity.modelID || objectPeer != localIdentity.modelID {
		t.Fatalf("authenticated callback identities = (%s, %s), want %s",
			deliveryPeer, objectPeer, localIdentity.modelID)
	}
}

func TestAgencyClientReturnsClosedRemoteFailureWithoutAdmissionMeaning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	localIdentity := testAuthorityPeer(t, "agency-failure-local")
	remoteIdentity := testAuthorityPeer(t, "agency-failure-remote")
	localHost := newBarePeerHost(t, localIdentity)
	remoteHost := newBarePeerHost(t, remoteIdentity)
	t.Cleanup(func() { _ = localHost.Close() })
	t.Cleanup(func() { _ = remoteHost.Close() })
	failure, err := NewAgencyProtocolError(AgencyProtocolErrorSpec{Code: AgencyErrorNotFound})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewAgencyServer(ctx, AgencyServerOptions{Host: remoteHost,
		Delivery: AgencyDeliveryHandlerFunc(func(_ context.Context, _ model.PeerID,
			offer AgencyDeliveryOffer,
		) (AgencyDeliveryReply, error) {
			return NewAgencyTransportAck(offer.DeliveryID(), offer.EnvelopeDigest())
		}),
		Object: AgencyObjectHandlerFunc(func(context.Context, model.PeerID,
			AgencyObjectRequest,
		) (AgencyObjectReply, error) {
			return failure, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	connectAgencyTestHosts(t, ctx, localHost, remoteHost)
	client, _ := NewAgencyClient(AgencyClientOptions{Host: localHost})
	_, delivery, _ := agencyFrameDomainFixture(t)
	request, _ := NewAgencyObjectRequest(delivery.ID().String(), delivery.EnvelopeDigest().String(),
		agencyObjectDigest([]byte("missing")))
	_, err = client.FetchObject(ctx, remoteIdentity.modelID, request)
	var remoteFailure *AgencyRemoteFailure
	if !errors.As(err, &remoteFailure) || remoteFailure.Code() != AgencyErrorNotFound ||
		remoteFailure.Retryable() || remoteFailure.RetryAfter() != 0 {
		t.Fatalf("remote failure = %#v, %v", remoteFailure, err)
	}
}

func TestAgencyServerBoundsConcurrencyAndDrainsCancelledHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	localIdentity := testAuthorityPeer(t, "agency-bound-local")
	remoteIdentity := testAuthorityPeer(t, "agency-bound-remote")
	localHost := newBarePeerHost(t, localIdentity)
	remoteHost := newBarePeerHost(t, remoteIdentity)
	t.Cleanup(func() { _ = localHost.Close() })
	t.Cleanup(func() { _ = remoteHost.Close() })
	entered := make(chan struct{}, 1)
	handlerCancelled := make(chan struct{}, 1)
	server, err := NewAgencyServer(ctx, AgencyServerOptions{Host: remoteHost,
		Delivery: AgencyDeliveryHandlerFunc(func(callCtx context.Context, _ model.PeerID,
			_ AgencyDeliveryOffer,
		) (AgencyDeliveryReply, error) {
			entered <- struct{}{}
			<-callCtx.Done()
			handlerCancelled <- struct{}{}
			return nil, callCtx.Err()
		}),
		Object: AgencyObjectHandlerFunc(func(context.Context, model.PeerID,
			AgencyObjectRequest,
		) (AgencyObjectReply, error) {
			return nil, errors.New("unexpected Object callback")
		})})
	if err != nil {
		t.Fatal(err)
	}
	// The production value is fixed at HermeticLimits. Narrowing the private
	// test budget before traffic gives an independent overload oracle.
	server.budget = make(chan struct{}, 1)
	connectAgencyTestHosts(t, ctx, localHost, remoteHost)
	client, _ := NewAgencyClient(AgencyClientOptions{Host: localHost})
	_, delivery, _ := agencyFrameDomainFixture(t)
	offer, _ := NewAgencyDeliveryOffer(AgencyDeliveryOfferSpec{
		DeliveryID: delivery.ID().String(), EnvelopeDigest: delivery.EnvelopeDigest().String(),
		CanonicalDelivery: delivery.CanonicalJSON(), Signature: bytes.Repeat([]byte{4}, ed25519.SignatureSize),
	})
	firstDone := make(chan error, 1)
	go func() {
		_, callErr := client.SendDelivery(ctx, remoteIdentity.modelID, offer)
		firstDone <- callErr
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first Delivery callback did not start")
	}
	_, err = client.SendDelivery(ctx, remoteIdentity.modelID, offer)
	var busy *AgencyRemoteFailure
	if !errors.As(err, &busy) || busy.Code() != AgencyErrorBusy || !busy.Retryable() {
		t.Fatalf("overload response = %#v, %v", busy, err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := server.CloseContext(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerCancelled:
	case <-closeCtx.Done():
		t.Fatal("server shutdown did not cancel callback")
	}
	select {
	case <-firstDone:
	case <-closeCtx.Done():
		t.Fatal("cancelled client exchange did not finish")
	}
	if err := server.CloseContext(closeCtx); err != nil {
		t.Fatalf("repeated CloseContext() error = %v", err)
	}
}

func TestAgencyServerRejectsDuplicateProtocolOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	identity := testAuthorityPeer(t, "agency-duplicate")
	nodeHost := newBarePeerHost(t, identity)
	defer nodeHost.Close()
	delivery := AgencyDeliveryHandlerFunc(func(context.Context, model.PeerID,
		AgencyDeliveryOffer,
	) (AgencyDeliveryReply, error) {
		return nil, errors.New("unused")
	})
	object := AgencyObjectHandlerFunc(func(context.Context, model.PeerID,
		AgencyObjectRequest,
	) (AgencyObjectReply, error) {
		return nil, errors.New("unused")
	})
	server, err := NewAgencyServer(ctx, AgencyServerOptions{Host: nodeHost,
		Delivery: delivery, Object: object})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if duplicate, err := NewAgencyServer(ctx, AgencyServerOptions{Host: nodeHost,
		Delivery: delivery, Object: object}); duplicate != nil || !errors.Is(err, ErrAgencyServer) {
		t.Fatalf("duplicate server = (%v, %v)", duplicate, err)
	}
}

func TestAgencyServerFailsClosedOnTypedNilCallbacks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	localIdentity := testAuthorityPeer(t, "agency-typed-nil-local")
	remoteIdentity := testAuthorityPeer(t, "agency-typed-nil-remote")
	localHost := newBarePeerHost(t, localIdentity)
	remoteHost := newBarePeerHost(t, remoteIdentity)
	t.Cleanup(func() { _ = localHost.Close() })
	t.Cleanup(func() { _ = remoteHost.Close() })

	server, err := NewAgencyServer(ctx, AgencyServerOptions{Host: remoteHost,
		Delivery: AgencyDeliveryHandlerFunc(func(context.Context, model.PeerID,
			AgencyDeliveryOffer,
		) (AgencyDeliveryReply, error) {
			var reply *AgencyTransportAck
			return reply, nil
		}),
		Object: AgencyObjectHandlerFunc(func(context.Context, model.PeerID,
			AgencyObjectRequest,
		) (AgencyObjectReply, error) {
			var reply *AgencyObjectResponse
			return reply, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	connectAgencyTestHosts(t, ctx, localHost, remoteHost)
	client, err := NewAgencyClient(AgencyClientOptions{Host: localHost})
	if err != nil {
		t.Fatal(err)
	}

	_, delivery, _ := agencyFrameDomainFixture(t)
	offer, err := NewAgencyDeliveryOffer(AgencyDeliveryOfferSpec{
		DeliveryID: delivery.ID().String(), EnvelopeDigest: delivery.EnvelopeDigest().String(),
		CanonicalDelivery: delivery.CanonicalJSON(), Signature: bytes.Repeat([]byte{5}, ed25519.SignatureSize),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendDelivery(ctx, remoteIdentity.modelID, offer); !errors.Is(err,
		ErrAgencyClientTransport) {
		t.Fatalf("typed-nil Delivery callback error = %v, want transport reset", err)
	}

	objectBytes := []byte("typed-nil object")
	request, err := NewAgencyObjectRequest(offer.DeliveryID(), offer.EnvelopeDigest(),
		agencyObjectDigest(objectBytes))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.FetchObject(ctx, remoteIdentity.modelID, request); !errors.Is(err,
		ErrAgencyClientTransport) {
		t.Fatalf("typed-nil Object callback error = %v, want transport reset", err)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := server.CloseContext(closeCtx); err != nil {
		t.Fatalf("CloseContext() after typed-nil callbacks = %v", err)
	}
	if err := server.CloseContext(closeCtx); err != nil {
		t.Fatalf("repeated CloseContext() after typed-nil callbacks = %v", err)
	}
}

func TestAgencyFramesAndReplyValidationRejectTypedNil(t *testing.T) {
	var deliveryReply *AgencyTransportAck
	var objectReply *AgencyObjectResponse
	if _, err := NewAgencyDeliveryFrame(deliveryReply); err == nil {
		t.Fatal("NewAgencyDeliveryFrame(typed nil) succeeded")
	}
	if _, err := NewAgencyObjectFrame(objectReply); err == nil {
		t.Fatal("NewAgencyObjectFrame(typed nil) succeeded")
	}
	if validAgencyDeliveryReply(AgencyDeliveryOffer{}, deliveryReply) {
		t.Fatal("typed-nil Delivery reply passed validation")
	}
	if validAgencyObjectReply(AgencyObjectRequest{}, objectReply) {
		t.Fatal("typed-nil Object reply passed validation")
	}
}

func connectAgencyTestHosts(t *testing.T, ctx context.Context, local, remote host.Host) {
	t.Helper()
	if local == nil || remote == nil || local.ID() == "" || remote.ID() == "" {
		t.Fatal("test Host identity is empty")
	}
	if err := local.Connect(ctx, libp2ppeer.AddrInfo{ID: remote.ID(), Addrs: remote.Addrs()}); err != nil {
		t.Fatal(err)
	}
}
