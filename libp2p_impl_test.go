package protocolnetwork_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pn "github.com/ipfs/go-protocolnetwork"
	"github.com/ipfs/go-protocolnetwork/internal/testutil"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/multiformats/go-multistream"
	"github.com/stretchr/testify/require"
)

type MessageHandlerSelector struct {
	v1Handler testutil.ProtoMessageHandler
	v2Handler testutil.IPLDMessageHandler
}

func (mhs *MessageHandlerSelector) Select(proto protocol.ID) pn.MessageHandler[*testutil.Message] {
	if proto == testutil.ProtocolMockV1 {
		return &mhs.v1Handler
	}
	return &mhs.v2Handler
}

// Receiver is an interface for receiving messages from the GraphSyncNetwork.
type receiver struct {
	peers           map[peer.ID]struct{}
	messageReceived chan struct{}
	connectionEvent chan bool
	lastMessage     *testutil.Message
	lastSender      peer.ID
	listener        network.Notifiee
}

func newReceiver() *receiver {
	return &receiver{
		peers:           make(map[peer.ID]struct{}),
		messageReceived: make(chan struct{}),
		// Avoid blocking. 100 is good enough for tests.
		connectionEvent: make(chan bool, 100),
	}
}

func (r *receiver) ReceiveMessage(
	ctx context.Context,
	sender peer.ID,
	incoming *testutil.Message) {
	r.lastSender = sender
	r.lastMessage = incoming
	select {
	case <-ctx.Done():
	case r.messageReceived <- struct{}{}:
	}
}

func (r *receiver) ReceiveError(p peer.ID, err error) {
}

func (r *receiver) PeerConnected(p peer.ID) {
	r.peers[p] = struct{}{}
	r.connectionEvent <- true
}

func (r *receiver) PeerDisconnected(p peer.ID) {
	delete(r.peers, p)
	r.connectionEvent <- false
}

var errMockNetErr = fmt.Errorf("network err")

type ErrStream struct {
	network.Stream
	lk        sync.Mutex
	err       error
	timingOut bool
	closed    bool
}

type ErrHost struct {
	host.Host
	lk        sync.Mutex
	err       error
	timingOut bool
	streams   []*ErrStream
}

func (es *ErrStream) Write(b []byte) (int, error) {
	es.lk.Lock()
	defer es.lk.Unlock()

	if es.err != nil {
		return 0, es.err
	}
	if es.timingOut {
		return 0, context.DeadlineExceeded
	}
	return es.Stream.Write(b)
}

func (es *ErrStream) Close() error {
	es.lk.Lock()
	es.closed = true
	es.lk.Unlock()

	return es.Stream.Close()
}

func (eh *ErrHost) Connect(ctx context.Context, pi peer.AddrInfo) error {
	eh.lk.Lock()
	defer eh.lk.Unlock()

	if eh.err != nil {
		return eh.err
	}
	if eh.timingOut {
		return context.DeadlineExceeded
	}
	return eh.Host.Connect(ctx, pi)
}

func (eh *ErrHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	eh.lk.Lock()
	defer eh.lk.Unlock()

	if eh.err != nil {
		return nil, errMockNetErr
	}
	if eh.timingOut {
		return nil, context.DeadlineExceeded
	}
	stream, err := eh.Host.NewStream(ctx, p, pids...)
	estrm := &ErrStream{Stream: stream, err: eh.err, timingOut: eh.timingOut}

	eh.streams = append(eh.streams, estrm)
	return estrm, err
}

func (eh *ErrHost) setError(err error) {
	eh.lk.Lock()
	defer eh.lk.Unlock()

	eh.err = err
	for _, s := range eh.streams {
		s.lk.Lock()
		s.err = err
		s.lk.Unlock()
	}
}

func (eh *ErrHost) setTimeoutState(timingOut bool) {
	eh.lk.Lock()
	defer eh.lk.Unlock()

	eh.timingOut = timingOut
	for _, s := range eh.streams {
		s.lk.Lock()
		s.timingOut = timingOut
		s.lk.Unlock()
	}
}

func newNetwork(mn mocknet.Mocknet, p tnet.Identity) pn.ProtocolNetwork[*testutil.Message] {

	client, err := mn.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	return pn.NewFromLibp2pHost[*testutil.Message]("mock", client, &MessageHandlerSelector{}, pn.SupportedProtocols(testutil.DefaultProtocols))

}
func TestMessageSendAndReceive(t *testing.T) {
	testutil.Flaky(t)

	// create network
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	mn := mocknet.New()
	defer mn.Close()

	p1 := tnet.RandIdentityOrFatal(t)
	p2 := tnet.RandIdentityOrFatal(t)

	pn1 := newNetwork(mn, p1)
	pn2 := newNetwork(mn, p2)
	r1 := newReceiver()
	r2 := newReceiver()
	pn1.Start(r1)
	t.Cleanup(pn1.Stop)
	pn2.Start(r2)
	t.Cleanup(pn2.Stop)

	err := mn.LinkAll()
	require.NoError(t, err)

	err = pn1.ConnectTo(ctx, p2.ID())
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatal("did not connect peer")
	case <-r1.connectionEvent:
	}
	err = pn2.ConnectTo(ctx, p1.ID())
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatal("did not connect peer")
	case <-r2.connectionEvent:
	}
	if _, ok := r1.peers[p2.ID()]; !ok {
		t.Fatal("did to connect to correct peer")
	}
	if _, ok := r2.peers[p1.ID()]; !ok {
		t.Fatal("did to connect to correct peer")
	}
	sent := &testutil.Message{
		Id:      testutil.RandomBytes(100),
		Payload: testutil.RandomBytes(100),
	}

	err = pn1.SendMessage(ctx, p2.ID(), sent)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case <-r2.messageReceived:
	}

	require.Equal(t, p1.ID(), r2.lastSender)

	require.Equal(t, sent, r2.lastMessage)
}

func prepareNetwork(t *testing.T, ctx context.Context, p1 tnet.Identity, r1 *receiver, p2 tnet.Identity, r2 *receiver) (*ErrHost, pn.ProtocolNetwork[*testutil.Message], *ErrHost, pn.ProtocolNetwork[*testutil.Message], *testutil.Message) {
	// create network
	mn := mocknet.New()
	defer mn.Close()

	// Host 1
	h1, err := mn.AddPeer(p1.PrivateKey(), p1.Address())
	if err != nil {
		t.Fatal(err)
	}
	eh1 := &ErrHost{Host: h1}
	pn1 := pn.NewFromLibp2pHost[*testutil.Message]("mock", eh1, &MessageHandlerSelector{}, pn.SupportedProtocols(testutil.DefaultProtocols))
	pn1.Start(r1)
	t.Cleanup(pn1.Stop)
	if r1.listener != nil {
		eh1.Network().Notify(r1.listener)
	}

	// Host 2
	h2, err := mn.AddPeer(p2.PrivateKey(), p2.Address())
	if err != nil {
		t.Fatal(err)
	}
	eh2 := &ErrHost{Host: h2}
	pn2 := pn.NewFromLibp2pHost[*testutil.Message]("mock", eh2, &MessageHandlerSelector{}, pn.SupportedProtocols(testutil.DefaultProtocols))
	pn2.Start(r2)
	t.Cleanup(pn2.Stop)
	if r2.listener != nil {
		eh2.Network().Notify(r2.listener)
	}

	// Networking
	err = mn.LinkAll()
	if err != nil {
		t.Fatal(err)
	}
	err = pn1.ConnectTo(ctx, p2.ID())
	if err != nil {
		t.Fatal(err)
	}
	isConnected := <-r1.connectionEvent
	if !isConnected {
		t.Fatal("Expected connect event")
	}

	err = pn2.ConnectTo(ctx, p1.ID())
	if err != nil {
		t.Fatal(err)
	}

	return eh1, pn1, eh2, pn2, &testutil.Message{
		Id:      testutil.RandomBytes(100),
		Payload: testutil.RandomBytes(100),
	}
}

func TestMessageResendAfterError(t *testing.T) {
	testutil.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	p1 := tnet.RandIdentityOrFatal(t)
	r1 := newReceiver()
	p2 := tnet.RandIdentityOrFatal(t)
	r2 := newReceiver()

	eh, pn1, _, _, msg := prepareNetwork(t, ctx, p1, r1, p2, r2)

	testSendErrorBackoff := 100 * time.Millisecond
	ms, err := pn1.NewMessageSender(ctx, p2.ID(), &pn.MessageSenderOpts{
		MaxRetries:       3,
		SendTimeout:      100 * time.Millisecond,
		SendErrorBackoff: testSendErrorBackoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	// Return an error from the networking layer the next time we try to send
	// a message
	eh.setError(errMockNetErr)

	go func() {
		time.Sleep(testSendErrorBackoff / 2)
		// Stop throwing errors so that the following attempt to send succeeds
		eh.setError(nil)
	}()

	// Send message with retries, first one should fail, then subsequent
	// message should succeed
	err = ms.SendMsg(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
		t.Fatal("did not receive message sent")
	case <-r2.messageReceived:
	}
}

func TestMessageSendTimeout(t *testing.T) {
	testutil.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	p1 := tnet.RandIdentityOrFatal(t)
	r1 := newReceiver()
	p2 := tnet.RandIdentityOrFatal(t)
	r2 := newReceiver()

	eh, pn1, _, _, msg := prepareNetwork(t, ctx, p1, r1, p2, r2)

	ms, err := pn1.NewMessageSender(ctx, p2.ID(), &pn.MessageSenderOpts{
		MaxRetries:       3,
		SendTimeout:      100 * time.Millisecond,
		SendErrorBackoff: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	// Return a DeadlineExceeded error from the networking layer the next time we try to
	// send a message
	eh.setTimeoutState(true)

	// Send message with retries, all attempts should fail
	err = ms.SendMsg(ctx, msg)
	if err == nil {
		t.Fatal("Expected error from SednMsg")
	}

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive disconnect event")
	case isConnected := <-r1.connectionEvent:
		if isConnected {
			t.Fatal("Expected disconnect event (got connect event)")
		}
	}
}

func TestMessageSendNotSupportedResponse(t *testing.T) {
	testutil.Flaky(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	p1 := tnet.RandIdentityOrFatal(t)
	r1 := newReceiver()
	p2 := tnet.RandIdentityOrFatal(t)
	r2 := newReceiver()

	eh, pn1, _, _, _ := prepareNetwork(t, ctx, p1, r1, p2, r2)

	eh.setError(multistream.ErrNotSupported[protocol.ID]{})
	ms, err := pn1.NewMessageSender(ctx, p2.ID(), &pn.MessageSenderOpts{
		MaxRetries:       3,
		SendTimeout:      100 * time.Millisecond,
		SendErrorBackoff: 100 * time.Millisecond,
	})
	if err == nil {
		ms.Close()
		t.Fatal("Expected ErrNotSupported")
	}

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive disconnect event")
	case isConnected := <-r1.connectionEvent:
		if isConnected {
			t.Fatal("Expected disconnect event (got connect event)")
		}
	}
}

func testNetworkCounters(t *testing.T, n1 int, n2 int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p1 := tnet.RandIdentityOrFatal(t)
	r1 := newReceiver()
	p2 := tnet.RandIdentityOrFatal(t)
	r2 := newReceiver()

	h1, pn1, h2, pn2, msg := prepareNetwork(t, ctx, p1, r1, p2, r2)

	for n := 0; n < n1; n++ {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		err := pn1.SendMessage(ctx, p2.ID(), msg)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("p2 did not receive message sent")
		case <-r2.messageReceived:
			for j := 0; j < 2; j++ {
				err := pn2.SendMessage(ctx, p1.ID(), msg)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-ctx.Done():
					t.Fatal("p1 did not receive message sent")
				case <-r1.messageReceived:
				}
			}
		}
		cancel()
	}

	if n2 > 0 {
		ms, err := pn1.NewMessageSender(ctx, p2.ID(), &pn.MessageSenderOpts{})
		if err != nil {
			t.Fatal(err)
		}
		defer ms.Close()
		for n := 0; n < n2; n++ {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			err = ms.SendMsg(ctx, msg)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("p2 did not receive message sent")
			case <-r2.messageReceived:
				for j := 0; j < 2; j++ {
					err := pn2.SendMessage(ctx, p1.ID(), msg)
					if err != nil {
						t.Fatal(err)
					}
					select {
					case <-ctx.Done():
						t.Fatal("p1 did not receive message sent")
					case <-r1.messageReceived:
					}
				}
			}
			cancel()
		}
		ms.Close()
	}

	// Wait until all streams are closed and MessagesRecvd counters
	// updated.
	ctxto, cancelto := context.WithTimeout(ctx, 5*time.Second)
	defer cancelto()
	ctxwait, cancelwait := context.WithCancel(ctx)
	go func() {
		// Wait until all streams are closed
		throttler := time.NewTicker(time.Millisecond * 5)
		defer throttler.Stop()
		for {
			h1.lk.Lock()
			var done bool
			for _, s := range h1.streams {
				s.lk.Lock()
				closed := s.closed
				closed = closed || s.err != nil
				s.lk.Unlock()
				if closed {
					continue
				}
				pid := s.Protocol()
				for _, v := range testutil.DefaultProtocols {
					if pid == v {
						goto ElseH1
					}
				}
			}
			done = true
		ElseH1:
			h1.lk.Unlock()
			if done {
				break
			}
			select {
			case <-ctxto.Done():
				return
			case <-throttler.C:
			}
		}

		for {
			h2.lk.Lock()
			var done bool
			for _, s := range h2.streams {
				s.lk.Lock()
				closed := s.closed
				closed = closed || s.err != nil
				s.lk.Unlock()
				if closed {
					continue
				}
				pid := s.Protocol()
				for _, v := range testutil.DefaultProtocols {
					if pid == v {
						goto ElseH2
					}
				}
			}
			done = true
		ElseH2:
			h2.lk.Unlock()
			if done {
				break
			}
			select {
			case <-ctxto.Done():
				return
			case <-throttler.C:
			}
		}

		cancelwait()
	}()

	select {
	case <-ctxto.Done():
		t.Fatal("network streams closing timed out")
	case <-ctxwait.Done():
	}

	if pn1.Stats().MessagesSent != uint64(n1+n2) {
		t.Fatal(fmt.Errorf("expected %d sent messages, got %d", n1+n2, pn1.Stats().MessagesSent))
	}

	if pn2.Stats().MessagesRecvd != uint64(n1+n2) {
		t.Fatal(fmt.Errorf("expected %d received messages, got %d", n1+n2, pn2.Stats().MessagesRecvd))
	}

	if pn1.Stats().MessagesRecvd != 2*uint64(n1+n2) {
		t.Fatal(fmt.Errorf("expected %d received reply messages, got %d", 2*(n1+n2), pn1.Stats().MessagesRecvd))
	}
}

func TestNetworkCounters(t *testing.T) {
	testutil.Flaky(t)

	for n := 0; n < 11; n++ {
		testNetworkCounters(t, 10-n, n)
	}
}
