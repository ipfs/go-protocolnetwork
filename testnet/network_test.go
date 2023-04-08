package testnet

import (
	"context"
	"sync"
	"testing"

	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-protocolnetwork"
	"github.com/ipfs/go-protocolnetwork/internal/testutil"
	"github.com/stretchr/testify/require"

	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSendMessageAsyncButWaitForResponse(t *testing.T) {
	net := VirtualNetwork[*testutil.Message](delay.Fixed(0), testutil.DefaultProtocols, &testutil.IPLDMessageHandler{})
	responderPeer := tnet.RandIdentityOrFatal(t)
	waiter := net.Adapter(tnet.RandIdentityOrFatal(t))
	responder := net.Adapter(responderPeer)

	var wg sync.WaitGroup

	wg.Add(1)

	expectedStr := "received async"

	responder.Start(lambda(func(
		ctx context.Context,
		fromWaiter peer.ID,
		msgFromWaiter *testutil.Message) {

		msgToWaiter := &testutil.Message{
			Id:      []byte(expectedStr),
			Payload: testutil.RandomBytes(100),
		}
		err := waiter.SendMessage(ctx, fromWaiter, msgToWaiter)
		if err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(responder.Stop)

	waiter.Start(lambda(func(
		ctx context.Context,
		fromResponder peer.ID,
		msgFromResponder *testutil.Message) {

		// TODO assert that this came from the correct peer and that the message contents are as expected
		require.Equal(t, expectedStr, string(msgFromResponder.Id))
		wg.Done()
	}))
	t.Cleanup(waiter.Stop)

	messageSentAsync := &testutil.Message{
		Id:      []byte(expectedStr),
		Payload: testutil.RandomBytes(100),
	}
	errSending := waiter.SendMessage(
		context.Background(), responderPeer.ID(), messageSentAsync)
	require.NoError(t, errSending)

	wg.Wait() // until waiter delegate function is executed
}

type receiverFunc func(ctx context.Context, p peer.ID,
	incoming *testutil.Message)

// lambda returns a Receiver instance given a receiver function
func lambda(f receiverFunc) protocolnetwork.Receiver[*testutil.Message] {
	return &lambdaImpl{
		f: f,
	}
}

type lambdaImpl struct {
	f func(ctx context.Context, p peer.ID, incoming *testutil.Message)
}

func (lam *lambdaImpl) ReceiveMessage(ctx context.Context,
	p peer.ID, incoming *testutil.Message) {
	lam.f(ctx, p, incoming)
}

func (lam *lambdaImpl) ReceiveError(p peer.ID, err error) {
	// TODO log error
}

func (lam *lambdaImpl) PeerConnected(p peer.ID) {
	// TODO
}
func (lam *lambdaImpl) PeerDisconnected(peer.ID) {
	// TODO
}
