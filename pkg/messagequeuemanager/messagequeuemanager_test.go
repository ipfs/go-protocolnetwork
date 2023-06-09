package messagequeuemanager_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-protocolnetwork/internal/testutil"
	"github.com/ipfs/go-protocolnetwork/pkg/messagequeuemanager"
)

type messageSent struct {
	p       peer.ID
	message *testutil.Message
}

var _ messagequeuemanager.MessageQueue[func(*testutil.SingleBuilder)] = (*fakePeer)(nil)

type fakePeer struct {
	p            peer.ID
	messagesSent chan messageSent
	onShutdown   func(peer.ID)
}

func (fp *fakePeer) BuildMessage(buildmessagefn func(b *testutil.SingleBuilder)) {
	builder := &testutil.SingleBuilder{}
	buildmessagefn(builder)
	message, _, err := builder.Build()
	if err != nil {
		panic(err)
	}

	fp.messagesSent <- messageSent{fp.p, message}
}

func (fp *fakePeer) Startup()  {}
func (fp *fakePeer) Shutdown() {}

//func (fp *fakePeer) AddRequest(graphSyncRequest gsmsg.GraphSyncRequest, notifees ...notifications.Notifee) {
//	message := gsmsg.New()
//	message.AddRequest(graphSyncRequest)
//	fp.messagesSent <- messageSent{fp.p, message}
//}

func makePeerQueueFactory(messagesSent chan messageSent) messagequeuemanager.MessageQueueFactory[func(*testutil.SingleBuilder)] {
	return func(ctx context.Context, p peer.ID, onShutdown func(peer.ID)) messagequeuemanager.MessageQueue[func(*testutil.SingleBuilder)] {
		return &fakePeer{
			p:            p,
			messagesSent: messagesSent,
			onShutdown:   onShutdown,
		}
	}
}

func TestSendingMessagesToPeers(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	messagesSent := make(chan messageSent, 5)
	peerQueueFactory := makePeerQueueFactory(messagesSent)

	tp := testutil.GeneratePeers(5)

	peerManager := messagequeuemanager.NewMessageQueueManager(ctx, peerQueueFactory)

	id := testutil.RandomBytes(100)
	payload := testutil.RandomBytes(100)

	peerManager.BuildMessage(tp[0], func(b *testutil.SingleBuilder) {
		b.SetID(id)
		b.SetPayload(payload)
	})
	peerManager.BuildMessage(tp[1], func(b *testutil.SingleBuilder) {
		b.SetID(id)
		b.SetPayload(payload)
	})

	payload2 := testutil.RandomBytes(100)
	peerManager.BuildMessage(tp[0], func(b *testutil.SingleBuilder) {
		b.SetID(id)
		b.SetPayload(payload2)
	})

	var firstMessage messageSent
	testutil.AssertReceive(ctx, t, messagesSent, &firstMessage, "first message did not send")
	require.Equal(t, tp[0], firstMessage.p, "first message sent to incorrect peer")
	require.Equal(t, &testutil.Message{
		Id:      id,
		Payload: payload,
	}, firstMessage.message)

	var secondMessage messageSent
	testutil.AssertReceive(ctx, t, messagesSent, &secondMessage, "second message did not send")
	require.Equal(t, tp[1], secondMessage.p, "second message sent to incorrect peer")
	require.Equal(t, &testutil.Message{
		Id:      id,
		Payload: payload,
	}, secondMessage.message)

	var thirdMessage messageSent
	testutil.AssertReceive(ctx, t, messagesSent, &thirdMessage, "third message did not send")

	require.Equal(t, &testutil.Message{
		Id:      id,
		Payload: payload2,
	}, thirdMessage.message)

	connectedPeers := peerManager.ConnectedPeers()
	require.Len(t, connectedPeers, 2)

	testutil.AssertContainsPeer(t, connectedPeers, tp[0])
	testutil.AssertContainsPeer(t, connectedPeers, tp[1])
}
