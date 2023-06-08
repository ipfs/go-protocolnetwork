package messagequeue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-protocolnetwork/internal/testutil"
	"github.com/ipfs/go-protocolnetwork/pkg/messagequeue"
	"github.com/ipfs/go-protocolnetwork/pkg/network"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
)

func TestStartupAndShutdown(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan *testutil.Message)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	bc := testutil.NewBuilderCollection()

	messageQueue := messagequeue.New[*testutil.Message, *testutil.Builder, int](ctx, peer, messageNetwork, bc, messageSenderOpts, nil)
	messageQueue.Startup()

	id := testutil.RandomBytes(100)
	payload := testutil.RandomBytes(100)
	waitGroup.Add(1)
	messageQueue.AllocateAndBuildMessage(0, func(b *testutil.Builder) {
		b.SetID(id)
		b.SetPayload(payload)
	})

	testutil.AssertDoesReceive(ctx, t, messagesSent, "message was not sent")

	messageQueue.Shutdown()

	testutil.AssertDoesReceiveFirst(t, resetChan, "message sender should be closed", fullClosedChan, ctx.Done())
}

func TestShutdownDuringMessageSend(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan *testutil.Message)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{
		fmt.Errorf("Something went wrong"),
		fullClosedChan,
		resetChan,
		messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	bc := testutil.NewBuilderCollection()

	messageQueue := messagequeue.New[*testutil.Message, *testutil.Builder, int](ctx, peer, messageNetwork, bc, messageSenderOpts, nil)
	messageQueue.Startup()
	id := testutil.RandomBytes(100)
	payload := testutil.RandomBytes(100)

	// setup a message and advance as far as beginning to send it
	waitGroup.Add(1)
	messageQueue.AllocateAndBuildMessage(0, func(b *testutil.Builder) {
		b.SetID(id)
		b.SetPayload(payload)
	})
	waitGroup.Wait()

	// now shut down
	messageQueue.Shutdown()

	// let the message send attempt complete and fail (as it would if
	// the connection were closed)
	testutil.AssertDoesReceive(ctx, t, messagesSent, "message send not attempted")

	// verify the connection is reset after a failed send attempt
	testutil.AssertDoesReceiveFirst(t, resetChan, "message sender was not reset", fullClosedChan, ctx.Done())

	// now verify after it's reset, no further retries, connection
	// resets, or attempts to close the connection, cause the queue
	// should realize it's shut down and stop processing
	// FIXME: this relies on time passing -- 100 ms to be exact
	// and we should instead mock out time as a dependency
	waitGroup.Add(1)
	testutil.AssertDoesReceiveFirst(t, ctx.Done(), "further message operations should not occur", messagesSent, resetChan, fullClosedChan)
}

func TestProcessingNotification(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	peer := testutil.GeneratePeers(1)[0]
	messagesSent := make(chan *testutil.Message)
	resetChan := make(chan struct{}, 1)
	fullClosedChan := make(chan struct{}, 1)
	messageSender := &fakeMessageSender{nil, fullClosedChan, resetChan, messagesSent}
	var waitGroup sync.WaitGroup
	messageNetwork := &fakeMessageNetwork{nil, nil, messageSender, &waitGroup}
	bc := testutil.NewBuilderCollection()

	messageQueue := messagequeue.New[*testutil.Message, *testutil.Builder, int](ctx, peer, messageNetwork, bc, messageSenderOpts, nil)
	messageQueue.Startup()
	id := testutil.RandomBytes(100)
	payload := testutil.RandomBytes(100)
	waitGroup.Add(1)

	messageQueue.AllocateAndBuildMessage(0, func(b *testutil.Builder) {
		b.SetID(id)
		b.SetPayload(payload)
	})

	// wait for send attempt
	waitGroup.Wait()

	var message *testutil.Message
	testutil.AssertReceive(ctx, t, messagesSent, &message, "message did not send")

	require.Equal(t, id, message.Id)
	require.Equal(t, payload, message.Payload)

	notifier := bc.Notifier(id)
	notifier.ExpectHandleQueued(ctx, t)
	notifier.ExpectHandleSent(ctx, t)
	notifier.ExpectHandleFinished(ctx, t)
}

const sendMessageTimeout = 10 * time.Minute
const sendErrorBackoff = 100 * time.Millisecond
const messageSendRetries = 10

var messageSenderOpts = &network.MessageSenderOpts{
	MaxRetries:       messageSendRetries,
	SendTimeout:      sendMessageTimeout,
	SendErrorBackoff: sendErrorBackoff,
}

var _ messagequeue.MessageNetwork[*testutil.Message] = &fakeMessageNetwork{}

type fakeMessageNetwork struct {
	connectError       error
	messageSenderError error
	messageSender      network.MessageSender[*testutil.Message]
	wait               *sync.WaitGroup
}

func (fmn *fakeMessageNetwork) ConnectTo(context.Context, peer.ID) error {
	return fmn.connectError
}

func (fmn *fakeMessageNetwork) NewMessageSender(context.Context, peer.ID, *network.MessageSenderOpts) (network.MessageSender[*testutil.Message], error) {
	fmn.wait.Done()
	if fmn.messageSenderError == nil {
		return fmn.messageSender, nil
	}
	return nil, fmn.messageSenderError
}

var _ network.MessageSender[*testutil.Message] = (*fakeMessageSender)(nil)

type fakeMessageSender struct {
	sendError    error
	fullClosed   chan<- struct{}
	reset        chan<- struct{}
	messagesSent chan<- *testutil.Message
}

func (fms *fakeMessageSender) SendMsg(ctx context.Context, msg *testutil.Message) error {
	fms.messagesSent <- msg
	return fms.sendError
}
func (fms *fakeMessageSender) Close() error { fms.fullClosed <- struct{}{}; return nil }
func (fms *fakeMessageSender) Reset() error { fms.reset <- struct{}{}; return nil }
func (fms *fakeMessageSender) Protocol() protocol.ID {
	return "mock"
}

type fakeCloser struct {
	fms    *fakeMessageSender
	closed bool
}

func (fc *fakeCloser) Close() error {
	fc.closed = true
	// clear error so the next send goes through
	fc.fms.sendError = nil
	return nil
}
