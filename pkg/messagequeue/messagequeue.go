package messagequeue

import (
	"context"
	"errors"
	"fmt"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipfs/go-protocolnetwork/pkg/network"
)

var log = logging.Logger("protocolnetwork/messagequeue")

// MessageNetwork is any network that can connect peers and generate a message
// sender.
type MessageNetwork[MessageType network.Message[MessageType]] interface {
	NewMessageSender(context.Context, peer.ID, *network.MessageSenderOpts) (network.MessageSender[MessageType], error)
	ConnectTo(context.Context, peer.ID) error
}

type Notifier interface {
	HandleQueued()
	HandleError(error)
	HandleSent()
	HandleFinished()
}

type Builder[MessageType network.Message[MessageType]] interface {
	Build() (MessageType, Notifier, error)
	Empty() bool
}

type BuilderCollection[MessageType network.Message[MessageType], BuilderType Builder[MessageType], EstimateType any] interface {
	NextBuilder(EstimateType) BuilderType
	ExtractFirstBuilder() BuilderType
	Empty() bool
}

// MessageQueue implements queue of want messages to send to peers.
type MessageQueue[MessageType network.Message[MessageType], BuilderType Builder[MessageType], EstimateType any] struct {
	p       peer.ID
	network MessageNetwork[MessageType]
	ctx     context.Context

	outgoingWork chan struct{}
	done         chan struct{}
	doneOnce     sync.Once

	// internal do not touch outside go routines
	sender     network.MessageSender[MessageType]
	buildersLk sync.RWMutex
	builders   BuilderCollection[MessageType, BuilderType, EstimateType]
	onShutdown func()
	opts       *network.MessageSenderOpts
}

// New creats a new MessageQueue.
func New[MessageType network.Message[MessageType], BuilderType Builder[MessageType], EstimateType any](
	ctx context.Context,
	p peer.ID,
	network MessageNetwork[MessageType],
	builders BuilderCollection[MessageType, BuilderType, EstimateType],
	opts *network.MessageSenderOpts,
	onShutdown func()) *MessageQueue[MessageType, BuilderType, EstimateType] {
	return &MessageQueue[MessageType, BuilderType, EstimateType]{
		ctx:          ctx,
		network:      network,
		p:            p,
		builders:     builders,
		outgoingWork: make(chan struct{}, 1),
		done:         make(chan struct{}),
		opts:         opts,
		onShutdown:   onShutdown,
	}
}

// AllocateAndBuildMessage allows you to work modify the next message that is sent in the queue.
// If blkSize > 0, message building may block until enough memory has been freed from the queues to allocate the message.
func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) AllocateAndBuildMessage(size EstimateType, buildMessageFn func(BuilderType)) {
	if mq.buildMessage(size, buildMessageFn) {
		mq.signalWork()
	}
}

func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) buildMessage(size EstimateType, buildMessageFn func(BuilderType)) bool {
	mq.buildersLk.Lock()
	defer mq.buildersLk.Unlock()
	builder := mq.builders.NextBuilder(size)
	buildMessageFn(builder)
	return !builder.Empty()
}

// Startup starts the processing of messages, and creates an initial message
// based on the given initial wantlist.
func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) Startup() {
	go mq.runQueue()
}

// Shutdown stops the processing of messages for a message queue.
func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) Shutdown() {
	mq.doneOnce.Do(func() {
		close(mq.done)
	})
}

func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) runQueue() {
	defer func() {
		if mq.onShutdown != nil {
			mq.onShutdown()
		}
	}()
	for {
		select {
		case <-mq.outgoingWork:
			mq.sendMessage()
		case <-mq.done:
			select {
			case <-mq.outgoingWork:
				for {
					_, notifier, err := mq.extractOutgoingMessage()
					if err == nil {
						notifier.HandleError(err)
						notifier.HandleFinished()
					} else {
						break
					}
				}
			default:
			}
			if mq.sender != nil {
				mq.sender.Reset()
			}
			return
		case <-mq.ctx.Done():
			if mq.sender != nil {
				_ = mq.sender.Reset()
			}
			return
		}
	}
}

func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) signalWork() {
	select {
	case mq.outgoingWork <- struct{}{}:
	default:
	}
}

var errEmptyMessage = errors.New("empty Message")

func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) extractOutgoingMessage() (MessageType, Notifier, error) {
	// grab outgoing message
	mq.buildersLk.Lock()
	if mq.builders.Empty() {
		mq.buildersLk.Unlock()
		var emptyMessage MessageType
		return emptyMessage, nil, errEmptyMessage
	}
	builder := mq.builders.ExtractFirstBuilder()
	// if there are more queued messages, signal we still have more work
	if !mq.builders.Empty() {
		select {
		case mq.outgoingWork <- struct{}{}:
		default:
		}
	}
	mq.buildersLk.Unlock()
	if builder.Empty() {
		var emptyMessage MessageType
		return emptyMessage, nil, errEmptyMessage
	}
	return builder.Build()
}

func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) sendMessage() {
	message, notifier, err := mq.extractOutgoingMessage()

	if err != nil {
		if err != errEmptyMessage {
			log.Errorf("Unable to assemble GraphSync message: %s", err.Error())
		}
		return
	}
	notifier.HandleQueued()
	defer notifier.HandleFinished()

	err = mq.initializeSender()
	if err != nil {
		log.Infof("cant open message sender to peer %s: %s", mq.p, err)
		// TODO: cant connect, what now?
		notifier.HandleError(fmt.Errorf("cant open message sender to peer %s: %w", mq.p, err))
		mq.Shutdown()
		return
	}

	if err = mq.sender.SendMsg(mq.ctx, message); err != nil {
		// If the message couldn't be sent, the networking layer will
		// emit a Disconnect event and the MessageQueue will get cleaned up
		log.Infof("Could not send message to peer %s: %s", mq.p, err)
		notifier.HandleError(fmt.Errorf("expended retries on SendMsg(%s)", mq.p))
		mq.Shutdown()
		return
	}

	notifier.HandleSent()
}

func (mq *MessageQueue[MessageType, BuilderType, EstimateType]) initializeSender() error {
	if mq.sender != nil {
		return nil
	}
	nsender, err := mq.network.NewMessageSender(mq.ctx, mq.p, mq.opts)
	if err != nil {
		return err
	}
	mq.sender = nsender
	return nil
}
