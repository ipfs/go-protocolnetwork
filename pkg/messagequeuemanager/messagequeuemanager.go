package messagequeuemanager

import (
	"context"

	"github.com/ipfs/go-protocolnetwork/pkg/peermanager"
	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageQueue is a queue for sending messages
type MessageQueue[BuildParams any] interface {
	Startup()
	Shutdown()
	BuildMessage(BuildParams)
}

// MessageQueueManager manages message queues for peers
type MessageQueueManager[BuildParams any] struct {
	*peermanager.PeerManager[MessageQueue[BuildParams]]
}

// MessageQueueFactory constructs a message queue
type MessageQueueFactory[BuildParams any] peermanager.PeerHandlerFactory[MessageQueue[BuildParams]]

// NewMessageQueueManager generates a new manger for sending messages
func NewMessageQueueManager[BuildParams any](ctx context.Context, createPeerQueue MessageQueueFactory[BuildParams]) *MessageQueueManager[BuildParams] {
	return &MessageQueueManager[BuildParams]{
		PeerManager: peermanager.New[MessageQueue[BuildParams]](
			ctx,
			peermanager.PeerHandlerFactory[MessageQueue[BuildParams]](createPeerQueue),
			peermanager.OnPeerAddedHook(func(mq MessageQueue[BuildParams]) {
				mq.Startup()
			}),
			peermanager.OnPeerRemovedHook(func(mq MessageQueue[BuildParams]) {
				mq.Shutdown()
			}),
		),
	}
}

// BuildMessage allows you to modify the next message that is sent for the given peer
// If blkSize > 0, message building may block until enough memory has been freed from the queues to allocate the message.
func (pmm *MessageQueueManager[BuildParams]) BuildMessage(p peer.ID, messageParams BuildParams) {
	pq := pmm.GetHandler(p)
	pq.BuildMessage(messageParams)
}
