package peermanager

import (
	"context"

	gsmsg "github.com/ipfs/go-graphsync/message"
	peer "github.com/libp2p/go-libp2p-peer"
)

// PeerQueue is a process that sends messages to a peer
type PeerQueue interface {
	PeerProcess
	AddRequest(graphSyncRequest gsmsg.GraphSyncRequest)
}

// PeerQueueFactory provides a function that will create a PeerQueue.
type PeerQueueFactory func(ctx context.Context, p peer.ID) PeerQueue

// PeerMessageManager manages message queues for peers
type PeerMessageManager struct {
	*PeerManager
}

// NewMessageManager generates a new manger for sending messages
func NewMessageManager(ctx context.Context, createPeerQueue PeerQueueFactory) *PeerMessageManager {
	return &PeerMessageManager{
		PeerManager: New(ctx, func(ctx context.Context, p peer.ID) PeerProcess {
			return createPeerQueue(ctx, p)
		}),
	}
}

// SendRequest sends the given GraphSyncRequest to the given peer
func (pmm *PeerMessageManager) SendRequest(p peer.ID, request gsmsg.GraphSyncRequest) {
	pq := pmm.GetProcess(p).(PeerQueue)
	pq.AddRequest(request)
}
