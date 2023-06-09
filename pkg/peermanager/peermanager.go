package peermanager

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerHandlerFactory provides a function that will create a PeerHandler.
type PeerHandlerFactory[PeerHandler any] func(ctx context.Context, p peer.ID, onShutdown func(peer.ID)) PeerHandler

// PeerAddedHook is called any time a new peer handler is created
type PeerAddedHook[PeerHandler any] func(PeerHandler)

// PeerRemovedHook is called when a peer handler will no longer be tracked
type PeerRemovedHook[PeerHandler any] func(PeerHandler)

// PeerManager manages a pool of handlers on behalf of connected peers
type PeerManager[PeerHandler any] struct {
	peerHandlers   map[peer.ID]PeerHandler
	peerHandlersLk sync.RWMutex

	createPeerHandler PeerHandlerFactory[PeerHandler]
	onPeerAdded       PeerAddedHook[PeerHandler]
	onPeerRemoved     PeerRemovedHook[PeerHandler]
	ctx               context.Context
}

// Option configures the PeerManager
type Option[PeerHandler any] func(*PeerManager[PeerHandler])

// OnPeerAddedHook specifies a hook to run when a peer handler is created
func OnPeerAddedHook[PeerHandler any](onPeerAdded PeerAddedHook[PeerHandler]) Option[PeerHandler] {
	return func(pm *PeerManager[PeerHandler]) {
		pm.onPeerAdded = onPeerAdded
	}
}

// OnPeerRemovedHook specifies a hook to run when a peer handler is created
func OnPeerRemovedHook[PeerHandler any](onPeerRemoved PeerRemovedHook[PeerHandler]) Option[PeerHandler] {
	return func(pm *PeerManager[PeerHandler]) {
		pm.onPeerRemoved = onPeerRemoved
	}
}

// New creates a new PeerManager, given a context and a PeerHandlerFactory.
func New[PeerHandler any](ctx context.Context, createPeerHandler PeerHandlerFactory[PeerHandler], options ...Option[PeerHandler]) *PeerManager[PeerHandler] {
	pm := &PeerManager[PeerHandler]{
		peerHandlers:      make(map[peer.ID]PeerHandler),
		createPeerHandler: createPeerHandler,
		ctx:               ctx,
	}
	for _, option := range options {
		option(pm)
	}
	return pm
}

// ConnectedPeers returns a list of peers this PeerManager is managing.
func (pm *PeerManager[PeerHandler]) ConnectedPeers() []peer.ID {
	pm.peerHandlersLk.RLock()
	defer pm.peerHandlersLk.RUnlock()
	peers := make([]peer.ID, 0, len(pm.peerHandlers))
	for p := range pm.peerHandlers {
		peers = append(peers, p)
	}
	return peers
}

// Connected is called to add a new peer to the pool
func (pm *PeerManager[PeerHandler]) Connected(p peer.ID) {
	pm.peerHandlersLk.Lock()
	defer pm.peerHandlersLk.Unlock()
	pm.getOrCreate(p)
}

// Disconnected is called to remove a peer from the pool.
func (pm *PeerManager[PeerHandler]) Disconnected(p peer.ID) {
	pm.peerHandlersLk.Lock()
	ph, ok := pm.peerHandlers[p]
	if !ok {
		pm.peerHandlersLk.Unlock()
		return
	}

	delete(pm.peerHandlers, p)
	pm.peerHandlersLk.Unlock()

	if pm.onPeerRemoved != nil {
		pm.onPeerRemoved(ph)
	}
}

// GetHandler returns the process for the given peer
func (pm *PeerManager[PeerHandler]) GetHandler(
	p peer.ID) PeerHandler {
	// Usually this this is just a read
	pm.peerHandlersLk.RLock()
	ph, ok := pm.peerHandlers[p]
	if ok {
		pm.peerHandlersLk.RUnlock()
		return ph
	}
	pm.peerHandlersLk.RUnlock()
	// but sometimes it involves a create (we still need to do get or create cause it's possible
	// another writer grabbed the Lock first and made the process)
	pm.peerHandlersLk.Lock()
	ph = pm.getOrCreate(p)
	pm.peerHandlersLk.Unlock()
	return ph
}

func (pm *PeerManager[PeerHandler]) getOrCreate(p peer.ID) PeerHandler {
	pqi, ok := pm.peerHandlers[p]
	if !ok {
		pqi = pm.createPeerHandler(pm.ctx, p, pm.onQueueShutdown)
		if pm.onPeerAdded != nil {
			pm.onPeerAdded(pqi)
		}
		pm.peerHandlers[p] = pqi
	}
	return pqi
}

func (pm *PeerManager[PeerHandler]) onQueueShutdown(p peer.ID) {
	pm.peerHandlersLk.Lock()
	defer pm.peerHandlersLk.Unlock()
	delete(pm.peerHandlers, p)
}
