package testnet

import (
	"github.com/ipfs/go-protocolnetwork"

	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
type Network[MessageType protocolnetwork.Message[MessageType]] interface {
	Adapter(tnet.Identity, ...protocolnetwork.NetOpt) protocolnetwork.ProtocolNetwork[MessageType]
	HasPeer(peer.ID) bool
}
