package testnet

import (
	"github.com/ipfs/go-protocolnetwork/pkg/network"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Network is an interface for generating bitswap network interfaces
// based on a test network.
type Network[MessageType network.Message[MessageType]] interface {
	Adapter(tnet.Identity, ...network.NetOpt) network.ProtocolNetwork[MessageType]
	HasPeer(peer.ID) bool
}
