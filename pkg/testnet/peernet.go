package testnet

import (
	"context"

	"github.com/ipfs/go-protocolnetwork/pkg/network"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

type peernet[MessageType network.Message[MessageType]] struct {
	mockpeernet.Mocknet
	protocolName       string
	supportedProtocols []protocol.ID
	handler            network.MessageHandlerSelector[MessageType]
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet[MessageType network.Message[MessageType]](
	ctx context.Context,
	protocolName string,
	supportedProtocols []protocol.ID,
	messageHandlerSelector network.MessageHandlerSelector[MessageType],
	net mockpeernet.Mocknet) (Network[MessageType], error) {
	return &peernet[MessageType]{net, protocolName, supportedProtocols, messageHandlerSelector}, nil
}

func (pn *peernet[MessageType]) Adapter(p tnet.Identity, opts ...network.NetOpt) network.ProtocolNetwork[MessageType] {
	client, err := pn.Mocknet.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	opts = append([]network.NetOpt{network.SupportedProtocols(pn.supportedProtocols)}, opts...)
	return network.NewFromLibp2pHost(pn.protocolName, client, pn.handler, opts...)
}

func (pn *peernet[MessageType]) HasPeer(p peer.ID) bool {
	for _, member := range pn.Mocknet.Peers() {
		if p == member {
			return true
		}
	}
	return false
}
