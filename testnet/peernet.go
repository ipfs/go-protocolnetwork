package testnet

import (
	"context"

	"github.com/ipfs/go-protocolnetwork"
	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	mockpeernet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

type peernet[MessageType protocolnetwork.Message[MessageType]] struct {
	mockpeernet.Mocknet
	protocolName       string
	supportedProtocols []protocol.ID
	handler            protocolnetwork.MessageHandlerSelector[MessageType]
}

// StreamNet is a testnet that uses libp2p's MockNet
func StreamNet[MessageType protocolnetwork.Message[MessageType]](
	ctx context.Context,
	protocolName string,
	supportedProtocols []protocol.ID,
	messageHandlerSelector protocolnetwork.MessageHandlerSelector[MessageType],
	net mockpeernet.Mocknet) (Network[MessageType], error) {
	return &peernet[MessageType]{net, protocolName, supportedProtocols, messageHandlerSelector}, nil
}

func (pn *peernet[MessageType]) Adapter(p tnet.Identity, opts ...protocolnetwork.NetOpt) protocolnetwork.ProtocolNetwork[MessageType] {
	client, err := pn.Mocknet.AddPeer(p.PrivateKey(), p.Address())
	if err != nil {
		panic(err.Error())
	}
	opts = append([]protocolnetwork.NetOpt{protocolnetwork.SupportedProtocols(pn.supportedProtocols)}, opts...)
	return protocolnetwork.NewFromLibp2pHost(pn.protocolName, client, pn.handler, opts...)
}

func (pn *peernet[MessageType]) HasPeer(p peer.ID) bool {
	for _, member := range pn.Mocknet.Peers() {
		if p == member {
			return true
		}
	}
	return false
}
