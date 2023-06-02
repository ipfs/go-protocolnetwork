package network

import (
	"context"
	"io"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"

	msgio "github.com/libp2p/go-msgio"
)

type Message[MessageType any] interface {
	Log(logger *logging.ZapEventLogger, eventDescription string)
	SendTimeout() time.Duration
	Clone() MessageType
}

// ProtocolNetwork provides network connectivity for BitSwap sessions.
type ProtocolNetwork[MessageType Message[MessageType]] interface {

	// SendMessage sends a BitSwap message to a peer.
	SendMessage(
		context.Context,
		peer.ID,
		MessageType) error

	// Start registers the Reciver and starts handling new messages, connectivity events, etc.
	Start(...Receiver[MessageType])
	// Stop stops the network service.
	Stop()

	ConnectTo(context.Context, peer.ID) error
	DisconnectFrom(context.Context, peer.ID) error

	NewMessageSender(context.Context, peer.ID, *MessageSenderOpts) (MessageSender[MessageType], error)

	Stats() Stats
	ConnectionManager() ConnManager
}

// ConnManager provides the methods needed to protect and unprotect connections
type ConnManager interface {
	TagPeer(peer.ID, string, int)
	UntagPeer(p peer.ID, tag string)
	Protect(peer.ID, string)
	Unprotect(peer.ID, string) bool
}

type MessageHandlerSelector[MessageType Message[MessageType]] interface {
	Select(protocol protocol.ID) MessageHandler[MessageType]
}

// MessageHandler provides a consistent interface for maintaining per-peer state
// within the differnet protocol versions
type MessageHandler[MessageType Message[MessageType]] interface {
	FromNet(peer.ID, io.Reader) (MessageType, error)
	FromMsgReader(peer.ID, msgio.Reader) (MessageType, error)
	ToNet(peer.ID, MessageType, io.Writer) error
}

// MessageSender is an interface for sending a series of messages over the bitswap
// network
type MessageSender[MessageType Message[MessageType]] interface {
	SendMsg(context.Context, MessageType) error
	Close() error
	Reset() error
	Protocol() protocol.ID
}

type MessageSenderOpts struct {
	MaxRetries       int
	SendTimeout      time.Duration
	SendErrorBackoff time.Duration
}

// Receiver is an interface that can receive messages from the BitSwapNetwork.
type Receiver[MessageType Message[MessageType]] interface {
	ReceiveMessage(
		ctx context.Context,
		sender peer.ID,
		incoming MessageType)

	ReceiveError(peer.ID, error)

	// Connected/Disconnected warns bitswap about peer connections.
	PeerConnected(peer.ID)
	PeerDisconnected(peer.ID)
}

// Pinger is an interface to ping a peer and get the average latency of all pings
type Pinger interface {
	// Ping a peer
	Ping(context.Context, peer.ID) ping.Result
	// Get the average latency of all pings
	Latency(peer.ID) time.Duration
}

// Stats is a container for statistics about the bitswap network
// the numbers inside are specific to bitswap, and not any other protocols
// using the same underlying network.
type Stats struct {
	MessagesSent  uint64
	MessagesRecvd uint64
}
