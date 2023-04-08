package protocolnetwork

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	msgio "github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multistream"
)

var connectTimeout = time.Second * 5

var maxSendTimeout = 2 * time.Minute
var minSendTimeout = 10 * time.Second
var sendLatency = 2 * time.Second
var minSendRate = (100 * 1000) / 8 // 100kbit/s

// NewFromLibp2pHost returns a BitSwapNetwork supported by underlying IPFS host.
func NewFromLibp2pHost[MessageType Message](
	protocolName string,
	host host.Host,
	messageHandlerSelector MessageHandlerSelector[MessageType],
	opts ...NetOpt) ProtocolNetwork[MessageType] {
	s := Settings{}
	for _, opt := range opts {
		opt(&s)
	}
	for i, proto := range s.SupportedProtocols {
		s.SupportedProtocols[i] = s.ProtocolPrefix + proto
	}

	return &libp2pProtocolNetwork[MessageType]{
		log:                    logging.Logger(protocolName + "_network"),
		protocolName:           protocolName,
		host:                   host,
		protocolPrefix:         s.ProtocolPrefix,
		supportedProtocols:     s.SupportedProtocols,
		messageHandlerSelector: messageHandlerSelector,
	}
}

// libp2pProtocolNetwork transforms the ipfs network interface, which sends and receives
// NetMessage objects, into the bitswap network interface.
type libp2pProtocolNetwork[MessageType Message] struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats Stats

	host           host.Host
	routing        routing.ContentRouting
	connectEvtMgr  *connectEventManager
	protocolName   string
	log            *logging.ZapEventLogger
	protocolPrefix protocol.ID

	supportedProtocols []protocol.ID

	messageHandlerSelector MessageHandlerSelector[MessageType]
	// inbound messages from the network are forwarded to the receiver
	receivers []Receiver[MessageType]
}

type streamMessageSender[MessageType Message] struct {
	to        peer.ID
	stream    network.Stream
	connected bool
	network   *libp2pProtocolNetwork[MessageType]

	opts *MessageSenderOpts
}

// Open a stream to the remote peer
func (s *streamMessageSender[MessageType]) Connect(ctx context.Context) (network.Stream, error) {
	if s.connected {
		return s.stream, nil
	}

	tctx, cancel := context.WithTimeout(ctx, s.opts.SendTimeout)
	defer cancel()

	if err := s.network.ConnectTo(tctx, s.to); err != nil {
		return nil, err
	}

	stream, err := s.network.newStreamToPeer(tctx, s.to)
	if err != nil {
		return nil, err
	}

	s.stream = stream
	s.connected = true
	return s.stream, nil
}

// Reset the stream
func (s *streamMessageSender[MessageType]) Reset() error {
	if s.stream != nil {
		err := s.stream.Reset()
		s.connected = false
		return err
	}
	return nil
}

// Close the stream
func (s *streamMessageSender[MessageType]) Close() error {
	return s.stream.Close()
}

// Indicates whether the peer supports HAVE / DONT_HAVE messages
func (s *streamMessageSender[MessageType]) Protocol() protocol.ID {
	return s.stream.Protocol()
}

// Send a message to the peer, attempting multiple times
func (s *streamMessageSender[MessageType]) SendMsg(ctx context.Context, msg MessageType) error {
	return s.multiAttempt(ctx, func() error {
		return s.send(ctx, msg)
	})
}

// Perform a function with multiple attempts, and a timeout
func (s *streamMessageSender[MessageType]) multiAttempt(ctx context.Context, fn func() error) error {
	// Try to call the function repeatedly
	var err error
	for i := 0; i < s.opts.MaxRetries; i++ {
		if err = fn(); err == nil {
			// Attempt was successful
			return nil
		}

		// Attempt failed

		// If the sender has been closed or the context cancelled, just bail out
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Protocol is not supported, so no need to try multiple times
		if errors.Is(err, multistream.ErrNotSupported[protocol.ID]{}) {
			s.network.connectEvtMgr.MarkUnresponsive(s.to)
			return err
		}

		// Failed to send so reset stream and try again
		_ = s.Reset()

		// Failed too many times so mark the peer as unresponsive and return an error
		if i == s.opts.MaxRetries-1 {
			s.network.connectEvtMgr.MarkUnresponsive(s.to)
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.opts.SendErrorBackoff):
			// wait a short time in case disconnect notifications are still propagating
			s.network.log.Infof("send message to %s failed but context was not Done: %s", s.to, err)
		}
	}
	return err
}

// Send a message to the peer
func (s *streamMessageSender[MessageType]) send(ctx context.Context, msg MessageType) error {
	start := time.Now()
	stream, err := s.Connect(ctx)
	if err != nil {
		s.network.log.Infof("failed to open stream to %s: %s", s.to, err)
		return err
	}

	// The send timeout includes the time required to connect
	// (although usually we will already have connected - we only need to
	// connect after a failed attempt to send)
	timeout := s.opts.SendTimeout - time.Since(start)
	if err = s.network.msgToStream(ctx, stream, msg, timeout); err != nil {
		s.network.log.Infof("failed to send message to %s: %s", s.to, err)
		return err
	}

	return nil
}

func (pn *libp2pProtocolNetwork[MessageType]) Self() peer.ID {
	return pn.host.ID()
}

func (pn *libp2pProtocolNetwork[MessageType]) Ping(ctx context.Context, p peer.ID) ping.Result {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	res := <-ping.Ping(ctx, pn.host, p)
	return res
}

func (pn *libp2pProtocolNetwork[MessageType]) Latency(p peer.ID) time.Duration {
	return pn.host.Peerstore().LatencyEWMA(p)
}

func (pn *libp2pProtocolNetwork[MessageType]) stripPrefix(proto protocol.ID) protocol.ID {
	return protocol.ID(strings.TrimPrefix(string(proto), string(pn.protocolPrefix)))
}

func (pn *libp2pProtocolNetwork[MessageType]) msgToStream(ctx context.Context, s network.Stream, msg MessageType, timeout time.Duration) error {

	msg.Log(pn.log, "outgoing")

	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	if err := s.SetWriteDeadline(deadline); err != nil {
		pn.log.Warnf("error setting deadline: %s", err)
	}

	if err := pn.messageHandlerSelector.Select(s.Protocol()).ToNet(s.Conn().RemotePeer(), msg, s); err != nil {
		pn.log.Debugf("error: %s", err)
		return err
	}

	atomic.AddUint64(&pn.stats.MessagesSent, 1)

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		pn.log.Warnf("error resetting deadline: %s", err)
	}
	return nil
}

func (pn *libp2pProtocolNetwork[MessageType]) NewMessageSender(ctx context.Context, p peer.ID, opts *MessageSenderOpts) (MessageSender[MessageType], error) {
	opts = setDefaultOpts(opts)

	sender := &streamMessageSender[MessageType]{
		to:      p,
		network: pn,
		opts:    opts,
	}

	err := sender.multiAttempt(ctx, func() error {
		_, err := sender.Connect(ctx)
		return err
	})

	if err != nil {
		return nil, err
	}

	return sender, nil
}

func setDefaultOpts(opts *MessageSenderOpts) *MessageSenderOpts {
	copy := *opts
	if opts.MaxRetries == 0 {
		copy.MaxRetries = 3
	}
	if opts.SendTimeout == 0 {
		copy.SendTimeout = maxSendTimeout
	}
	if opts.SendErrorBackoff == 0 {
		copy.SendErrorBackoff = 100 * time.Millisecond
	}
	return &copy
}

func (pn *libp2pProtocolNetwork[MessageType]) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing MessageType) error {

	tctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	s, err := pn.newStreamToPeer(tctx, p)
	if err != nil {
		return err
	}

	if err = pn.msgToStream(ctx, s, outgoing, outgoing.SendTimeout()); err != nil {
		_ = s.Reset()
		return err
	}

	return s.Close()
}

func (pn *libp2pProtocolNetwork[MessageType]) newStreamToPeer(ctx context.Context, p peer.ID) (network.Stream, error) {
	return pn.host.NewStream(ctx, p, pn.supportedProtocols...)
}

func (pn *libp2pProtocolNetwork[MessageType]) Start(r ...Receiver[MessageType]) {
	pn.receivers = r
	{
		connectionListeners := make([]ConnectionListener, len(r))
		for i, v := range r {
			connectionListeners[i] = v
		}
		pn.connectEvtMgr = newConnectEventManager(connectionListeners...)
	}
	for _, proto := range pn.supportedProtocols {
		pn.host.SetStreamHandler(proto, pn.handleNewStream)
	}
	pn.host.Network().Notify((*netNotifiee[MessageType])(pn))
	pn.connectEvtMgr.Start()

}

func (pn *libp2pProtocolNetwork[MessageType]) Stop() {
	pn.connectEvtMgr.Stop()
	pn.host.Network().StopNotify((*netNotifiee[MessageType])(pn))
}

func (pn *libp2pProtocolNetwork[MessageType]) ConnectTo(ctx context.Context, p peer.ID) error {
	return pn.host.Connect(ctx, peer.AddrInfo{ID: p})
}

func (pn *libp2pProtocolNetwork[MessageType]) DisconnectFrom(ctx context.Context, p peer.ID) error {
	return pn.host.Network().ClosePeer(p)
}

func (pn *libp2pProtocolNetwork[MessageType]) ConnectionManager() ConnManager {
	return pn.host.ConnManager()
}

// handleNewStream receives a new stream from the network.
func (pn *libp2pProtocolNetwork[MessageType]) handleNewStream(s network.Stream) {
	defer s.Close()

	if len(pn.receivers) == 0 {
		_ = s.Reset()
		return
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		received, err := pn.messageHandlerSelector.Select(pn.stripPrefix(s.Protocol())).FromMsgReader(s.Conn().RemotePeer(), reader)

		if err != nil {
			if err != io.EOF {
				_ = s.Reset()
				go func() {
					for _, v := range pn.receivers {
						v.ReceiveError(s.Conn().RemotePeer(), err)
					}
				}()
				pn.log.Debugf("bitswap net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		p := s.Conn().RemotePeer()
		ctx := context.Background()
		pn.log.Debugf("bitswap net handleNewStream from %s", s.Conn().RemotePeer())
		pn.connectEvtMgr.OnMessage(s.Conn().RemotePeer())
		atomic.AddUint64(&pn.stats.MessagesRecvd, 1)
		for _, v := range pn.receivers {
			v.ReceiveMessage(ctx, p, received)
		}
	}
}

func (bsnet *libp2pProtocolNetwork[MessageType]) Stats() Stats {
	return Stats{
		MessagesRecvd: atomic.LoadUint64(&bsnet.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&bsnet.stats.MessagesSent),
	}
}

type netNotifiee[MessageType Message] libp2pProtocolNetwork[MessageType]

func (nn *netNotifiee[MessageType]) impl() *libp2pProtocolNetwork[MessageType] {
	return (*libp2pProtocolNetwork[MessageType])(nn)
}

func (nn *netNotifiee[MessageType]) Connected(n network.Network, v network.Conn) {
	// ignore transient connections
	if v.Stat().Transient {
		return
	}

	nn.impl().connectEvtMgr.Connected(v.RemotePeer())
}
func (nn *netNotifiee[MessageType]) Disconnected(n network.Network, v network.Conn) {
	// Only record a "disconnect" when we actually disconnect.
	if n.Connectedness(v.RemotePeer()) == network.Connected {
		return
	}

	nn.impl().connectEvtMgr.Disconnected(v.RemotePeer())
}
func (nn *netNotifiee[MessageType]) OpenedStream(n network.Network, s network.Stream) {}
func (nn *netNotifiee[MessageType]) ClosedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee[MessageType]) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *netNotifiee[MessageType]) ListenClose(n network.Network, a ma.Multiaddr)    {}
