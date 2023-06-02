package testnet

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	delay "github.com/ipfs/go-ipfs-delay"
	"github.com/ipfs/go-protocolnetwork/pkg/network"

	tnet "github.com/libp2p/go-libp2p-testing/net"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

// VirtualNetwork generates a new testnet instance - a fake network that
// is used to simulate sending messages.
func VirtualNetwork[MessageType network.Message[MessageType]](
	d delay.D,
	supportedProtocols []protocol.ID,
	handler network.MessageHandler[MessageType],
) Network[MessageType] {
	return &virtualnetwork[MessageType]{
		latencies:          make(map[peer.ID]map[peer.ID]time.Duration),
		clients:            make(map[peer.ID]*receiverQueue[MessageType]),
		delay:              d,
		isRateLimited:      false,
		rateLimitGenerator: nil,
		conns:              make(map[string]struct{}),
		supportedProtocols: supportedProtocols,
		handler:            handler,
	}
}

// RateLimitGenerator is an interface for generating rate limits across peers
type RateLimitGenerator interface {
	NextRateLimit() float64
}

// RateLimitedVirtualNetwork generates a testnet instance where nodes are rate
// limited in the upload/download speed.
func RateLimitedVirtualNetwork[MessageType network.Message[MessageType]](
	d delay.D,
	rateLimitGenerator RateLimitGenerator,
	supportedProtocols []protocol.ID,
	handler network.MessageHandler[MessageType],
) Network[MessageType] {
	return &virtualnetwork[MessageType]{
		latencies:          make(map[peer.ID]map[peer.ID]time.Duration),
		rateLimiters:       make(map[peer.ID]map[peer.ID]*mocknet.RateLimiter),
		clients:            make(map[peer.ID]*receiverQueue[MessageType]),
		delay:              d,
		isRateLimited:      true,
		rateLimitGenerator: rateLimitGenerator,
		conns:              make(map[string]struct{}),
		supportedProtocols: supportedProtocols,
		handler:            handler,
	}
}

type virtualnetwork[MessageType network.Message[MessageType]] struct {
	mu                 sync.Mutex
	latencies          map[peer.ID]map[peer.ID]time.Duration
	rateLimiters       map[peer.ID]map[peer.ID]*mocknet.RateLimiter
	clients            map[peer.ID]*receiverQueue[MessageType]
	delay              delay.D
	isRateLimited      bool
	rateLimitGenerator RateLimitGenerator
	conns              map[string]struct{}
	supportedProtocols []protocol.ID
	handler            network.MessageHandler[MessageType]
}

type message[MessageType network.Message[MessageType]] struct {
	from       peer.ID
	msg        MessageType
	shouldSend time.Time
}

// receiverQueue queues up a set of messages to be sent, and sends them *in
// order* with their delays respected as much as sending them in order allows
// for
type receiverQueue[MessageType network.Message[MessageType]] struct {
	receiver *networkClient[MessageType]
	queue    []*message[MessageType]
	active   bool
	lk       sync.Mutex
}

func (n *virtualnetwork[MessageType]) Adapter(p tnet.Identity, opts ...network.NetOpt) network.ProtocolNetwork[MessageType] {
	n.mu.Lock()
	defer n.mu.Unlock()

	s := network.Settings{
		SupportedProtocols: n.supportedProtocols,
	}
	for _, opt := range opts {
		opt(&s)
	}

	client := &networkClient[MessageType]{
		local:              p.ID(),
		network:            n,
		supportedProtocols: s.SupportedProtocols,
	}
	n.clients[p.ID()] = &receiverQueue[MessageType]{receiver: client}
	return client
}

func (n *virtualnetwork[MessageType]) HasPeer(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, found := n.clients[p]
	return found
}

// TODO should this be completely asynchronous?
// TODO what does the network layer do with errors received from services?
func (n *virtualnetwork[MessageType]) SendMessage(
	ctx context.Context,
	from peer.ID,
	to peer.ID,
	mes MessageType) error {

	mes = mes.Clone()

	n.mu.Lock()
	defer n.mu.Unlock()

	latencies, ok := n.latencies[from]
	if !ok {
		latencies = make(map[peer.ID]time.Duration)
		n.latencies[from] = latencies
	}

	latency, ok := latencies[to]
	if !ok {
		latency = n.delay.NextWaitTime()
		latencies[to] = latency
	}

	var bandwidthDelay time.Duration
	if n.isRateLimited {
		rateLimiters, ok := n.rateLimiters[from]
		if !ok {
			rateLimiters = make(map[peer.ID]*mocknet.RateLimiter)
			n.rateLimiters[from] = rateLimiters
		}

		rateLimiter, ok := rateLimiters[to]
		if !ok {
			rateLimiter = mocknet.NewRateLimiter(n.rateLimitGenerator.NextRateLimit())
			rateLimiters[to] = rateLimiter
		}

		buf := new(bytes.Buffer)
		err := n.handler.ToNet(peer.ID("foo"), mes, buf)
		if err != nil {
			return err
		}
		size := buf.Len()
		bandwidthDelay = rateLimiter.Limit(size)
	} else {
		bandwidthDelay = 0
	}

	receiver, ok := n.clients[to]
	if !ok {
		return errors.New("cannot locate peer on network")
	}

	// nb: terminate the context since the context wouldn't actually be passed
	// over the network in a real scenario

	msg := &message[MessageType]{
		from:       from,
		msg:        mes,
		shouldSend: time.Now().Add(latency).Add(bandwidthDelay),
	}
	receiver.enqueue(msg)

	return nil
}

type networkClient[MessageType network.Message[MessageType]] struct {
	// These need to be at the top of the struct (allocated on the heap) for alignment on 32bit platforms.
	stats network.Stats

	local              peer.ID
	receivers          []network.Receiver[MessageType]
	network            *virtualnetwork[MessageType]
	routing            routing.Routing
	supportedProtocols []protocol.ID
}

func (nc *networkClient[MessageType]) ReceiveMessage(ctx context.Context, sender peer.ID, incoming MessageType) {
	for _, v := range nc.receivers {
		v.ReceiveMessage(ctx, sender, incoming)
	}
}

func (nc *networkClient[MessageType]) ReceiveError(p peer.ID, e error) {
	for _, v := range nc.receivers {
		v.ReceiveError(p, e)
	}
}

func (nc *networkClient[MessageType]) PeerConnected(p peer.ID) {
	for _, v := range nc.receivers {
		v.PeerConnected(p)
	}
}
func (nc *networkClient[MessageType]) PeerDisconnected(p peer.ID) {
	for _, v := range nc.receivers {
		v.PeerDisconnected(p)
	}
}

func (nc *networkClient[MessageType]) Self() peer.ID {
	return nc.local
}

func (nc *networkClient[MessageType]) Ping(ctx context.Context, p peer.ID) ping.Result {
	return ping.Result{RTT: nc.Latency(p)}
}

func (nc *networkClient[MessageType]) Latency(p peer.ID) time.Duration {
	nc.network.mu.Lock()
	defer nc.network.mu.Unlock()
	return nc.network.latencies[nc.local][p]
}

func (nc *networkClient[MessageType]) SendMessage(
	ctx context.Context,
	to peer.ID,
	message MessageType) error {
	if err := nc.network.SendMessage(ctx, nc.local, to, message); err != nil {
		return err
	}
	atomic.AddUint64(&nc.stats.MessagesSent, 1)
	return nil
}

func (nc *networkClient[MessageType]) Stats() network.Stats {
	return network.Stats{
		MessagesRecvd: atomic.LoadUint64(&nc.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&nc.stats.MessagesSent),
	}
}

func (nc *networkClient[MessageType]) ConnectionManager() network.ConnManager {
	return &connmgr.NullConnMgr{}
}

type messagePasser[MessageType network.Message[MessageType]] struct {
	net    *networkClient[MessageType]
	target peer.ID
	local  peer.ID
	ctx    context.Context
}

func (mp *messagePasser[MessageType]) SendMsg(ctx context.Context, m MessageType) error {
	return mp.net.SendMessage(ctx, mp.target, m)
}

func (mp *messagePasser[MessageType]) Close() error {
	return nil
}

func (mp *messagePasser[MessageType]) Reset() error {
	return nil
}

func (mp *messagePasser[MessageType]) Protocol() protocol.ID {
	protos := mp.net.network.clients[mp.target].receiver.supportedProtocols
	return protos[0]
}

func (nc *networkClient[MessageType]) NewMessageSender(ctx context.Context, p peer.ID, opts *network.MessageSenderOpts) (network.MessageSender[MessageType], error) {
	return &messagePasser[MessageType]{
		net:    nc,
		target: p,
		local:  nc.local,
		ctx:    ctx,
	}, nil
}

func (nc *networkClient[MessageType]) Start(r ...network.Receiver[MessageType]) {
	nc.receivers = r
}

func (nc *networkClient[MessageType]) Stop() {
}

func (nc *networkClient[MessageType]) ConnectTo(_ context.Context, p peer.ID) error {
	nc.network.mu.Lock()
	otherClient, ok := nc.network.clients[p]
	if !ok {
		nc.network.mu.Unlock()
		return errors.New("no such peer in network")
	}

	tag := tagForPeers(nc.local, p)
	if _, ok := nc.network.conns[tag]; ok {
		nc.network.mu.Unlock()
		// log.Warning("ALREADY CONNECTED TO PEER (is this a reconnect? test lib needs fixing)")
		return nil
	}
	nc.network.conns[tag] = struct{}{}
	nc.network.mu.Unlock()

	otherClient.receiver.PeerConnected(nc.local)
	nc.PeerConnected(p)
	return nil
}

func (nc *networkClient[MessageType]) DisconnectFrom(_ context.Context, p peer.ID) error {
	nc.network.mu.Lock()
	defer nc.network.mu.Unlock()

	otherClient, ok := nc.network.clients[p]
	if !ok {
		return errors.New("no such peer in network")
	}

	tag := tagForPeers(nc.local, p)
	if _, ok := nc.network.conns[tag]; !ok {
		// Already disconnected
		return nil
	}
	delete(nc.network.conns, tag)

	otherClient.receiver.PeerDisconnected(nc.local)
	nc.PeerDisconnected(p)
	return nil
}

func (rq *receiverQueue[MessageType]) enqueue(m *message[MessageType]) {
	rq.lk.Lock()
	defer rq.lk.Unlock()
	rq.queue = append(rq.queue, m)
	if !rq.active {
		rq.active = true
		go rq.process()
	}
}

func (rq *receiverQueue[MessageType]) Swap(i, j int) {
	rq.queue[i], rq.queue[j] = rq.queue[j], rq.queue[i]
}

func (rq *receiverQueue[MessageType]) Len() int {
	return len(rq.queue)
}

func (rq *receiverQueue[MessageType]) Less(i, j int) bool {
	return rq.queue[i].shouldSend.UnixNano() < rq.queue[j].shouldSend.UnixNano()
}

func (rq *receiverQueue[MessageType]) process() {
	for {
		rq.lk.Lock()
		sort.Sort(rq)
		if len(rq.queue) == 0 {
			rq.active = false
			rq.lk.Unlock()
			return
		}
		m := rq.queue[0]
		if time.Until(m.shouldSend).Seconds() < 0.1 {
			rq.queue = rq.queue[1:]
			rq.lk.Unlock()
			time.Sleep(time.Until(m.shouldSend))
			atomic.AddUint64(&rq.receiver.stats.MessagesRecvd, 1)
			rq.receiver.ReceiveMessage(context.TODO(), m.from, m.msg)
		} else {
			rq.lk.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func tagForPeers(a, b peer.ID) string {
	if a < b {
		return string(a + b)
	}
	return string(b + a)
}
