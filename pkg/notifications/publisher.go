package notifications

import (
	"sync"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("protocolnetwork/notifications")

type operation int

const (
	subscribe operation = iota
	pub
	unsubAll
	closeTopic
	shutdown
)

type cmd[Topic comparable, Event any] struct {
	op     operation
	topics []Topic
	sub    Subscriber[Topic, Event]
	msg    Event
}

// publisher is a publisher of events for
type publisher[Topic comparable, Event any] struct {
	lk     sync.RWMutex
	closed chan struct{}
	cmds   []cmd[Topic, Event]
	cmdsLk *sync.Cond
}

// NewPublisher returns a new message event publisher
func NewPublisher[Topic comparable, Event any]() Publisher[Topic, Event] {
	ps := &publisher[Topic, Event]{
		cmdsLk: sync.NewCond(&sync.Mutex{}),
		closed: make(chan struct{}),
	}
	return ps
}

func (ps *publisher[Topic, Event]) Startup() {
	go ps.start()
}

// Publish publishes an event for the given message id
func (ps *publisher[Topic, Event]) Publish(topic Topic, event Event) {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	select {
	case <-ps.closed:
		return
	default:
	}

	ps.queue(cmd[Topic, Event]{op: pub, topics: []Topic{topic}, msg: event})
}

// Shutdown shuts down all events and subscriptions
func (ps *publisher[Topic, Event]) Shutdown() {
	ps.lk.Lock()
	defer ps.lk.Unlock()
	select {
	case <-ps.closed:
		return
	default:
	}
	close(ps.closed)
	ps.queue(cmd[Topic, Event]{op: shutdown})
}

func (ps *publisher[Topic, Event]) Close(id Topic) {
	ps.lk.RLock()
	defer ps.lk.RUnlock()
	select {
	case <-ps.closed:
		return
	default:
	}
	ps.queue(cmd[Topic, Event]{op: closeTopic, topics: []Topic{id}})
}

func (ps *publisher[Topic, Event]) Subscribe(topic Topic, sub Subscriber[Topic, Event]) bool {
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	select {
	case <-ps.closed:
		return false
	default:
	}

	ps.queue(cmd[Topic, Event]{op: subscribe, topics: []Topic{topic}, sub: sub})
	return true
}

func (ps *publisher[Topic, Event]) Unsubscribe(sub Subscriber[Topic, Event]) bool {
	ps.lk.RLock()
	defer ps.lk.RUnlock()

	select {
	case <-ps.closed:
		return false
	default:
	}

	ps.queue(cmd[Topic, Event]{op: unsubAll, sub: sub})
	return true
}

func (ps *publisher[Topic, Event]) start() {
	reg := subscriberRegistry[Topic, Event]{
		topics:    make(map[Topic]map[Subscriber[Topic, Event]]struct{}),
		revTopics: make(map[Subscriber[Topic, Event]]map[Topic]struct{}),
	}

loop:
	for {
		cmd := ps.dequeue()
		if cmd.topics == nil {
			switch cmd.op {
			case unsubAll:
				reg.removeSubscriber(cmd.sub)

			case shutdown:
				break loop
			}

			continue loop
		}

		for _, topic := range cmd.topics {
			switch cmd.op {
			case subscribe:
				reg.add(topic, cmd.sub)

			case pub:
				reg.send(topic, cmd.msg)

			case closeTopic:
				reg.removeTopic(topic)
			}
		}
	}

	for topic, subs := range reg.topics {
		for sub := range subs {
			reg.remove(topic, sub)
		}
	}
}

type subscriberRegistry[Topic comparable, Event any] struct {
	topics    map[Topic]map[Subscriber[Topic, Event]]struct{}
	revTopics map[Subscriber[Topic, Event]]map[Topic]struct{}
}

func (reg *subscriberRegistry[Topic, Event]) add(topic Topic, sub Subscriber[Topic, Event]) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[Subscriber[Topic, Event]]struct{})
	}
	reg.topics[topic][sub] = struct{}{}

	if reg.revTopics[sub] == nil {
		reg.revTopics[sub] = make(map[Topic]struct{})
	}
	reg.revTopics[sub][topic] = struct{}{}
}

func (reg *subscriberRegistry[Topic, Event]) send(topic Topic, msg Event) {
	for sub := range reg.topics[topic] {
		sub.OnNext(topic, msg)
	}
}

func (reg *subscriberRegistry[Topic, Event]) removeTopic(topic Topic) {
	for sub := range reg.topics[topic] {
		reg.remove(topic, sub)
	}
}

func (reg *subscriberRegistry[Topic, Event]) removeSubscriber(sub Subscriber[Topic, Event]) {
	for topic := range reg.revTopics[sub] {
		reg.remove(topic, sub)
	}
}

func (reg *subscriberRegistry[Topic, Event]) remove(topic Topic, sub Subscriber[Topic, Event]) {
	if _, ok := reg.topics[topic]; !ok {
		return
	}

	if _, ok := reg.topics[topic][sub]; !ok {
		return
	}

	delete(reg.topics[topic], sub)
	delete(reg.revTopics[sub], topic)

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[sub]) == 0 {
		delete(reg.revTopics, sub)
	}

	sub.OnClose(topic)
}

func (ps *publisher[Topic, Event]) queue(cmd cmd[Topic, Event]) {
	ps.cmdsLk.L.Lock()
	ps.cmds = append(ps.cmds, cmd)
	cmdsLen := len(ps.cmds)
	ps.cmdsLk.L.Unlock()
	log.Debugw("added notification command", "cmd", cmd, "queue len", cmdsLen)
	ps.cmdsLk.Signal()
}

func (ps *publisher[Topic, Event]) dequeue() cmd[Topic, Event] {
	ps.cmdsLk.L.Lock()
	for len(ps.cmds) == 0 {
		ps.cmdsLk.Wait()
	}

	cmd := ps.cmds[0]
	ps.cmds = ps.cmds[1:]
	cmdsLen := len(ps.cmds)
	ps.cmdsLk.L.Unlock()
	log.Debugw("processing notification command", "cmd", cmd, "remaining in queue", cmdsLen)
	return cmd
}
