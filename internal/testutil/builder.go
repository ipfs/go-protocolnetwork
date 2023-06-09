package testutil

import (
	"errors"
	"sync"

	"github.com/ipfs/go-protocolnetwork/pkg/messagequeue"
)

type SingleBuilder struct {
	id          []byte
	payload     []byte
	newNotifier func(id []byte) *Notifier
	notifier    *Notifier
}

func (b *SingleBuilder) Empty() bool {
	return b.id == nil
}
func (b *SingleBuilder) Build() (*Message, messagequeue.Notifier, error) {
	if b.id == nil {
		return nil, nil, errors.New("empty")
	}
	return &Message{Id: b.id, Payload: b.payload}, b.notifier, nil
}

func (b *SingleBuilder) SetID(id []byte) {
	b.id = id
	if b.newNotifier != nil {
		b.notifier = b.newNotifier(id)
	}
}

func (b *SingleBuilder) SetPayload(payload []byte) {
	b.payload = payload
}

type MessageBuilder struct {
	notifiers map[string]*Notifier
	builder   *SingleBuilder
	builderLk sync.Mutex
}

func NewMessageBuilder() *MessageBuilder {
	return &MessageBuilder{
		notifiers: make(map[string]*Notifier),
	}
}
func (bc *MessageBuilder) BuildMessage(op func(*SingleBuilder)) bool {
	bc.builderLk.Lock()
	defer bc.builderLk.Unlock()
	if bc.builder == nil {
		bc.builder = &SingleBuilder{newNotifier: bc.Notifier}
	}
	op(bc.builder)
	return !bc.builder.Empty()
}

func (bc *MessageBuilder) NextMessage() (messagequeue.MessageSpec[*Message], bool, error) {
	bc.builderLk.Lock()
	defer bc.builderLk.Unlock()
	if bc.builder == nil {
		return nil, false, errors.New("no messages")
	}
	builder := bc.builder
	bc.builder = nil
	if builder.Empty() {
		return nil, false, errors.New("Message empty")
	}
	return builder.Build, false, nil
}

func (bc *MessageBuilder) Notifier(id []byte) *Notifier {
	n, ok := bc.notifiers[string(id)]
	if !ok {
		n = &Notifier{
			handleQueued:   make(chan struct{}, 1),
			handleError:    make(chan struct{}, 1),
			handleSent:     make(chan struct{}, 1),
			handleFinished: make(chan struct{}, 1),
		}
		bc.notifiers[string(id)] = n
	}
	return n
}
