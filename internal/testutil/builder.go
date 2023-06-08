package testutil

import (
	"errors"

	"github.com/ipfs/go-protocolnetwork/pkg/messagequeue"
)

type Builder struct {
	id          []byte
	payload     []byte
	newNotifier func(id []byte) *Notifier
	notifier    *Notifier
}

func (b *Builder) Empty() bool {
	return b.id == nil
}
func (b *Builder) Build() (*Message, messagequeue.Notifier, error) {
	if b.id == nil {
		return nil, nil, errors.New("empty")
	}
	return &Message{Id: b.id, Payload: b.payload}, b.notifier, nil
}

func (b *Builder) SetID(id []byte) {
	b.id = id
	b.notifier = b.newNotifier(id)
}

func (b *Builder) SetPayload(payload []byte) {
	b.payload = payload
}

type BuilderCollection struct {
	notifiers map[string]*Notifier
	builder   *Builder
}

func NewBuilderCollection() *BuilderCollection {
	return &BuilderCollection{
		notifiers: make(map[string]*Notifier),
	}
}
func (bc *BuilderCollection) NextBuilder(_ int) *Builder {
	if bc.builder == nil {
		bc.builder = &Builder{newNotifier: bc.Notifier}
	}
	return bc.builder
}

func (bc *BuilderCollection) ExtractFirstBuilder() *Builder {
	builder := bc.builder
	bc.builder = nil
	return builder
}

func (bc *BuilderCollection) Empty() bool {
	return bc.builder == nil
}

func (bc *BuilderCollection) Notifier(id []byte) *Notifier {
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
