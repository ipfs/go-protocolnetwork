package testutil

import (
	"context"
	"testing"
)

type Notifier struct {
	handleQueued   chan struct{}
	handleError    chan struct{}
	Err            error
	handleSent     chan struct{}
	handleFinished chan struct{}
}

func (n *Notifier) HandleQueued() {
	n.handleQueued <- struct{}{}
}

func (n *Notifier) HandleError(err error) {
	n.Err = err
	n.handleError <- struct{}{}
}

func (n *Notifier) HandleSent() {
	n.handleSent <- struct{}{}
}

func (n *Notifier) HandleFinished() {
	n.handleFinished <- struct{}{}
}

func (n *Notifier) ExpectHandleQueued(ctx context.Context, t *testing.T) {
	AssertDoesReceive(ctx, t, n.handleQueued, "did not receive handle queued")
}

func (n *Notifier) ExpectHandleError(ctx context.Context, t *testing.T) {
	AssertDoesReceive(ctx, t, n.handleError, "did not receive handle error")
}

func (n *Notifier) ExpectHandleSent(ctx context.Context, t *testing.T) {
	AssertDoesReceive(ctx, t, n.handleSent, "did not receive handle sent")
}

func (n *Notifier) ExpectHandleFinished(ctx context.Context, t *testing.T) {
	AssertDoesReceive(ctx, t, n.handleFinished, "did not receive handle finished")
}
