package testutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-protocolnetwork/pkg/notifications"
)

type TestSubscriber[Topic comparable, Event any] struct {
	receivedEvents chan DispatchedEvent[Topic, Event]
	closed         chan Topic
}

type DispatchedEvent[Topic comparable, Event any] struct {
	Topic Topic
	Event Event
}

func NewTestSubscriber[Topic comparable, Event any](bufferSize int) *TestSubscriber[Topic, Event] {
	return &TestSubscriber[Topic, Event]{
		receivedEvents: make(chan DispatchedEvent[Topic, Event], bufferSize),
		closed:         make(chan Topic, bufferSize),
	}
}

func (ts *TestSubscriber[Topic, Event]) OnNext(topic Topic, ev Event) {
	ts.receivedEvents <- DispatchedEvent[Topic, Event]{topic, ev}
}

func (ts *TestSubscriber[Topic, Event]) OnClose(topic Topic) {
	ts.closed <- topic
}

func (ts *TestSubscriber[Topic, Event]) ExpectEvents(ctx context.Context, t *testing.T, events []DispatchedEvent[Topic, Event]) {
	t.Helper()
	for _, expectedEvent := range events {
		var event DispatchedEvent[Topic, Event]
		AssertReceive(ctx, t, ts.receivedEvents, &event, "should receive another event")
		require.Equal(t, expectedEvent, event)
	}
}
func (ts *TestSubscriber[Topic, Event]) ExpectEventsAllTopics(ctx context.Context, t *testing.T, events []Event) {
	t.Helper()
	for _, expectedEvent := range events {
		var event DispatchedEvent[Topic, Event]
		AssertReceive(ctx, t, ts.receivedEvents, &event, "should receive another event")
		require.Equal(t, expectedEvent, event.Event)
	}
}

func (ts *TestSubscriber[Topic, Event]) NoEventsReceived(t *testing.T) {
	t.Helper()
	AssertChannelEmpty(t, ts.receivedEvents, "should have received no events")
}

func (ts *TestSubscriber[Topic, Event]) ExpectClosesAnyOrder(ctx context.Context, t *testing.T, topics []Topic) {
	t.Helper()
	expectedTopics := make(map[Topic]struct{})
	receivedTopics := make(map[Topic]struct{})
	for _, expectedTopic := range topics {
		expectedTopics[expectedTopic] = struct{}{}
		var topic Topic
		AssertReceive(ctx, t, ts.closed, &topic, "should receive another event")
		receivedTopics[topic] = struct{}{}
	}
	require.Equal(t, expectedTopics, receivedTopics)
}

func (ts *TestSubscriber[Topic, Event]) ExpectNCloses(ctx context.Context, t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		AssertDoesReceive(ctx, t, ts.closed, "should receive another event")
	}
}

func (ts *TestSubscriber[Topic, Event]) ExpectCloses(ctx context.Context, t *testing.T, topics []Topic) {
	t.Helper()
	for _, expectedTopic := range topics {
		var topic Topic
		AssertReceive(ctx, t, ts.closed, &topic, "should receive another event")
		require.Equal(t, expectedTopic, topic)
	}
}

type MockPublisher[Topic comparable, Event any] struct {
	subscribersLk sync.Mutex
	subscribers   []notifications.Subscriber[Topic, Event]
}

func (mp *MockPublisher[Topic, Event]) AddSubscriber(subscriber notifications.Subscriber[Topic, Event]) {
	mp.subscribersLk.Lock()
	mp.subscribers = append(mp.subscribers, subscriber)
	mp.subscribersLk.Unlock()
}

func (mp *MockPublisher[Topic, Event]) PublishEvents(topic Topic, events []Event) {
	mp.subscribersLk.Lock()
	for _, subscriber := range mp.subscribers {

		for _, ev := range events {
			subscriber.OnNext(topic, ev)
		}
		subscriber.OnClose(topic)
	}
	mp.subscribersLk.Unlock()
}

func NewMockPublisher[Topic comparable, Event any]() *MockPublisher[Topic, Event] {
	return &MockPublisher[Topic, Event]{}
}
