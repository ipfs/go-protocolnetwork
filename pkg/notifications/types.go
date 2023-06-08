package notifications

// Subscriber is a subscriber that can receive events
type Subscriber[Topic comparable, Event any] interface {
	OnNext(Topic, Event)
	OnClose(Topic)
}

// Subscribable is a stream that can be subscribed to
type Subscribable[Topic comparable, Event any] interface {
	Subscribe(topic Topic, sub Subscriber[Topic, Event]) bool
	Unsubscribe(sub Subscriber[Topic, Event]) bool
}

// Publisher is an publisher of events that can be subscribed to
type Publisher[Topic comparable, Event any] interface {
	Close(Topic)
	Publish(Topic, Event)
	Shutdown()
	Startup()
	Subscribable[Topic, Event]
}
