package notifications_test

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-protocolnetwork/internal/testutil"
	"github.com/ipfs/go-protocolnetwork/pkg/notifications"
)

type Topic string
type Event string

func TestSubscribeWithData(t *testing.T) {
	ctx := context.Background()
	testCases := map[string]func(ctx context.Context, t *testing.T, ps notifications.Publisher[Topic, Event]){
		"Add subscriptions": func(ctx context.Context, t *testing.T, ps notifications.Publisher[Topic, Event]) {
			sub1 := testutil.NewTestSubscriber[Topic, Event](3)
			sub2 := testutil.NewTestSubscriber[Topic, Event](3)
			ps.Subscribe("t1", sub1)
			ps.Subscribe("t2", sub2)

			ps.Publish("t1", "hi1")
			ps.Publish("t2", "hi2")

			ps.Subscribe("t2", sub1)
			ps.Subscribe("t3", sub1)

			ps.Publish("t2", "hi3")
			ps.Publish("t3", "hi4")

			ps.Shutdown()
			sub1.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{
					Topic: "t1",
					Event: "hi1",
				},
				{
					Topic: "t2",
					Event: "hi3",
				},
				{
					Topic: "t3",
					Event: "hi4",
				},
			})
			sub1.ExpectClosesAnyOrder(ctx, t, []Topic{"t1", "t2", "t3"})
			sub2.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{Topic: "t2", Event: "hi2"}, {Topic: "t2", Event: "hi3"},
			})
			sub2.ExpectClosesAnyOrder(ctx, t, []Topic{"t2"})
		},
		"Unsubscribe": func(ctx context.Context, t *testing.T, ps notifications.Publisher[Topic, Event]) {
			sub1 := testutil.NewTestSubscriber[Topic, Event](3)
			sub2 := testutil.NewTestSubscriber[Topic, Event](3)
			ps.Subscribe("t1", sub1)
			ps.Subscribe("t2", sub1)
			ps.Subscribe("t3", sub1)
			ps.Subscribe("t1", sub2)
			ps.Subscribe("t3", sub2)
			ps.Unsubscribe(sub1)
			ps.Publish("t1", "hi")
			ps.Shutdown()

			sub2.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{Topic: "t1", Event: "hi"},
			})
			sub2.ExpectClosesAnyOrder(ctx, t, []Topic{"t1", "t3"})
			sub1.ExpectClosesAnyOrder(ctx, t, []Topic{"t1", "t2", "t3"})
			sub1.NoEventsReceived(t)
		},
		"Close": func(ctx context.Context, t *testing.T, ps notifications.Publisher[Topic, Event]) {
			sub1 := testutil.NewTestSubscriber[Topic, Event](3)
			sub2 := testutil.NewTestSubscriber[Topic, Event](3)
			sub3 := testutil.NewTestSubscriber[Topic, Event](3)
			sub4 := testutil.NewTestSubscriber[Topic, Event](3)
			ps.Subscribe("t1", sub1)
			ps.Subscribe("t1", sub2)
			ps.Subscribe("t2", sub3)
			ps.Subscribe("t3", sub4)

			ps.Publish("t1", "hi")
			ps.Publish("t2", "hello")
			ps.Close("t1")
			ps.Close("t2")

			sub1.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{Topic: "t1", Event: "hi"},
			})
			sub1.ExpectClosesAnyOrder(ctx, t, []Topic{"t1"})
			sub2.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{Topic: "t1", Event: "hi"},
			})
			sub2.ExpectClosesAnyOrder(ctx, t, []Topic{"t1"})
			sub3.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{Topic: "t2", Event: "hello"},
			})
			sub3.ExpectClosesAnyOrder(ctx, t, []Topic{"t2"})

			// publishing on a topic after close should be like starting from scratch
			// -- no one listening before should receive events
			ps.Publish("t1", "hi")
			ps.Publish("t2", "hi")

			ps.Publish("t3", "welcome")
			ps.Shutdown()

			sub4.ExpectEvents(ctx, t, []testutil.DispatchedEvent[Topic, Event]{
				{Topic: "t3", Event: "welcome"},
			})
			sub4.ExpectClosesAnyOrder(ctx, t, []Topic{"t3"})
			sub1.NoEventsReceived(t)
			sub2.NoEventsReceived(t)
			sub3.NoEventsReceived(t)
		},
		"Shutdown": func(ctx context.Context, t *testing.T, ps notifications.Publisher[Topic, Event]) {
			sub1 := testutil.NewTestSubscriber[Topic, Event](3)
			sub2 := testutil.NewTestSubscriber[Topic, Event](3)
			ps.Subscribe("t1", sub1)
			ps.Subscribe("t2", sub2)

			ps.Shutdown()

			// operations after shutdown have no effect
			ps.Publish("t1", "hi")
			ps.Publish("t2", "hello")
			sub1.ExpectClosesAnyOrder(ctx, t, []Topic{"t1"})
			sub2.ExpectClosesAnyOrder(ctx, t, []Topic{"t2"})
			time.Sleep(100 * time.Millisecond)
			sub1.NoEventsReceived(t)
			sub2.NoEventsReceived(t)
		},
	}
	for testCase, testPublisher := range testCases {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			ps := notifications.NewPublisher[Topic, Event]()
			ps.Startup()
			testPublisher(ctx, t, ps)
			ps.Shutdown()
		})
	}

}
