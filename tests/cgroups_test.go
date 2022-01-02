package tests

import (
	"context"
	"testing"
	"time"

	"github.com/alexrudd/gomdb"
	"github.com/alexrudd/gomdb/cgroup"
)

func TestJoinConsumerGroup(t *testing.T) {
	c := NewClient(t)
	received := map[int64]bool{}
	category := NewTestCategory("cgroup")
	lastPos := int64(0)

	// populate a category
	for i := 0; i < 10; i++ {
		stream := NewTestStream(category)

		_, err := c.WriteMessage(context.TODO(), stream, gomdb.ProposedMessage{
			ID:   GenUUID(),
			Type: "Test",
			Data: i,
		}, gomdb.AnyVersion)
		if err != nil {
			t.Fatal(err)
		}

		msg, err := c.GetLastStreamMessage(context.TODO(), stream)
		if err != nil {
			t.Fatal(err)
		}

		received[msg.GlobalPosition] = false
		lastPos = msg.GlobalPosition
	}

	group := GenUUID()
	client := cgroup.NewClient(c)
	gCtx, stop := context.WithTimeout(context.TODO(), 3*time.Second)

	go func() {
		err := client.ObserveGroup(gCtx, group, func(gs *cgroup.GroupState, e interface{}, b bool) {
			t.Logf("Event %T:\n%+v", e, e)
			t.Logf(`Group state:
Live: %v
Version: %v
Leader: %s
Leader Expires in: %s
Active consumers: %d
Idle consumers: %d
			`,
				b,
				gs.Version,
				gs.Leader,
				time.Until(gs.LeaderExpires),
				len(gs.ActiveConsumers),
				len(gs.IdleConsumers),
			)
		})
		if err != nil {
			t.Log(err)
		}
	}()

	err := client.JoinGroup(gCtx, group, category, "con1", func(m *gomdb.Message) {
		received[m.GlobalPosition] = true

		t.Logf("Received message: %d", m.GlobalPosition)

		if m.GlobalPosition == lastPos {
			stop()
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	for pos, r := range received {
		if !r {
			t.Errorf("Did not receive message for position %d", pos)
		}
	}

}
