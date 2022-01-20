package tests

import (
	"context"
	"testing"

	"github.com/alexrudd/gomdb"
	"github.com/alexrudd/gomdb/cgroup"
	"gopkg.in/yaml.v3"
)

func TestJoinConsumerGroup(t *testing.T) {
	c := NewClient(t)
	received := map[int64]bool{}
	category := NewTestCategory("cgroup")
	// lastPos := int64(0)

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
		// lastPos = msg.GlobalPosition
	}

	group := GenUUID()
	client := cgroup.NewClient(c)
	gCtx, stop := context.WithCancel(context.TODO())

	go func() {
		err := client.ObserveGroup(gCtx, group, func(gs *cgroup.GroupState, e interface{}, b bool) {
			event, _ := yaml.Marshal(e)
			state, _ := yaml.Marshal(gs)

			t.Logf("Event (%T):\n%s\n\nState:\n%s\n\n", e, event, state)
		})
		if err != nil {
			t.Log(err)
		}
	}()

	// c1Ctx, c1 := context.WithTimeout(gCtx, 10*time.Second)
	// defer c1()

	// go func() {
	err := client.JoinGroup(gCtx, group, category, "con1", func(m *gomdb.Message) {
		received[m.GlobalPosition] = true

		t.Logf("con1 received message: %d", m.GlobalPosition)

		// if m.GlobalPosition == lastPos {
		// 	stop()
		// }
	})
	if err != nil {
		t.Fatal(err)
	}
	// }()

	// c2Ctx, c2 := context.WithTimeout(gCtx, 10*time.Second)
	// defer c2()

	// go func() {

	// 	_ = client.JoinGroup(c2Ctx, group, category, "con2", func(m *gomdb.Message) {
	// 		received[m.GlobalPosition] = true

	// 		t.Logf("con2 received message: %d", m.GlobalPosition)

	// 		if m.GlobalPosition == lastPos {
	// 			c2()
	// 		}
	// 	})
	// }()

	<-gCtx.Done()
	// <-c2Ctx.Done()
	stop()

	for pos, r := range received {
		if !r {
			t.Errorf("Did not receive message for position %d", pos)
		}
	}

}
