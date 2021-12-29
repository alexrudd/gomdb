package cgroup

import (
	"context"

	"github.com/alexrudd/gomdb"
)

const GroupCategory = "_group"

type Client struct {
	mdbc *gomdb.Client
	log  Logger
}

type Logger interface {
	Printf(format string, v ...interface{})
}

func (c *Client) JoinGroup(
	ctx context.Context,
	group, consumerID string,
	handleMessage gomdb.MessageHandler,
	handleLiveness gomdb.LivenessHandler,
	handleDropped gomdb.SubDroppedHandler,
) error {
	gs := &GroupState{}

	c.mdbc.SubscribeToCategory(ctx, gs.Category, handleMessage, handleLiveness, handleDropped, gomdb.FromPosition(gs.CurrentMilestone.From), gomdb.AsConsumerGroup(int64(gs.CurrentMilestone.Partitions[consumerID], len(gs.CurrentMilestone.Partitions))))

	return nil
}

// GroupStateHandler
// TODO: is the bool required? or should the handler just not be called until
// state is live?
type GroupStateHandler func(*GroupState, bool)

func (c *Client) ObserveGroup(
	ctx context.Context,
	group string,
	stateHandler GroupStateHandler,
) error {
	var (
		gs   = &GroupState{}
		live = false
	)

	return c.mdbc.SubscribeToStream(ctx, gomdb.StreamIdentifier{
		Category: GroupCategory,
		ID:       group,
	}, func(m *gomdb.Message) {
		evt, err := eventFromMessage(m)
		if err != nil {
			c.log.Printf("converting messaging to consumer group event: %s", err)
			return
		}

		evt.Apply(gs, m.Version)

		stateHandler(gs, live)
	}, func(b bool) {
		live = b
	}, func(e error) {
		c.log.Printf("received error on subscription: %s", e)
	})
}
