package cgroup

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/alexrudd/gomdb"
	"github.com/gofrs/uuid"
)

const GroupCategory = "_group"

type Logger interface {
	Printf(format string, v ...interface{})
}

type Client struct {
	mdbc *gomdb.Client
	log  Logger
}

func NewClient(mdbc *gomdb.Client) *Client {
	return &Client{
		mdbc: mdbc,
		log:  log.Default(),
	}
}

func (c *Client) JoinGroup(
	ctx context.Context,
	group, category, consumerID string,
	handleMessage gomdb.MessageHandler,
) error {
	var (
		gs = &GroupState{
			Version:         gomdb.NoStreamVersion,
			ActiveConsumers: map[string]*ConsumerState{},
			IdleConsumers:   map[string]*ConsumerState{},
		}
		gsMtx = &sync.Mutex{}
		cs    = &ConsumerState{
			MilestoneComplete: true,
		}
		csMtx       = &sync.Mutex{}
		pctx, stop  = context.WithCancel(ctx)
		live        = false
		outerr      error
		groupStream = gomdb.StreamIdentifier{
			Category: GroupCategory,
			ID:       group,
		}
	)

	// subscribe to consumer group stream and catch up
	go func() {
		c.log.Printf("Starting group state subscription")

		outerr = c.mdbc.SubscribeToStream(pctx, groupStream, func(m *gomdb.Message) {
			evt, err := eventFromMessage(m)
			if err != nil {
				c.log.Printf("converting messaging to consumer group event: %s", err)
				return
			}

			c.log.Printf("Received group state event %d:%s", m.Version, evt.Type())

			gsMtx.Lock()
			defer gsMtx.Unlock()

			evt.Apply(gs, m.Version)

			// if a MilestoneStarted event is received then rebuild consumer
			// state from the Milestone
			if live && evt.Type() == MilestoneStartedEventType {
				csMtx.Lock()
				defer csMtx.Unlock()

				// replace consumer state using milestone goal
				cs = gs.CurrentMilestone.initialStateFor(consumerID)
			}
		}, func(b bool) {
			live = b
		}, func(e error) {
			c.log.Printf("received error on subscription: %s", e)
		})
		if outerr != nil {
			c.log.Printf("stopping group state subscription: %s", outerr)
			stop()
		}
	}()

	// try to be leader as often as possible
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()

		for {
			select {
			case <-pctx.Done():
				return
			case <-t.C:
			}

			if live {
				c.log.Printf("Doing group management")

				gsMtx.Lock()

				// has the leader expired or am I the leader and am about to
				// expire?
				var (
					noLeader               = gs.LeaderExpires.Before(time.Now())
					myLeadershipIsExpiring = gs.Leader == consumerID && gs.LeaderExpires.Before(time.Now().Add(time.Second))
				)

				if noLeader || myLeadershipIsExpiring {
					c.log.Printf("Declaring leader")

					evt := &LeaderDeclared{
						GroupName:  group,
						ConsumerID: consumerID,
						Until:      time.Now().Add(5 * time.Second),
					}

					_, err := c.mdbc.WriteMessage(pctx, groupStream, gomdb.ProposedMessage{
						ID:   uuid.Must(uuid.NewV4()).String(),
						Type: evt.Type(),
						Data: evt,
					}, gs.Version)
					if err != nil {
						if !errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
							c.log.Printf("writing LeaderDeclared event: %s", err)
							outerr = err
							stop()
						}
					}

					gsMtx.Unlock()
					continue
				}

				// have all consumers completed or died?
				if len(gs.IdleConsumers) > 0 && (len(gs.ActiveConsumers) == 0 || !gs.activeConsumersAreAlive()) {
					current := gs.CurrentMilestone
					next := Milestone{
						ID:         1,
						From:       0,
						End:        100,
						Partitions: map[string]int64{},
						Debt:       map[string][]*ParitionDebt{},
					}

					if current != nil {
						next.ID = current.ID + 1
						next.From = current.End
						next.End = next.From + 100
					}

					// get all idle consumers
					var idlers []string
					for cid, cs := range gs.IdleConsumers {
						if cs.NextCheckIn.After(time.Now()) {
							idlers = append(idlers, cid)
						}
					}

					// capture any debt from current milestone
					var debts []*ParitionDebt
					for cid, cs := range gs.ActiveConsumers {
						debts = append(debts, &ParitionDebt{
							GroupSize: int64(len(current.Partitions)),
							Partition: current.Partitions[cid],
							From:      cs.CurrentPosition + 1,
							End:       current.End,
						})

						for _, dbt := range cs.Debt {
							debts = append(debts, &ParitionDebt{
								GroupSize: dbt.GroupSize,
								Partition: dbt.Partition,
								From:      dbt.CurrentPosition + 1,
								End:       dbt.End,
							})
						}
					}

					// partition milestone
					for idx, cid := range idlers {
						next.Partitions[cid] = int64(idx)
					}

					// partition debt
					for idx, dbt := range debts {

						cid := idlers[idx%len(idlers)]
						next.Debt[cid] = append(next.Debt[cid], dbt)
					}

					evt := &MilestoneStarted{
						GroupName: group,
						Milestone: next,
					}
					_, err := c.mdbc.WriteMessage(pctx, groupStream, gomdb.ProposedMessage{
						ID:   uuid.Must(uuid.NewV4()).String(),
						Type: evt.Type(),
						Data: evt,
					}, gs.Version)
					if err != nil {
						if !errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
							c.log.Printf("writing MilestoneStarted event: %s", err)
							outerr = err
							stop()
						}
					}

					gsMtx.Unlock()
					continue
				}

				gsMtx.Unlock()
			}
		}
	}()

	// when live, publish a ConsumerCheckIn events at regular intervals (pause if not live)
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()

		for {
			select {
			case <-pctx.Done():
				return
			case <-t.C:
			}

			if live {
				csMtx.Lock()
				gsMtx.Lock()

				cs.CheckedIn = time.Now()
				cs.NextCheckIn = cs.CheckedIn.Add(110 * time.Millisecond)

				evt := &ConsumerCheckedIn{
					GroupName:     group,
					ConsumerState: *cs,
				}

				_, err := c.mdbc.WriteMessage(pctx, groupStream, gomdb.ProposedMessage{
					ID:   uuid.Must(uuid.NewV4()).String(),
					Type: evt.Type(),
					Data: evt,
				}, gs.Version)
				if err != nil {
					if !errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
						c.log.Printf("writing consumer check in event: %s", err)
						outerr = err
						stop()
					}
				}

				csMtx.Unlock()
				gsMtx.Unlock()
			}

		}
	}()

	// iterate to progress through milestone
	t := time.NewTimer(0)
	defer t.Stop()

	for {
		select {
		case <-pctx.Done():
			return outerr
		case <-t.C:
		}

		var msgs []*gomdb.Message

		if ms := gs.CurrentMilestone; live && ms != nil {
			csMtx.Lock()

			msgs, outerr = c.mdbc.GetCategoryMessages(
				pctx,
				category,
				gomdb.AsConsumerGroup(ms.Partitions[consumerID], int64(len(ms.Partitions))),
				gomdb.FromPosition(cs.CurrentPosition+1),
				gomdb.WithCategoryBatchSize(10),
			)

			if len(msgs) == 10 {
				t.Reset(0)
			} else {
				t.Reset(10 * time.Millisecond)
			}

			for _, m := range msgs {
				if m.GlobalPosition >= ms.End {
					break
				}

				handleMessage(m)
				cs.CurrentPosition = m.GlobalPosition
			}

			csMtx.Unlock()
		}
	}
}

// GroupStateHandler
// TODO: is the bool required? or should the handler just not be called until
// state is live?
type GroupStateHandler func(*GroupState, interface{}, bool)

// ObserveGroup will build the specified group's state
func (c *Client) ObserveGroup(
	ctx context.Context,
	group string,
	stateHandler GroupStateHandler,
) error {
	var (
		gs = &GroupState{
			ActiveConsumers: map[string]*ConsumerState{},
			IdleConsumers:   map[string]*ConsumerState{},
		}
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

		stateHandler(gs, evt, live)
	}, func(b bool) {
		live = b
	}, func(e error) {
		c.log.Printf("received error on subscription: %s", e)
	})
}
