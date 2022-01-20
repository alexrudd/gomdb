package cgroup

import (
	"context"
	"errors"
	"fmt"
	"log"
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

type config struct {
	groupManagementPeriod time.Duration
	updateStatePeriod     time.Duration
	checkinPeriod         time.Duration
	checkinFrequency      int
}

func (c *Client) JoinGroup(
	ctx context.Context,
	group, category, consumerID string,
	handleMessage gomdb.MessageHandler,
) error {
	var (
		gs = &GroupState{
			Version:         gomdb.NoStreamVersion,
			Name:            group,
			Category:        category,
			ActiveConsumers: map[string]*ConsumerState{},
			IdleConsumers:   map[string]*ConsumerState{},
		}
		cs = &ConsumerState{
			ConsumerID:        consumerID,
			MilestoneComplete: true,
		}
		cfg = &config{
			groupManagementPeriod: time.Second,
			updateStatePeriod:     time.Second,
			checkinPeriod:         time.Second,
			checkinFrequency:      20,
		}
		strat = gomdb.DynamicPolling(
			0.5,
			10*time.Millisecond,
			10*time.Millisecond,
			time.Second,
		)()
		outerr error
	)

	updateStateNow := make(chan struct{}, 1)
	updateStateTimer := time.NewTimer(0)
	defer updateStateTimer.Stop()

	checkInNow := make(chan struct{}, 1)
	checkinTimer := time.NewTimer(100 * time.Millisecond)
	defer checkinTimer.Stop()

	manageGroupNow := make(chan struct{}, 1)
	manageGroupTimer := time.NewTimer(0)
	<-manageGroupTimer.C // clear timer until we've caught up on state
	defer manageGroupTimer.Stop()

	readMessagesTimer := time.NewTimer(0)
	<-readMessagesTimer.C // clear timer until we're ready to read messages
	defer readMessagesTimer.Stop()

	for {
		var (
			live             = false
			updated          = false
			checkedIn        = false
			managedGroup     = false
			startedMilestone = false
			endReached       = false
			delay            time.Duration
		)

		select {
		case <-ctx.Done():
			return nil
		case <-updateStateNow:
			// Group state
			updated, startedMilestone, live, outerr = c.updateState(ctx, gs)
			// if !updateStateTimer.Stop() {
			// 	<-updateStateTimer.C
			// }
			updateStateTimer.Reset(cfg.updateStatePeriod)
		case <-updateStateTimer.C:
			updated, startedMilestone, live, outerr = c.updateState(ctx, gs)
			// if !updateStateTimer.Stop() {
			// 	<-updateStateTimer.C
			// }
			updateStateTimer.Reset(cfg.updateStatePeriod)

		case <-checkInNow:
			checkedIn, outerr = c.checkIn(ctx, gs, cs, cfg.checkinPeriod)
			// if !checkinTimer.Stop() {
			// 	<-checkinTimer.C
			// }
			checkinTimer.Reset(cfg.checkinPeriod)
		case <-checkinTimer.C:
			checkedIn, outerr = c.checkIn(ctx, gs, cs, cfg.checkinPeriod)
			// if !checkinTimer.Stop() {
			// 	<-checkinTimer.C
			// }
			checkinTimer.Reset(cfg.checkinPeriod)

		case <-manageGroupNow:
			managedGroup, outerr = c.manageGroup(ctx, gs, cs)
			// if !manageGroupTimer.Stop() {
			// 	<-manageGroupTimer.C
			// }
			manageGroupTimer.Reset(cfg.groupManagementPeriod)
		case <-manageGroupTimer.C:
			managedGroup, outerr = c.manageGroup(ctx, gs, cs)
			// if !manageGroupTimer.Stop() {
			// 	<-manageGroupTimer.C
			// }
			manageGroupTimer.Reset(cfg.groupManagementPeriod)

		case <-readMessagesTimer.C:
			delay, endReached, outerr = c.readMessages(ctx, gs, cs, handleMessage, strat)
			// if !readMessagesTimer.Stop() {
			// 	<-readMessagesTimer.C
			// }
			readMessagesTimer.Reset(delay)
		}

		if outerr != nil {
			return outerr
		}

		switch {
		case live && managedGroup:
			select {
			case updateStateNow <- struct{}{}:
			default:
			}
		case checkedIn:
			select {
			case updateStateNow <- struct{}{}:
			default:
			}
		case live && (updated || !updated && gs.Version == gomdb.NoStreamVersion):
			select {
			case manageGroupNow <- struct{}{}:
			default:
			}
			if startedMilestone {
				cs = gs.CurrentMilestone.initialStateFor(consumerID)
				readMessagesTimer.Reset(0)

				select {
				case checkInNow <- struct{}{}:
				default:
				}
			}
		case endReached:
			select {
			case checkInNow <- struct{}{}:
			default:
			}
		}
	}
}

func (c *Client) updateState(ctx context.Context, gs *GroupState) (bool, bool, bool, error) {
	msgs, err := c.mdbc.GetStreamMessages(ctx, gomdb.StreamIdentifier{
		Category: GroupCategory,
		ID:       gs.Name,
	}, gomdb.FromVersion(gs.Version+1), gomdb.WithStreamBatchSize(100))
	if err != nil {
		return false, false, false, fmt.Errorf("reading group state stream: %w", err)
	}

	// there have been no state changes
	if len(msgs) == 0 {
		return false, false, true, nil
	}

	ms := false

	for _, m := range msgs {
		evt, err := eventFromMessage(m)
		if err != nil {
			continue
		}

		ms = ms || evt.Type() == MilestoneStartedEventType

		evt.Apply(gs, m.Version, m.GlobalPosition)
	}

	return true, ms, len(msgs) != 100, nil
}

func (c *Client) checkIn(ctx context.Context, gs *GroupState, cs *ConsumerState, period time.Duration) (bool, error) {
	cs.CheckedIn = time.Now()
	cs.NextCheckIn = cs.CheckedIn.Add(time.Duration(float64(period) * 1.1)) // give 10% leeway

	evt := &ConsumerCheckedIn{
		GroupName:     gs.Name,
		ConsumerState: *cs,
	}

	_, err := c.mdbc.WriteMessage(ctx, gomdb.StreamIdentifier{
		Category: GroupCategory,
		ID:       gs.Name,
	}, gomdb.ProposedMessage{
		ID:   uuid.Must(uuid.NewV4()).String(),
		Type: evt.Type(),
		Data: evt,
	}, gs.Version)
	if err != nil {
		if errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
			return false, nil
		}

		return false, fmt.Errorf("writing message: %w", err)
	}

	return true, nil
}

func (c *Client) manageGroup(ctx context.Context, gs *GroupState, cs *ConsumerState) (bool, error) {
	// has the leader expired or am I the leader and am about to expire?
	var (
		noLeader               = time.Now().After(gs.LeaderExpires)
		myLeadershipIsExpiring = gs.Leader == cs.ConsumerID && gs.LeaderExpires.Before(time.Now().Add(time.Second))
	)

	if noLeader || myLeadershipIsExpiring {
		evt := &LeaderDeclared{
			GroupName:  gs.Name,
			ConsumerID: cs.ConsumerID,
			Until:      time.Now().Add(5 * time.Second),
		}

		_, err := c.mdbc.WriteMessage(ctx, gomdb.StreamIdentifier{
			Category: GroupCategory,
			ID:       gs.Name,
		}, gomdb.ProposedMessage{
			ID:   uuid.Must(uuid.NewV4()).String(),
			Type: evt.Type(),
			Data: evt,
		}, gs.Version)
		if err != nil {
			if errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
				return false, nil
			}
			return false, fmt.Errorf("writing leader declared event: %w", err)
		}

		return true, nil
	}

	if cs.ConsumerID != gs.Leader {
		return false, nil
	}

	// have all consumers completed or died?
	if gs.thereAreIdleConsumers() && (len(gs.ActiveConsumers) == 0 || gs.activeConsumersHaveExpired()) && (gs.CurrentMilestone == nil || gs.CurrentMilestone.End <= gs.HighWaterMark) {
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
			GroupName: gs.Name,
			Milestone: next,
		}
		_, err := c.mdbc.WriteMessage(ctx, gomdb.StreamIdentifier{
			Category: GroupCategory,
			ID:       gs.Name,
		}, gomdb.ProposedMessage{
			ID:   uuid.Must(uuid.NewV4()).String(),
			Type: evt.Type(),
			Data: evt,
		}, gs.Version)
		if err != nil {
			if errors.Is(err, gomdb.ErrUnexpectedStreamVersion) {
				return false, nil
			}
			return false, fmt.Errorf("writing milestone started event: %w", err)
		}

		return true, nil
	}

	return false, nil
}

func (c *Client) readMessages(
	ctx context.Context,
	gs *GroupState,
	cs *ConsumerState,
	mh gomdb.MessageHandler,
	strat gomdb.PollingStrategy,
) (time.Duration, bool, error) {
	if gs.CurrentMilestone == nil || cs.MilestoneComplete {
		return time.Hour, false, nil
	}

	ms := gs.CurrentMilestone

	msgs, err := c.mdbc.GetCategoryMessages(
		ctx,
		gs.Category,
		gomdb.AsConsumerGroup(ms.Partitions[cs.ConsumerID], int64(len(ms.Partitions))),
		gomdb.FromPosition(cs.CurrentPosition+1),
		gomdb.WithCategoryBatchSize(100),
	)
	if err != nil {
		return time.Hour, false, fmt.Errorf("reading category messages: %w", err)
	}

	for _, m := range msgs {
		if m.GlobalPosition >= ms.End {
			cs.MilestoneComplete = true
			return time.Hour, true, nil
		}

		mh(m)
		cs.CurrentPosition = m.GlobalPosition
	}

	return strat(int64(len(msgs)), 100), false, nil
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
			Name:            group,
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

		evt.Apply(gs, m.Version, m.GlobalPosition)

		stateHandler(gs, evt, live)
	}, func(b bool) {
		live = b
	}, func(e error) {
		if e != nil {
			c.log.Printf("received error on subscription: %s", e)
		}
	})
}
