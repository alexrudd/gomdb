package cgroup

import (
	"fmt"
	"time"

	"github.com/alexrudd/gomdb"
)

// event is an interface that all consumer group coordination events must
// conform to.
type event interface {
	// Apply this event to the provided GroupState.
	Apply(gs *GroupState, ver int64)
	// Type returns the type name of this event.
	Type() string
}

const (
	// LeaderDeclaredEventType is the event type for the LeaderDeclared event.
	LeaderDeclaredEventType = "LeaderDeclared"
	// ConsumerCheckedInEventType is the event type for the ConsumerCheckedIn
	// event.
	ConsumerCheckedInEventType = "ConsumerCheckedIn"
	// MilestoneStartedEventType is the event type for the MilestoneStarted
	// event.
	MilestoneStartedEventType = "MilestoneStarted"
)

func eventFromMessage(m *gomdb.Message) (event, error) {
	var evt event

	switch m.Type {
	case LeaderDeclaredEventType:
		evt = &LeaderDeclared{}
	case ConsumerCheckedInEventType:
		evt = &ConsumerCheckedIn{}
	case MilestoneStartedEventType:
		evt = &MilestoneStarted{}
	default:
		return nil, fmt.Errorf("unknown event type: %s", m.Type)
	}

	if err := m.UnmarshalData(evt); err != nil {
		return nil, fmt.Errorf("unmarshalling event to %T: %w", err)
	}

	return evt, nil
}

// LeaderDeclared

// LeaderDeclared is published when a consumer declares itself the new leader.
// The new leader should specify a time at which they will reliquish leadership
// though they may also choose to extend their leadership by publishing another
// LeaderDeclared event before leadership expirary.
type LeaderDeclared struct {
	GroupName  string
	ConsumerID string
	Until      time.Time
}

// Type returns the LeaderDeclared event type.
func (e *LeaderDeclared) Type() string {
	return LeaderDeclaredEventType
}

// Apply will set the leader of the provided GroupState that the consumer ID
// stored in the event and update the expirary time.
func (e *LeaderDeclared) Apply(gs *GroupState, ver int64) {
	gs.Version = ver
	gs.Leader = e.ConsumerID
	gs.LeaderExpires = e.Until
}

// ConsumerCheckedIn

// ConsumerCheckedIn is published periodically by each consumer so that its
// progress can be tracked by all group members. Once a consumer has reached
// The current milestone's target, the Complete bool should be set to true.
type ConsumerCheckedIn struct {
	GroupName string
	ConsumerState
}

// Type returns the ConsumerCheckedIn event type.
func (e *ConsumerCheckedIn) Type() string {
	return ConsumerCheckedInEventType
}

// Apply will update the active consumer state in the GroupState, and move the
// consumer to the idle set if it has completed its milestone.
func (e *ConsumerCheckedIn) Apply(gs *GroupState, ver int64) {
	gs.Version = ver

	// if complete put the consumer in the idle set, else put the consumer in
	// the active set.
	if e.MilestoneComplete {
		gs.IdleConsumers[e.ConsumerID] = &e.ConsumerState
		delete(gs.ActiveConsumers, e.ConsumerID)
	} else {
		gs.ActiveConsumers[e.ConsumerID] = &e.ConsumerState
		delete(gs.IdleConsumers, e.ConsumerID)
	}
}

// MilestoneStarted

// MilestoneStarted is published by the group's leader where a new milestone
// is set. All consumers should setup new subscriptions to work towards the new
// milestone's goals.
type MilestoneStarted struct {
	GroupName string
	Milestone
}

// Type returns the MilestoneStarted event type.
func (e *MilestoneStarted) Type() string {
	return MilestoneStartedEventType
}

// Apply will set the current milestone of the group to the milestone in the
// event.
func (e *MilestoneStarted) Apply(gs *GroupState, ver int64) {
	gs.Version = ver
	gs.CurrentMilestone = &e.Milestone
}
