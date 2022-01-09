package cgroup

import (
	"time"
)

// GroupState is the complete state of a consumer group including all active and
// idle consumers, and the goals for the current milestone. All consumers in a
// group build an identical version on the GroupState so that they can make
// desicions asynchronously.
type GroupState struct {
	Version          int64
	Name             string
	Category         string
	Leader           string
	LeaderExpires    time.Time
	ActiveConsumers  map[string]*ConsumerState
	IdleConsumers    map[string]*ConsumerState
	CurrentMilestone *Milestone
}

func (gs *GroupState) activeConsumersHaveExpired() bool {
	active := false

	for _, cs := range gs.ActiveConsumers {
		active = active || cs.NextCheckIn.After(time.Now())
	}

	return !active
}

func (gs *GroupState) thereAreIdleConsumers() bool {
	idle := false

	for _, cs := range gs.IdleConsumers {
		idle = idle || gs.CurrentMilestone == nil || cs.MilestoneID == gs.CurrentMilestone.ID || cs.MilestoneID == 0
	}

	return idle
}

// ConsumerState contains the progress that a consumer has made towards the
// current milestone.
type ConsumerState struct {
	ConsumerID        string
	CurrentPosition   int64
	MilestoneID       int64
	MilestoneComplete bool
	Debt              []*DebtState
	CheckedIn         time.Time
	NextCheckIn       time.Time
}

// contains the progress that a consumer has made towards clearing their debt.
type DebtState struct {
	ParitionDebt
	CurrentPosition int64
	DebtCleared     bool
}

// Milestone
type Milestone struct {
	ID         int64
	From       int64                      // the inclusive global position to start from
	End        int64                      // the position to end before
	Partitions map[string]int64           // the index that each consumer should consume
	Debt       map[string][]*ParitionDebt // debt from the previous milestones that has been assigned to a consumer
}

func (ms *Milestone) initialStateFor(consumerID string) *ConsumerState {
	cs := &ConsumerState{
		MilestoneID:       ms.ID,
		ConsumerID:        consumerID,
		CurrentPosition:   ms.From - 1,
		MilestoneComplete: false,
	}

	for _, dbt := range ms.Debt[consumerID] {
		cs.Debt = append(cs.Debt, &DebtState{
			ParitionDebt:    *dbt,
			CurrentPosition: dbt.From - 1,
			DebtCleared:     false,
		})
	}

	return cs
}

// ParitionDebt
type ParitionDebt struct {
	GroupSize int64
	Partition int64
	From      int64 // the inclusive global position to start from
	End       int64 // the position to end before
}
