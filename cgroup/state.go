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

// ConsumerState contains the progress that a consumer has made towards the
// current milestone.
type ConsumerState struct {
	ConsumerID        string
	CurrentPosition   int64
	MilestoneComplete bool
	Debt              []*DebtState
}

// contains the progress that a consumer has made towards clearing their debt.
type DebtState struct {
	ParitionDebt
	CurrentPosition int64
	DebtCleared     bool
}

// Milestone
type Milestone struct {
	From       int64                      // the inclusive global position to start from
	End        int64                      // the position to end before
	Partitions map[string]int             // the index that each consumer should consume
	Debt       map[string][]*ParitionDebt // debt from the previous milestones that has been assigned to a consumer
}

// ParitionDebt
type ParitionDebt struct {
	GroupSize int
	Partition int
	From      int64 // the inclusive global position to start from
	End       int64 // the position to end before
}
