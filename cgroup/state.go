package cgroup

import "time"

type state struct {
	version      int64                // version of the group state
	leader       string               // current leader of the group
	active       map[string]*consumer // active consumers in current milestone
	idle         map[string]*consumer // idle consumers ready for next milestone
	lastComplete time.Time            // time of last partition completion
	milestone    *milestone           // current milestone
}

type consumer struct {
	id       string // unique ID of the consumer
	position int64  // current position of the consumer
	complete bool   // has this consumer completed their partition
}

type milestone struct {
	id         string           // unique ID of the milestone
	from       int64            // the inclusive global position to start from
	ends       int64            // the position to end before
	partitions map[string]int   // partition assignments for consumers
	debt       map[string]int64 // debt carried over from previous milestones
}

// Events

type LeaderDeclared struct {
	ID string
}

func (cmd *LeaderDeclared) apply(s *state) {
	s.leader = cmd.ID
}

type ConsumerCheckedIn struct {
	ID       string
	Position int64
	Complete bool
}

func (cmd *ConsumerCheckedIn) apply(s *state) {
	if !cmd.Complete {
		s.active[cmd.ID] = &consumer{
			id:       cmd.ID,
			position: cmd.Position,
			complete: cmd.Complete,
		}
	} else if s.active[cmd.ID] != nil {
		s.lastComplete = time.Now()
		delete(s.active, cmd.ID)
	}

	s.idle[cmd.ID] = &consumer{
		id:       cmd.ID,
		position: cmd.Position,
		complete: cmd.Complete,
	}
}

type MilestoneStarted struct {
	ID         string
	From       int64
	Ends       int64
	Partitions map[string]int
	Debt       map[string]int64
}

func (cmd *MilestoneStarted) apply(s *state) {
	s.milestone = &milestone{
		id:         cmd.ID,
		from:       cmd.From,
		ends:       cmd.Ends,
		partitions: cmd.Partitions,
		debt:       cmd.Debt,
	}
}
