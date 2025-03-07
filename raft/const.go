package raft

import "time"

type State int

// Time
const (
	SLEEPTIME     = 10 * time.Millisecond
	HEARTBEATTIME = 100 * time.Millisecond
	MONITORTIME   = 10 * time.Millisecond
)

// State
const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	if s == Follower {
		return "Follower"
	} else if s == Candidate {
		return "Candidate"
	} else if s == Leader {
		return "Leader"
	}
	return ""
}
