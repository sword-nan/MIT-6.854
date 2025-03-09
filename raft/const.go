package raft

import "time"

type State int

// Time
const (
	SleepTime               = 10 * time.Millisecond
	HeartTime               = 200 * time.Millisecond
	MonitorSnapshot         = 20 * time.Millisecond
	MonitorNextIndexTime    = 20 * time.Millisecond
	MonitorMatchedIndexTime = 10 * time.Millisecond
)

// 防止超时导致的 FAIL
// 开启多个协程发送消息可以减小---长时间网络延迟---的概率
const MaxGoroutines = 50

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
