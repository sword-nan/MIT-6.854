package raft

import "fmt"

type MatchedIndex []int

func (m MatchedIndex) Len() int {
	return len(m)
}

func (m MatchedIndex) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m MatchedIndex) Less(i, j int) bool {
	return m[i] > m[j]
}

type Pair struct {
	start int
	end   int
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选者 term
	CandidateId  int // 候选者 id
	LastLogTerm  int // 最后一个 log entry 的 term
	LastLogIndex int // 最后一个 log entry 的 index
}

func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs{\n\tTerm: %d, \n\tCandidateId: %d, \n\tLastLogTerm: %d, \n\tLastLogIndex: %d, \n\n}", rva.Term, rva.CandidateId, rva.LastLogTerm, rva.LastLogIndex)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // receiver 的 term
	VoteGranted bool // 是否投票给候选者
}

func (rvp *RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply{\n\tTerm: %d, \n\tVoteGranted: %v\n}", rvp.Term, rvp.VoteGranted)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (aea *AppendEntriesArgs) String() string {
	return fmt.Sprintf(`
	AppendEntriesArgs{
		Term: %d,
		LeaderId: %d,
		PrevLogIndex: %d,
		PrevLogTerm: %d,
		Entries: %v,
		LeaderCommit: %d
	}`, aea.Term, aea.LeaderId, aea.PrevLogIndex, aea.PrevLogTerm, aea.Entries, aea.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // 冲突 entry 对应的 term
	XIndex  int // follower 中日志中冲突 entry term 对应的第一个 index
	XLen    int // follower 中日志长度
}

func (aer *AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply{\n\tTerm: %d, \n\tSuccess: %v\n}", aer.Term, aer.Success)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type SnapShot struct {
	LastIncludedIndex int
	Commands          []interface{}
	LastIncludedTerm  int
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("LogEntry{term: %d, index: %d, command: %v}", l.Term, l.Index, l.Command)
}
