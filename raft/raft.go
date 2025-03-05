package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

// Time
const (
	SLEEPTIME     = 10 * time.Millisecond
	HEARTBEATTIME = 100 * time.Millisecond
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// apply command 后传输给 serverManager 的 channel
	applyCh chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// ----------------------------
	// persistent state on all servers
	currentTerm int
	votedFor    int   //  -1 表示还未投票
	state       State // 节点状态
	log         []LogEntry
	// launchTimer <-chan time.Time
	// for heartbeat
	heartbeatC chan struct{} // 心跳，sever 收到 heartC 时触发
	// endHeartbeat chan struct{} // 停止 heart beat
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex    []int
	matchedIndex []int
	// snap shot
	X        int // log start from the X index(initial value is 0)
	snapshot *SnapShot
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(rf.state)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	var snapshotState []byte
	if rf.snapshot != nil {
		snapshotState = rf.encodeSnapshot(
			rf.snapshot.LastIncludedIndex,
			rf.snapshot.Commands,
			rf.snapshot.LastIncludedTerm,
		)
	}
	rf.persister.Save(raftstate, snapshotState)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		// state       State
		votedFor int
		log      []LogEntry
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("readPersist: decode failed")
	} else {
		// rf.state = state
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	lastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot() {

}

func (rf *Raft) decodeSnapshot(snapshot []byte) (lastIncludedIndex int, commands []interface{}) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&commands) != nil {
		// DPrintf("decodeSnapshot: decode failed")
		log.Fatal("decodeSnapshot: decode failed")
	}
	return
}

func (rf *Raft) encodeSnapshot(lastIncludedIndex int, commands []interface{}, lastIncludedTerm int) []byte {
	var w bytes.Buffer
	e := labgob.NewEncoder(&w)
	e.Encode(lastIncludedIndex)
	e.Encode(commands)
	e.Encode(lastIncludedTerm)
	return w.Bytes()
}

// 根据日志的 index 映射得到其在 logs 中的下标
func (rf *Raft) logIndex2sliceIndex(index int) int {
	return index - rf.X
}

func (rf *Raft) sliceIndex2logIndex(index int) int {
	return index + rf.X
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// 解码得到新的 snapshot 内容
	var (
		lastIncludedIndex int           // 快照对应的日志 index
		lastIncludedTerm  int           // 快照对应的日志 term
		mergedCommands    []interface{} // 合并后的 commands
		// appliedSnapshot   []byte        // 应用到 server 的 snapshot
	)
	lastIncludedIndex, commands := rf.decodeSnapshot(snapshot)
	DPrintf("Insert snapshot server %d: %d %v\n", rf.me, lastIncludedIndex, commands)
	// if rf.snapshot != nil {
	// 	fmt.Printf("%v\n", rf.snapshot.Commands...)
	// }
	if index != lastIncludedIndex {
		log.Fatal("Snapshot: index != lastIncludedIndex")
	}
	// 将其和之前的快照进行合并
	if rf.snapshot != nil {
		mergedCommands = append(mergedCommands, rf.snapshot.Commands...)
	}
	mergedCommands = append(mergedCommands, commands...)

	rf.mu.Lock()
	lastIncludedTerm = rf.log[index-rf.X].Term
	rf.mu.Unlock()

	fmt.Println("Snapshot")

	rf.snapshot = &SnapShot{
		LastIncludedIndex: lastIncludedIndex,
		Commands:          mergedCommands,
		LastIncludedTerm:  lastIncludedTerm,
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	sliceIndex := rf.logIndex2sliceIndex(lastIncludedIndex)

	// 清除 index 之前的日志
	newLogs := []LogEntry{
		{ // no-op
			Term:  lastIncludedTerm,
			Index: lastIncludedIndex,
		},
	}
	rf.log = append(newLogs, rf.log[sliceIndex+1:]...)
	DPrintf("NewLogs: logs: %v\n", rf.log)
	rf.X = index
	rf.snapshot = &SnapShot{
		LastIncludedIndex: lastIncludedIndex,
		Commands:          mergedCommands,
		LastIncludedTerm:  lastIncludedTerm,
	}
	rf.persist()
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

func (rf *Raft) isUpToDate(term int, index int) (flag bool) {
	thisTerm, thisIndex := rf.getLastLogEntryTermIndex()
	// DPrintf("thisTerm %v thisIndex %v\nterm %v index %v\n", thisTerm, thisIndex, term, index)
	if term < thisTerm { // entry term 过时
		flag = false
	} else if term > thisTerm { // entry term 新
		flag = true
	} else if index >= thisIndex { // entry term 相同但 index 更大
		flag = true
	} else {
		flag = false
	}
	return
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 过时的消息直接拒绝
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm { // 更新 term, 退化为 follower 状态
		// DPrintf("server %d's term [%d] is bigger than server %d, term is [%d]\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		rf.becomeFollower(args.Term, -1)
	}
	updateToDate := rf.isUpToDate(args.LastLogTerm, args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && updateToDate { // 符合选举的条件
		// DPrintf("server %d's term [%d] is bigger than server %d, term is [%d]\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		if !rf.killed() {
			rf.persist()
		}
	} else {
		reply.VoteGranted = false
	}
	// fmt.Println(rf.me, args)
	// fmt.Println(rf.me, reply)
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			sliceIndex := rf.logIndex2sliceIndex(rf.lastApplied)
			log := rf.log[sliceIndex]
			rf.mu.Unlock()
			DPrintf("server %d commit/apply index [%d], log%s", rf.me, rf.lastApplied, &log)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(SLEEPTIME)
	}
}

// 同步执行
// func (rf *Raft) applier(isleader bool, start, end int) {
// 	if isleader {
// 		DPrintf("server %d execute leader commit", rf.me)
// 	}
// 	DPrintf("server %d commit %d-%d log entry %v, old commitIndex %d, new commitIndex %d", rf.me, start, end, rf.log[start:end+1], rf.commitIndex, end)
// 	for _, log := range rf.log[start : end+1] {
// 		if rf.killed() {
// 			return
// 		}
// 		// DPrintf("server %d commit log %s", rf.me, &log)
// 		rf.applyCh <- ApplyMsg{
// 			CommandValid: true,
// 			Command:      log.Command,
// 			CommandIndex: log.Index,
// 		}
// 		rf.lastApplied = log.Index
// 		rf.persist()
// 	}
// }

// 异步执行(不保证执行顺序和 commit 顺序一致)
// func (rf *Raft) applier() {
// 	for logs := range rf.applierC {
// 		if rf.killed() {
// 			close(rf.applierC)
// 			return
// 		}
// 		start, end := logs[0].Index, logs[len(logs)-1].Index
// 		DPrintf("server %d commit %d-%d log entry %v, new commitIndex %d", rf.me, start, end, logs, end)
// 		for _, log := range logs {
// 			rf.applyCh <- ApplyMsg{
// 				CommandValid: true,
// 				Command:      log.Command,
// 				CommandIndex: log.Index,
// 			}
// 			rf.lastApplied = log.Index
// 			rf.persist()
// 		}
// 	}
// }

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var isHeartbeat = len(args.Entries) == 0
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 过时的消息
		var t string
		if len(args.Entries) == 0 {
			t = "heartbeat"
		} else {
			t = "append entries"
		}
		DPrintf("server %d refuse the message from %d, %s, argsTerm: %d, server term: %d", rf.me, args.LeaderId, t, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}
	rf.becomeFollower(args.Term, args.LeaderId)
	if isHeartbeat { // heartbeat
		reply.Success = true
		rf.heartbeatC <- struct{}{}
	}

	reply.XLen = len(rf.log) + rf.X
	reply.Success = false

	prevLogSliceIndex := rf.logIndex2sliceIndex(args.PrevLogIndex)

	var (
		entries []LogEntry // 需要根据 follower 中的 snapshot lastIncludedIndex 值进行截断
		start   int        // 同上
	)

	// prevLog 不在 snapshot 中
	if prevLogSliceIndex > 0 {
		// 日志太短
		if prevLogSliceIndex > len(rf.log)-1 {
			return
		}

		// 日志不一致
		if rf.log[prevLogSliceIndex].Term != args.PrevLogTerm {
			xTerm := rf.log[prevLogSliceIndex].Term
			reply.XTerm = xTerm
			for i := prevLogSliceIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != xTerm {
					reply.XIndex = rf.log[i+1].Index
					return
				}
			}
		}
		entries = args.Entries
		start = prevLogSliceIndex + 1
	} else { // prevLog 在 snapshot 中
		start = 1
		if rf.X-args.PrevLogIndex < len(args.Entries) {
			entries = args.Entries[rf.X-args.PrevLogIndex:]
		}
	}

	reply.Success = true
	end := len(rf.log)
	flag := false

	var ii int
	// 寻找与 leader 日志保持不一致的 index
	for i, j := start, 0; i < end && j < len(entries); i, j = i+1, j+1 {
		if rf.log[i].Term != entries[j].Term { // 日志不一致，需要删除当前及后面所有的日志条目
			rf.log = rf.log[:i]
			entries = entries[j:]
			ii = i
			flag = true
			break
		}
	}

	if !isHeartbeat {
		if flag {
			DPrintf("server %d delete the logs from %d", rf.me, ii)
			// DPrintf("server %d, logs: %v", rf.me, rf.log)
		} else { // 如果没有日志不一致，直接追加日志
			// 需要判定追加的位置
			s := min(end-start, len(entries))
			entries = entries[s:]
			// DPrintf("%v", entries)
		}
		rf.log = append(rf.log, entries...)
		if !rf.killed() {
			rf.persist()
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		var end int // 更新后的 commitIndex
		if isHeartbeat {
			end = min(args.LeaderCommit, args.PrevLogIndex)
		} else {
			if len(entries) > 0 {
				end = min(args.LeaderCommit, entries[len(entries)-1].Index)
			} else {
				end = min(args.LeaderCommit, args.PrevLogIndex)
			}
		}
		rf.commitIndex = end
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 二分搜索
// 是否包含目标 term
func (rf *Raft) isContainTerm(logs []LogEntry, term int) (flag bool, index int) {
	left, right := 0, len(logs)-1
	for left <= right {
		mid := (left + right) / 2
		if logs[mid].Term == term {
			left = mid
			break
		} else if logs[mid].Term < term {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	if logs[left].Term != term {
		flag = false
		return
	}

	// term 不存在，寻找相同 term 的最后一个 index
	flag = true
	for i := left + 1; i < len(logs)-1; i++ {
		if logs[i].Term != term {
			index = i - 1
			break
		}
	}
	return
}

func (rf *Raft) appendEntries(term, leaderCommit, end int) {
	if rf.killed() {
		return
	}
	count := 0
	commitChan := make(chan bool)
	for i := range rf.peers {
		if i != rf.me {
			ii := i
			go func(i int) {
				rf.mu.Lock()
				start := rf.nextIndex[i]
				rf.mu.Unlock()
				for {
					rf.mu.Lock()
					sliceStart := rf.logIndex2sliceIndex(start)
					sliceEnd := rf.logIndex2sliceIndex(end)
					// 不是 leader
					// start 比快照中的 index 小
					// nextIndex[i] 比 end 大 (说明已经有更长的日志成功添加到 follower 中)
					if rf.state != Leader || start <= rf.X || rf.nextIndex[i] > end || rf.killed() {
						rf.mu.Unlock()
						commitChan <- false
						break
					}
					// DPrintf("server %d send entries to server %d, nextIndex: %v commitIndex: %v lastLogIndex: %d, entries: %v", rf.me, i, rf.nextIndex[i], leaderCommit, end, rf.log[start:end+1])
					prevLog := rf.log[sliceStart-1]
					var entries = make([]LogEntry, sliceEnd-sliceStart+1)
					copy(entries, rf.log[sliceStart:sliceEnd+1])
					args := AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: prevLog.Index,
						PrevLogTerm:  prevLog.Term,
						Entries:      entries,
						LeaderCommit: leaderCommit,
					}
					rf.mu.Unlock()

					// 发 rpc
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &args, &reply)
					if !ok {
						time.Sleep(SLEEPTIME)
						continue
					}

					// 发送成功
					rf.mu.Lock()
					if rf.currentTerm != term {
						rf.mu.Unlock()
						commitChan <- false
						break
					}

					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term, -1)
						rf.mu.Unlock()
						commitChan <- false
						break
					}

					if reply.Success { // 当前 term 下，且 append 日志条目成功
						if end+1 > rf.nextIndex[i] {
							rf.nextIndex[i] = end + 1
							rf.matchedIndex[i] = end
						}
						rf.mu.Unlock()
						commitChan <- true
						break
					} else {
						// 日志不一致，需要调整 start
						if reply.XLen-1 < args.PrevLogIndex {
							start = reply.XLen
							DPrintf("follower %d's log is too short: server %d appendEntries start: %d", i, rf.me, start)
							rf.mu.Unlock()
							continue
						}
						var copyLogs = make([]LogEntry, len(rf.log))
						copy(copyLogs, rf.log)
						lastIncludedIndex := rf.X
						rf.mu.Unlock()
						flag, index := rf.isContainTerm(copyLogs, reply.XTerm)
						if !flag {
							start = reply.XIndex
							DPrintf("leader doesn't have XTerm: server %d appendEntries start: %d", rf.me, start)
							continue
						} else {
							start = lastIncludedIndex + index
							DPrintf("leader has XTerm: server %d appendEntries start: %d", rf.me, start)
							continue
						}
					}
				}
			}(ii)
		}
	}

	go func() { // 监听是否达到了 commit 的条件
		flag := true
		for i := 0; i < len(rf.peers)-1; i++ {
			if val := <-commitChan; val {
				count++
				if count >= len(rf.peers)/2 && flag && !rf.killed() { // 投票数过半且没有更新过 commitIndex
					rf.mu.Lock()
					if rf.state == Leader && rf.currentTerm == term { // 状态为 leader 且 term 没有变
						if rf.commitIndex < end { // 防止已经有其他协程更新过 commitIndex
							// rf.applierC <- rf.log[rf.commitIndex+1 : end+1]
							// rf.applier(true, rf.commitIndex+1, end)
							rf.commitIndex = end
						}
					}
					rf.mu.Unlock()
					flag = false
				}
			}
		}
	}()
}

func (rf *Raft) heartbeat() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for i := range rf.peers { // 广播心跳
		if i != rf.me {
			ii := i
			go func(i int) {
				// 发送失败则重新发送
				for !rf.killed() {
					// DPrintf("Term %d: HeartBeat: server %d send the heartbeat to server %d\n", rf.currentTerm, rf.me, i)
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						break
					}
					sliceIndex := rf.logIndex2sliceIndex(rf.nextIndex[i] - 1)
					var prevLog LogEntry
					if sliceIndex < 0 {
						prevLog = LogEntry{
							Term:  rf.snapshot.LastIncludedIndex,
							Index: rf.snapshot.LastIncludedTerm,
						}
					} else {
						prevLog = rf.log[sliceIndex]
					}
					// fmt.Printf("sliceIndex: %d\n", sliceIndex)
					args := AppendEntriesArgs{
						Term:         term,
						LeaderId:     rf.me,
						PrevLogIndex: prevLog.Index,
						PrevLogTerm:  prevLog.Term,
						Entries:      []LogEntry{},
						LeaderCommit: commitIndex,
					}
					rf.mu.Unlock()
					reply := AppendEntriesReply{}

					// DPrintf("server %d send heartbeat to server %d, args: %s", rf.me, i, &args)

					ok := rf.sendAppendEntries(i, &args, &reply)

					if !ok {
						time.Sleep(SLEEPTIME)
						continue
					}

					// 发送成功
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						// DPrintf("server %d, reply.Term: %d rf.currentTerm: %d term: %d\n", rf.me, reply.Term, rf.currentTerm, term)
						rf.becomeFollower(reply.Term, -1)
					}
					break
					// DPrintf("server %d send the heartbeat to server %d, error!\n", rf.me, i)
				}
			}(ii)
		}
	}
}

func (rf *Raft) periodicallyHeartbeat() {
	for !rf.killed() && rf.isTargetState(Leader) {
		<-time.After(HEARTBEATTIME)
		go rf.heartbeat()
	}
}

func (rf *Raft) becomeCandidate(term int) {
	rf.state = Candidate
	rf.currentTerm = term
	rf.votedFor = rf.me
	if !rf.killed() {
		rf.persist()
	}
}

func (rf *Raft) becomeFollower(term int, votedFor int) {
	if rf.state == Leader {
		DPrintf("Term %d : server %d 状态由 Leader 转化为 Follower term: %d\n", term, rf.me, rf.currentTerm)
		// close(rf.endHeartbeat)
	}
	// 重置选举时间
	// 防止收到投票请求后有人当选 leader 但此时定时器已经触发
	// 自己开始投票，出现需要多发一轮投票的情况
	rf.heartbeatC <- struct{}{}
	// } else if rf.state == Candidate {
	// 	// DPrintf("Term %d : server %d 状态由 Candidate 转化为 Follower term: %d\n", term, rf.me, rf.currentTerm)
	// }
	// }else{
	// 	DPrintf("Term %d : server %d 状态由 Follower 转化为 Follower term: %d\n", term, rf.me, rf.currentTerm)
	// }
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	if !rf.killed() {
		rf.persist()
	}
}

func (rf *Raft) becomeLeader() {
	DPrintf("Term: %v, server %v become leader\n", rf.currentTerm, rf.me)
	rf.state = Leader
	go rf.heartbeat()
	go rf.periodicallyHeartbeat()
	rf.matchedIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.matchedIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
}

func (rf *Raft) isTargetState(state State) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == state
}

func (rf *Raft) getLastLogEntryTermIndex() (term int, index int) {
	lastLogEntry := rf.log[len(rf.log)-1]
	term = lastLogEntry.Term
	index = lastLogEntry.Index
	return
}

func (rf *Raft) election() {
	if rf.killed() || rf.isTargetState(Leader) {
		return
	}
	rf.mu.Lock()
	rf.becomeCandidate(rf.currentTerm + 1)
	lastEntryTerm, lastEntryIndex := rf.getLastLogEntryTermIndex()
	term := rf.currentTerm
	DPrintf("Term %d: server %d start election\n", rf.currentTerm, rf.me)
	rf.mu.Unlock()
	count := 0
	voteChan := make(chan bool) // 监听投票结果
	for i := range rf.peers {   // 广播选举
		if i != rf.me {
			ii := i
			go func(i int) {
				for {
					// 直接 return 会存在内存泄漏
					// 收集投票的协程无法关闭
					if rf.killed() {
						voteChan <- false
						return
					}
					args := RequestVoteArgs{
						Term:         term,
						CandidateId:  rf.me,
						LastLogTerm:  lastEntryTerm,
						LastLogIndex: lastEntryIndex,
					}
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &args, &reply) // 直到发送成功
					if !ok {
						time.Sleep(SLEEPTIME)
						continue
					}
					rf.mu.Lock()

					if args.Term != rf.currentTerm { // 不是当前任期的投票，废弃
						// fmt.Println("yes")
						rf.mu.Unlock()
						voteChan <- false // 为了优雅地关闭监听投票的协程
						break
					}

					if reply.Term > rf.currentTerm { // 对方 term 比自身 term 大，退化为 follower
						// DPrintf("Election: 接收到来自 server %d 的回复，term %d 比 leader %d 的 term %d 大，由 %s 退化为 Follower\n", ii, reply.Term, rf.me, rf.currentTerm, rf.state)
						rf.becomeFollower(reply.Term, -1)
						rf.mu.Unlock()
						voteChan <- false
						break
					}

					// 只接收当前任期的投票
					// 是否接收投票
					// 仅当 reply 回复了 true 且状态仍然是 candidate
					// reply.term <= rf.currentTerm
					if reply.VoteGranted { // 获得该投票
						// DPrintf("Election: Term %d, server %d get the vote from server %d\n", rf.currentTerm, rf.me, ii)
						rf.mu.Unlock()
						voteChan <- true
					} else {
						// DPrintf("Term: %v, server %v is not candidate\n", rf.currentTerm, rf.me)
						rf.mu.Unlock()
						voteChan <- false
					}
					break
					// DPrintf("server %d request vote error!\n", rf.me)
				}
			}(ii)
		}
	}

	go func() { // 检测是否投票达到了大多数
		flag := true
		for i := 0; i < len(rf.peers)-1; i++ {
			if val := <-voteChan; val { // 投票成功
				count++
				if count >= len(rf.peers)/2 && flag && !rf.killed() {
					rf.mu.Lock()
					if rf.state == Candidate && rf.currentTerm == term {
						rf.becomeLeader()
					}
					rf.mu.Unlock()
					flag = false
				}
			}
		}
	}()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + rf.X
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (3B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
		if !rf.killed() {
			rf.persist()
			go rf.broadcastLogs()
		}
		// DPrintf("Term %d, server %d get the command from client. log: %v\n", rf.currentTerm, rf.me, rf.log)
		// 广播日志保证一致性
	}
	return index, term, isLeader
}

func (rf *Raft) broadcastLogs() {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	end := rf.sliceIndex2logIndex(len(rf.log) - 1)
	rf.mu.Unlock()
	// DPrintf("server %d, send the batch command to follower, log: %v\n", rf.me, rf.log)
	rf.appendEntries(term, leaderCommit, end)
}

// func (rf *Raft) checkBatchCommand() {
// 	var timer <-chan time.Time
// 	currentBatchLen := 0
// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		select {
// 		case <-rf.batchC: // 如果有新的命令
// 			timer = time.After(100 * time.Millisecond)
// 			currentBatchLen += 1
// 			if currentBatchLen == TargetBatchCommandLen {
// 				currentBatchLen = 0
// 				// 发送日志
// 				rf.broadcastLogs()
// 			}
// 		case <-timer:
// 			currentBatchLen = 0
// 			timer = nil
// 			// 发送日志
// 			rf.broadcastLogs()
// 		}
// 	}
// }

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetElectionTimer() time.Duration {
	// pause for a random amount of time between 300 and 500
	// milliseconds.
	rand.Seed(time.Now().UnixNano())
	ms := 300 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.heartbeatC: // 什么都不做
			// DPrintf("server %d 收到心跳\n", rf.me)
		case <-time.After(rf.resetElectionTimer()): // 否则进行选举
			go rf.election()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.heartbeatC = make(chan struct{})
	// rf.endHeartbeat = make(chan struct{})
	// rf.applierC = make(chan []LogEntry, 10)
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 初始填充一个空 Log 以保证索引从 1 开始
	// 符合 raft 论文描述要求
	rf.log = []LogEntry{
		{0, 0, 0},
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()
	// go rf.applySnapshot()
	return rf
}
