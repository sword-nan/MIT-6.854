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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
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
	votedFor    int //  -1 表示还未投票
	log         []LogEntry
	// launchTimer <-chan time.Time
	// for heartbeat
	heartbeatC chan struct{} // 心跳，sever 收到 heartC 时触发
	// endHeartbeat chan struct{} // 停止 heart beat
	// volatile state on all servers
	state       State // 节点状态
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex    []int
	matchedIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) encodedState() []byte {
	var term, votedFor = rf.currentTerm, rf.votedFor
	var entries = make([]LogEntry, len(rf.log))
	copy(entries, rf.log)
	return encodeRaftstate(term, votedFor, entries)
}

func (rf *Raft) getFirstLogEntry() LogEntry {
	var entry = rf.log[0]
	return entry
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
	rf.persister.Save(rf.encodedState(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readRaftstate(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	term, votedFor, logEntries := decodeRaftstate(data)
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = logEntries
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	lastIncludedIndex, _ := decodeSnapshot(data)
	// 执行 commands
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.lastApplied < rf.commitIndex {
			var X, commitIndex, lastAppliedIndex = rf.getFirstLogEntry().Index, rf.commitIndex, rf.lastApplied
			var entries = make([]LogEntry, commitIndex-lastAppliedIndex)
			copy(entries, rf.log[lastAppliedIndex-X+1:commitIndex-X+1])
			rf.mu.RUnlock()
			DPrintf("server %d commit/apply index [%d-%d] commands", rf.me, lastAppliedIndex+1, commitIndex)
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
			}
			rf.mu.Lock()
			// lastApplied 可能已经被 InstallSnapshot 更新了
			rf.lastApplied = max(rf.lastApplied, commitIndex)
			rf.mu.Unlock()
		} else {
			rf.mu.RUnlock()
		}
		// 可以改成条件变量进行触发
		time.Sleep(SleepTime)
	}
}

// 根据日志的 index 映射得到其在 logs 中的下标
func (rf *Raft) logIndex2sliceIndex(index int) int {
	return index - rf.getFirstLogEntry().Index
}

func (rf *Raft) sliceIndex2logIndex(index int) int {
	return index + rf.getFirstLogEntry().Index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 外部调用
	// 正常应该是有一个协程监听当前日志长度和快照长度，当比例达到 4 : 1 时进行快照
	if index <= rf.getFirstLogEntry().Index { // installSnapshot 加速了整个过程
		return
	}
	truncatedIndex := rf.logIndex2sliceIndex(index)
	rf.log = rf.log[truncatedIndex:]
	rf.log[0].Command = nil
	rf.persister.Save(rf.encodedState(), snapshot)
	DPrintf("Insert snapshot to server %d: truncatedIndex: %d, truncatedLogIndex: %d", rf.me, truncatedIndex, index)
}

func (rf *Raft) installSnapshot(server, term, lastIncludedIndex, lastIncludedTerm int, snapshot []byte) {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.RUnlock()
			break
		}
		rf.mu.RUnlock()
		DPrintf("server %d installSnapshot to server %d, lastIncludedIndex: %d", rf.me, server, lastIncludedIndex)
		args := InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Data:              snapshot,
		}
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if !ok {
			time.Sleep(SleepTime)
			continue
		}

		rf.mu.Lock()
		// 过时的回复
		if rf.currentTerm != term {
			rf.mu.Unlock()
			break
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term, -1)
			rf.mu.Unlock()
			break
		}

		if lastIncludedIndex > rf.matchedIndex[server] {
			rf.matchedIndex[server] = lastIncludedIndex
			rf.nextIndex[server] = lastIncludedIndex + 1
		}

		rf.mu.Unlock()
		break
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 快照需要定期调用
// 不能每次 leader 调用 Snapshot 时才广播
// 如下原因:
/*
* 如果出现了 follower 重连或者重启即存在一个 follower 落后太多
* 此时对于 leader-follower 会出现一个空窗期 (follower 无法跟上 leader)
	* 由于 leader 提交日志数没有达到预先设定的值，无法生成新的快照因此无法广播 InstallSnapshot
	* follower 的日志完全处于 leader 快照的前缀，通过 AppendEntries 无法进行日志复制 (会一直提示 follower 的日志过短，修改 start 的值)
	*
*/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm { // 过时消息
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, -1)
	}
	rf.heartbeatC <- struct{}{}

	// 等待 applyLog 协程去提交
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	DPrintf("to server %d, lastApplied: %d, lastIncludedIndex: %d", rf.me, rf.lastApplied, args.LastIncludedIndex)
	if args.LastIncludedIndex >= rf.sliceIndex2logIndex(len(rf.log))-1 { // follower 日志是快照的前缀
		// 删除 follower 中的所有日志
		// 执行快照中的命令
		rf.log = nil
		rf.log = []LogEntry{
			{ // no-op
				Term:  args.LastIncludedTerm,
				Index: args.LastIncludedIndex,
			},
		}
	} else { // follower 中部分日志是快照的前缀
		// 保留后缀
		truncatedIndex := rf.logIndex2sliceIndex(args.LastIncludedIndex)
		rf.log = rf.log[truncatedIndex:]
		rf.log[0].Command = nil
	}

	rf.persister.Save(rf.encodedState(), args.Data)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	DPrintf("server %d, commit the snapshot [--%d]", rf.me, args.LastIncludedIndex)
	// 必须持有锁，不然可能导致提交乱序
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) isUpToDate(term int, index int) (flag bool) {
	thisTerm, thisIndex := rf.getLastLogEntryTermIndex()
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
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 过时的消息直接拒绝
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm { // 更新 term, 退化为 follower 状态
		rf.becomeFollower(args.Term, -1)
	}
	updateToDate := rf.isUpToDate(args.LastLogTerm, args.LastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && updateToDate { // 符合选举的条件
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.heartbeatC <- struct{}{}
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	var isHeartbeat = len(args.Entries) == 0
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 过时的消息
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term, -1)
	}
	if isHeartbeat { // heartbeat
		rf.heartbeatC <- struct{}{}
	}

	reply.XLen = rf.sliceIndex2logIndex(len(rf.log))
	reply.Success = false

	firstLogEntry := rf.getFirstLogEntry()
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
			// 之前没有考虑位于快照的 index
			// 返回快照的 index
			reply.XIndex = rf.getFirstLogEntry().Index
			return
		}
		// 一致
		entries = args.Entries
		start = prevLogSliceIndex + 1
	} else { // prevLog 在 snapshot 中
		start = 1
		if firstLogEntry.Index-args.PrevLogIndex < len(args.Entries) { // 截断
			entries = args.Entries[firstLogEntry.Index-args.PrevLogIndex:]
		} else {
			entries = nil
		}
	}

	reply.Success = true
	end := len(rf.log)
	flag := false

	// 寻找与 leader 日志保持不一致的 index
	for i, j := start, 0; i < end && j < len(entries); i, j = i+1, j+1 {
		if rf.log[i].Term != entries[j].Term { // 日志不一致，需要删除当前及后面所有的日志条目
			rf.log = rf.log[:i]
			entries = entries[j:]
			flag = true
			break
		}
	}
	// 考虑 WAL
	// 数据落盘前日志先落盘
	if !isHeartbeat {
		if !flag {
			// 如果没有日志不一致，直接追加日志
			// 需要判定追加的位置
			s := min(end-start, len(entries))
			entries = entries[s:]
		}
		rf.log = append(rf.log, entries...)
	} else {
		entries = nil
	}

	if args.LeaderCommit > rf.commitIndex {
		var end int // 更新后的 commitIndex
		if len(entries) > 0 {
			end = min(args.LeaderCommit, entries[len(entries)-1].Index)
		} else {
			end = min(args.LeaderCommit, args.PrevLogIndex)
		}
		rf.commitIndex = end
	}
}

// 二分搜索
// 是否包含目标 term
// 存在则一并返回最后一个 index
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

// 监听 leader 的 commitIndex slice
// 如果超过一半的数目比当前的 leader commitIndex 大
// 修改为目标值
func (rf *Raft) monitorMatchedIndex(term int) {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.state != Leader || term != rf.currentTerm {
			rf.mu.RUnlock()
			break
		}
		var commitIndex int
		var middleIndex = len(rf.peers)/2 - 1
		var matchedIndex = make([]int, len(rf.peers))
		commitIndex = rf.commitIndex
		copy(matchedIndex, rf.matchedIndex)
		rf.mu.RUnlock()
		// 从大到小排序
		sort.Sort(MatchedIndex(matchedIndex))
		// 仅允许提交当前任期内的日志
		// 参考 Figure8
		majorityMatchedIndex := matchedIndex[middleIndex]
		if commitIndex < majorityMatchedIndex {
			rf.mu.Lock()
			majorityMatchedIndexSliceIndex := rf.logIndex2sliceIndex(majorityMatchedIndex)
			if rf.state == Leader && // 仍然为 leader
				term == rf.currentTerm && // 且为当前 term 的 leader
				majorityMatchedIndex > rf.commitIndex && // 该值大于目前的 commitIndex
				rf.log[majorityMatchedIndexSliceIndex].Term == term { // 且为当前 term 下生成的 log
				DPrintf("server %d, majorityMatchedIndex: %d, matchedIndex: %v, term: %d, commitIndex: %d, ", rf.me, majorityMatchedIndex, rf.matchedIndex, rf.currentTerm, rf.commitIndex)
				rf.commitIndex = majorityMatchedIndex
			}
			rf.mu.Unlock()
		}
		time.Sleep(MonitorMatchedIndexTime)
	}
}

func (rf *Raft) monitorSnapshot(server, term int) {
	installSnapshotRPCSet := make(map[int]int)
	for !rf.killed() {
		rf.mu.RLock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.RUnlock()
			break
		}
		firstLogEntry := rf.getFirstLogEntry()

		if rf.matchedIndex[server] < firstLogEntry.Index { // 需要发送快照让 follower catch up
			lastIncludedIndex := firstLogEntry.Index
			lastIncludedTerm := firstLogEntry.Term
			snapshot := rf.persister.ReadSnapshot()
			if installSnapshotRPCSet[lastIncludedIndex] < MaxGoroutines {
				go rf.installSnapshot(server, term, lastIncludedIndex, lastIncludedTerm, snapshot)
				installSnapshotRPCSet[lastIncludedIndex] += 1
			}
		}
		rf.mu.RUnlock()
		time.Sleep(MonitorNextIndexTime)
	}
}

// 监听 follower 的 nextIndex
// 如果 nextIndex 值小于等于 leader 日志长度则进行复制
// 定时监听发送 InstallSnapshot 还是 AppendEntries
func (rf *Raft) monitorNextIndex(server, term int) {
	appendEntriesRPCSet := make(map[Pair]int)
	for !rf.killed() {
		rf.mu.RLock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.RUnlock()
			break
		}
		firstLogEntry := rf.getFirstLogEntry()

		if rf.nextIndex[server] < rf.sliceIndex2logIndex(len(rf.log)) && rf.nextIndex[server] > firstLogEntry.Index { // 发送 appendEntries
			leaderCommit := rf.commitIndex
			start := rf.nextIndex[server]
			// 需要利用快照下标进行映射
			end := rf.sliceIndex2logIndex(len(rf.log)) - 1
			key := Pair{start: start, end: end}
			if appendEntriesRPCSet[key] < MaxGoroutines {
				go rf.appendEntries(server, term, leaderCommit, start, end)
				appendEntriesRPCSet[key] += 1
			}
		}
		rf.mu.RUnlock()
		time.Sleep(MonitorNextIndexTime)
	}
}

func (rf *Raft) appendEntries(server, term, leaderCommit, start, end int) {
	for !rf.killed() {
		rf.mu.RLock()
		// 是 leader
		// 且为当前任期的 leader (可能出现宕机，然后在 term + n 时重新当选 leader)
		// 判定左边界和右边界
		// nextIndex[i] 比 end 大 (说明已经有更长的日志成功添加到 follower 中)
		firstLogEntry := rf.getFirstLogEntry()
		if rf.state != Leader || term != rf.currentTerm || start <= firstLogEntry.Index || rf.nextIndex[server] > end {
			rf.mu.RUnlock()
			break
		}
		DPrintf("server %d send the logs from %d to %d to server %d, leaderCommit: %d, logLength: %d, lastIncludedIndex: %d", rf.me, start, end, server, leaderCommit, len(rf.log), firstLogEntry.Index)
		sliceStart := rf.logIndex2sliceIndex(start)
		sliceEnd := rf.logIndex2sliceIndex(end)
		prevLog := rf.log[sliceStart-1]
		// 待发送的日志条目
		var entries = make([]LogEntry, sliceEnd-sliceStart+1)
		copy(entries, rf.log[sliceStart:sliceEnd+1])
		rf.mu.RUnlock()
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		// 发 rpc
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			time.Sleep(SleepTime)
			continue
		}

		// 发送成功
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()
			break
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term, -1)
			rf.mu.Unlock()
			break
		}

		if reply.Success { // 当前 term 下，且 append 日志条目成功
			if end+1 > rf.nextIndex[server] {
				rf.nextIndex[server] = end + 1
				rf.matchedIndex[server] = end
			}
			rf.mu.Unlock()
			break
		} else { // 日志不一致，需要调整 start
			if reply.XLen-1 < args.PrevLogIndex {
				start = reply.XLen
				DPrintf("follower %d's log is too short: server %d appendEntries start: %d", server, rf.me, start)
				rf.mu.Unlock()
				continue
			}
			var copyLogs = make([]LogEntry, len(rf.log))
			copy(copyLogs, rf.log)
			lastIncludedIndex := rf.getFirstLogEntry().Index
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
}

// 对 server 发送心跳
func (rf *Raft) heartbeat(server, term, commitIndex int) {
	for !rf.killed() {
		var prevLog LogEntry
		rf.mu.RLock()
		if rf.state != Leader || term != rf.currentTerm {
			rf.mu.RUnlock()
			break
		}
		sliceIndex := rf.logIndex2sliceIndex(rf.nextIndex[server] - 1)
		// 快照中已经包含了该下标
		if sliceIndex < 0 {
			prevLog = LogEntry{
				Term:  rf.getFirstLogEntry().Term,
				Index: rf.getFirstLogEntry().Index,
			}
		} else {
			prevLog = rf.log[sliceIndex]
		}
		rf.mu.RUnlock()
		// fmt.Printf("sliceIndex: %d\n", sliceIndex)
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      []LogEntry{},
			LeaderCommit: commitIndex,
		}
		reply := AppendEntriesReply{}

		// DPrintf("server %d send heartbeat to server %d, args: %s", rf.me, i, &args)

		ok := rf.sendAppendEntries(server, &args, &reply)

		if !ok {
			time.Sleep(SleepTime)
			continue
		}

		// 发送成功
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// DPrintf("server %d, reply.Term: %d rf.currentTerm: %d term: %d\n", rf.me, reply.Term, rf.currentTerm, term)
			rf.becomeFollower(reply.Term, -1)
		}
		rf.mu.Unlock()
		break
		// DPrintf("server %d send the heartbeat to server %d, error!\n", rf.me, i)
	}
}

// 广播心跳
func (rf *Raft) boradcastHeartbeat() {
	rf.mu.RLock()
	// DPrintf("Term %d: server %d broadcast the heartbeat", rf.currentTerm, rf.me)
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	rf.mu.RUnlock()
	for i := range rf.peers {
		ii := i
		if i != rf.me {
			// DPrintf("Term %d: HeartBeat: server %d send the heartbeat to server %d\n", rf.currentTerm, rf.me, ii)
			go rf.heartbeat(ii, term, commitIndex)
		}
	}
}

// 定期广播心跳
func (rf *Raft) periodicallyHeartbeat() {
	for !rf.killed() {
		<-time.After(HeartTime)
		rf.boradcastHeartbeat()
	}
}

func (rf *Raft) becomeCandidate(term int) {
	rf.state = Candidate
	rf.currentTerm = term
	rf.votedFor = rf.me
}

func (rf *Raft) becomeFollower(term int, votedFor int) {
	// 重置选举时间
	// 防止收到投票请求后有人当选 leader 但此时定时器已经触发
	// 自己开始投票，出现需要多发一轮投票的情况
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	DPrintf("Term: %v, server %v become leader\n", rf.currentTerm, rf.me)
	rf.state = Leader
	rf.matchedIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	var term = rf.currentTerm
	for i := range rf.peers {
		rf.matchedIndex[i] = 0
		rf.nextIndex[i] = rf.sliceIndex2logIndex(len(rf.log))
	}
	// 广播心跳
	go rf.boradcastHeartbeat()
	// 定期广播心跳
	go rf.periodicallyHeartbeat()
	// 监听 commitIndex 更新 leader 的 commitIndex
	go rf.monitorMatchedIndex(term)
	// 广播快照
	// DPrintf("Term: %v, server %v monitorMatchedIndex\n", rf.currentTerm, rf.me)
	for i := range rf.peers {
		ii := i
		if i != rf.me {
			// 监听 nextIndex 确定发送给 follower 的日志条目
			go rf.monitorNextIndex(ii, term)
			go rf.monitorSnapshot(ii, term)
		}
	}
}

// 获取最后一个日志条目的任期和下标
func (rf *Raft) getLastLogEntryTermIndex() (term int, index int) {
	lastLogEntry := rf.log[len(rf.log)-1]
	term = lastLogEntry.Term
	index = lastLogEntry.Index
	return term, index
}

// 向 server 发起选举
func (rf *Raft) election(server, term, lastEntryTerm, lastEntryIndex int, voteCount *int, voteMutex *sync.Cond) {
	// 可以改用 sync.Cond
	// voteMutex.Broadcast()
	for !rf.killed() {
		// send
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogTerm:  lastEntryTerm,
			LastLogIndex: lastEntryIndex,
		}
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(server, &args, &reply) // 直到发送成功
		if !ok {
			time.Sleep(SleepTime)
			continue
		}
		// receive
		// 发送投票的请求成功
		rf.mu.Lock()
		if rf.currentTerm != term { // 不是当前任期的投票，废弃 (延迟太高了，收到时已经过了任期)
			rf.mu.Unlock()
			voteMutex.Signal()
			// voteChan <- false // 为了优雅地关闭监听投票的协程
			break
		}

		if reply.Term > rf.currentTerm { // 对方 term 比自身 term 大，退化为 follower
			// DPrintf("Election: 接收到来自 server %d 的回复，term %d 比 leader %d 的 term %d 大，由 %s 退化为 Follower\n", ii, reply.Term, rf.me, rf.currentTerm, rf.state)
			rf.becomeFollower(reply.Term, -1)
			rf.mu.Unlock()
			voteMutex.Signal()
			// voteChan <- false
			break
		}

		// reply.term <= rf.currentTerm
		// 只接收当前任期的投票
		// 仅当 reply 回复了 true
		if reply.VoteGranted { // 获得该投票
			// DPrintf("Election: Term %d, server %d get the vote from server %d\n", rf.currentTerm, rf.me, ii)
			rf.mu.Unlock()
			voteMutex.L.Lock()
			*voteCount += 1
			voteMutex.L.Unlock()
			voteMutex.Signal()
			break
			// voteChan <- true
		} else {
			// DPrintf("Term: %v, server %v is not candidate\n", rf.currentTerm, rf.me)
			rf.mu.Unlock()
			voteMutex.Signal()
			break
			// voteChan <- false
		}
	}
}

func (rf *Raft) broadcastElection() {
	rf.mu.RLock()
	if rf.killed() || rf.state == Leader {
		rf.mu.RUnlock()
		return
	}
	rf.becomeCandidate(rf.currentTerm + 1)
	lastEntryTerm, lastEntryIndex := rf.getLastLogEntryTermIndex()
	term := rf.currentTerm
	rf.persist()
	rf.mu.RUnlock()
	// count 表示该 term 期间内获得的投票数
	var voteMutex = sync.NewCond(&sync.Mutex{})
	var voteCount = 0
	for i := range rf.peers { // 广播选举
		if i != rf.me {
			ii := i
			go rf.election(ii, term, lastEntryTerm, lastEntryIndex, &voteCount, voteMutex)
		}
	}
	for !rf.killed() { // 监听投票
		voteMutex.L.Lock()
		if voteCount < len(rf.peers)/2 {
			voteMutex.Wait()
		}

		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != Candidate {
			rf.mu.Unlock()
			voteMutex.L.Unlock()
			break
		}

		if voteCount >= len(rf.peers)/2 {
			rf.becomeLeader()
			rf.mu.Unlock()
			voteMutex.L.Unlock()
			break
		}
		rf.mu.Unlock()
		voteMutex.L.Unlock()
	}
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
	index := rf.sliceIndex2logIndex(len(rf.log))
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (3B).
	if isLeader {
		DPrintf("server %d, index: %d, command: %v", rf.me, index, command)
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
		rf.persist()
	}
	return index, term, isLeader
}

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
			go rf.broadcastElection()
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
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 初始填充一个空 Log 以保证索引从 1 开始
	// 符合 raft 论文描述要求
	rf.log = []LogEntry{
		{0, 0, 0},
	}
	// initialize from state persisted before a crash
	rf.readRaftstate(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()
	return rf
}
