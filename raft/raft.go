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
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	var term, voteFor int
	var log = make([]LogEntry, len(rf.log))
	var snapshotState []byte
	if rf.snapshot != nil {
		snapshotState = encodeSnapshot(
			rf.snapshot.LastIncludedIndex,
			rf.snapshot.Commands,
			rf.snapshot.LastIncludedTerm,
		)
	}
	term = rf.currentTerm
	voteFor = rf.votedFor
	copy(log, rf.log)
	raftstate := encodeRaftstate(term, voteFor, log)
	rf.persister.Save(raftstate, snapshotState)
}

// restore previously persisted state.
func (rf *Raft) readRaftstate(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	term, votedFor, log := decodeRaftstate(data)
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = log
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	lastIncludedIndex, commands, lastIncludedTerm := decodeSnapshot(data)
	rf.snapshot = &SnapShot{
		LastIncludedIndex: lastIncludedIndex,
		Commands:          commands,
		LastIncludedTerm:  lastIncludedTerm,
	}
	rf.X = rf.snapshot.LastIncludedIndex
	rf.commitIndex = rf.snapshot.LastIncludedIndex
	rf.lastApplied = rf.snapshot.LastIncludedIndex
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			// DPrintf("server %d, lastapplied: %d, commitIndex: %d", rf.me, rf.lastApplied, rf.commitIndex)
			rf.lastApplied += 1
			sliceIndex := rf.logIndex2sliceIndex(rf.lastApplied)
			log := rf.log[sliceIndex]
			// DPrintf("server %d commit/apply index [%d], log %s", rf.me, rf.lastApplied, &log)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}
			rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 外部调用
	// 正常应该是有一个协程监听当前日志长度和快照长度，当比例达到 4 : 1 时进行快照
	// Your code here (3D).
	// 解码得到新的 snapshot 内容
	var (
		lastIncludedIndex int           // 快照对应的日志 index
		lastIncludedTerm  int           // 快照对应的日志 term
		mergedCommands    []interface{} // 合并后的 commands
	)
	lastIncludedIndex, commands := decodeAppliedSnapshot(snapshot)
	if index != lastIncludedIndex {
		log.Fatal("Snapshot: index != lastIncludedIndex")
	}
	if index <= rf.log[0].Index { // installSnapshot 加速了整个过程
		return
	}
	// 将其和之前的快照进行合并
	// if rf.snapshot != nil {
	// 	mergedCommands = append(mergedCommands, rf.snapshot.Commands...)
	// }
	// mergedCommands = append(mergedCommands, commands...)
	// if rf.snapshot != nil {
	// 	mergedCommands = append(mergedCommands, rf.snapshot.Commands...)
	// 	mergedCommands = append(mergedCommands, commands[rf.X+1:]...)
	// 	// DPrintf("%d", len(rf.snapshot.Commands))
	// 	// DPrintf("%d", len(mergedCommands))
	// } else {
	// 	mergedCommands = commands
	// }
	mergedCommands = commands

	lastIncludedTerm = rf.log[rf.logIndex2sliceIndex(index)].Term

	rf.snapshot = &SnapShot{
		LastIncludedIndex: lastIncludedIndex,
		Commands:          mergedCommands,
		LastIncludedTerm:  lastIncludedTerm,
	}
	sliceIndex := rf.logIndex2sliceIndex(lastIncludedIndex)

	// 清除 index 之前的日志
	newLogs := []LogEntry{
		{ // no-op
			Term:  lastIncludedTerm,
			Index: lastIncludedIndex,
		},
	}
	rf.log = append(newLogs, rf.log[sliceIndex+1:]...)
	DPrintf("Insert snapshot server %d: %d %v", rf.me, lastIncludedIndex, mergedCommands)
	DPrintf("NewLogs: logs: %v, Snapshot commands: %v", rf.log, rf.snapshot.Commands)
	rf.X = index
	rf.persist()
	// go rf.broadcastInstallSnapshot()
	// rf.persistCh <- struct{}{}
}

func (rf *Raft) installSnapshot(server, term, lastIncludedIndex, lastIncludedTerm int, commands []byte) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		args := InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Data:              commands,
		}
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, &args, &reply)
		if !ok {
			time.Sleep(SLEEPTIME)
			continue
		}
		rf.mu.Lock()
		if lastIncludedIndex > rf.matchedIndex[server] {
			rf.matchedIndex[server] = lastIncludedIndex
			rf.nextIndex[server] = lastIncludedIndex + 1
		}
		if reply.Term < rf.currentTerm {
			rf.becomeFollower(reply.Term, -1)
		}
		rf.mu.Unlock()
		break
	}
}

func (rf *Raft) broadcastInstallSnapshot() {
	rf.mu.Lock()
	if rf.snapshot == nil {
		rf.mu.Unlock()
		return
	}
	copyCommands := make([]interface{}, len(rf.snapshot.Commands))
	term := rf.currentTerm
	// snapshot 的 index 以及 term 可以直接从空日志中取
	// 不必从 snapshot 中
	// 该实验默认都在内存中
	// 实际上应在磁盘中
	lastIncludedIndex := rf.snapshot.LastIncludedIndex
	lastIncludedTerm := rf.snapshot.LastIncludedTerm
	copy(copyCommands, rf.snapshot.Commands)
	rf.mu.Unlock()
	var w bytes.Buffer
	e := labgob.NewEncoder(&w)
	e.Encode(copyCommands)
	commands := w.Bytes()
	for i := range rf.peers {
		if i != rf.me {
			ii := i
			go rf.installSnapshot(ii, term, lastIncludedIndex, lastIncludedTerm, commands)
		}
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
		// rf.mu.Unlock()
		return
	}
	rf.heartbeatC <- struct{}{}

	if args.LastIncludedIndex <= rf.log[0].Index { // 消息传递太慢，后面的快照长的消息先到
		// rf.mu.Unlock()
		return
	}

	var commands []interface{}
	r := bytes.NewBuffer(args.Data)
	d := labgob.NewDecoder(r)
	d.Decode(&commands)
	// DPrintf("to server %d, %+v", rf.me, args)
	// DPrintf("lastApplied: %d log length: %d", rf.lastApplied, rf.sliceIndex2logIndex(len(rf.log)))
	// DPrintf("commands length: %d, commands: %v", len(commands), commands)
	DPrintf("to server %d, lastApplied: %d, lastIncludedIndex: %d", rf.me, rf.lastApplied, args.LastIncludedIndex)
	// var start, end int = rf.lastApplied + 1, len(commands)
	newLogs := []LogEntry{
		{ // no-op
			Term:  args.LastIncludedTerm,
			Index: args.LastIncludedIndex,
		},
	}
	// DPrintf("server %d: before last applied %d", rf.me, rf.lastApplied)
	if args.LastIncludedIndex >= rf.sliceIndex2logIndex(len(rf.log))-1 { // follower 日志是快照的前缀
		// 删除 follower 中的所有日志
		// 执行快照中的命令
		rf.log = nil
		rf.log = newLogs
	} else { // follower 中部分日志是快照的前缀
		// 保留后缀
		index := rf.logIndex2sliceIndex(args.LastIncludedIndex)
		rf.log = append(newLogs, rf.log[index+1:]...)
	}
	rf.X = args.LastIncludedIndex
	rf.snapshot = &SnapShot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Commands:          commands,
	}
	rf.persist()
	// 防止 lastApplied == 快照 lastIncludedIndex && (lastApplied + 1) % SnapshotSize == 0
	// 造成循环等待
	// applier
	if rf.lastApplied >= args.LastIncludedIndex {
		return
	}
	rf.lastApplied = args.LastIncludedIndex
	// DPrintf("server %d: last applied %d, logs: %v", rf.me, rf.lastApplied, rf.log)
	// 可能会出现死锁
	// applyCommand 后可能会调用 Snapshot
	// 在 config.applierSnap 中调用 snap 函数，而 snapshot 会请求锁
	// applyCh 需要在 Snapshot 完成后才会接收值
	// 因此造成循环等待
	// for i, command := range commands[start:end] {
	// 	DPrintf("server %d apply command [%d] command %v", rf.me, i+start, command)
	// 	rf.applyCh <- ApplyMsg{
	// 		CommandValid: true,
	// 		Command:      command,
	// 		CommandIndex: start + i,
	// 	}
	// }
	/*
		* 需要加锁
		applyCh 不加锁会出现如下情况:
		1. lastApplied 值已经发生改变
		2. applyLog apply 了 lastApplied 下一个值 (但是测试配置config中的lastApplied还是之前的值)
		3. 此时 ApplyMsg 会出现竞争 [1. applyLog 提交日志 2. 提交快照]
		4. 如果 [1] 竞争成功，此时 applyLog 提交的日志 index 和 lastApplied + 1 不一致，导致错误
	*/
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      encodeSnapshot(args.LastIncludedIndex, commands, args.LastIncludedTerm),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
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
		rf.persist()
		// rf.persistCh <- struct{}{}
	} else {
		reply.VoteGranted = false
	}
	// fmt.Println(rf.me, args)
	// fmt.Println(rf.me, reply)
}

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

	reply.XLen = rf.sliceIndex2logIndex(len(rf.log))
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
			if !isHeartbeat {
				DPrintf("server %d, X: %d, PrevLogIndex: %d, logLength: %d, leaderCommit: %d, commit: %d", rf.me, rf.X, args.PrevLogIndex, rf.sliceIndex2logIndex(len(rf.log)), args.LeaderCommit, rf.commitIndex)
			}
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
		// 一致
		entries = args.Entries
		start = prevLogSliceIndex + 1
	} else { // prevLog 在 snapshot 中
		start = 1
		if rf.X-args.PrevLogIndex < len(args.Entries) { // 截断
			entries = args.Entries[rf.X-args.PrevLogIndex:]
		} else {
			entries = nil
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
		rf.persist()
		// rf.persistCh <- struct{}{}
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
func (rf *Raft) monitorMatchedIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		var commitIndex int
		var middleIndex = len(rf.peers)/2 - 1
		var matchedIndex = make([]int, len(rf.peers))
		commitIndex = rf.commitIndex
		copy(matchedIndex, rf.matchedIndex)
		rf.mu.Unlock()
		sort.Sort(MatchedIndex(matchedIndex))
		if commitIndex < matchedIndex[middleIndex] {
			rf.mu.Lock()
			if rf.state == Leader {
				DPrintf("server %d, %v, commitIndex: %d", rf.me, rf.matchedIndex, rf.commitIndex)
				rf.commitIndex = matchedIndex[middleIndex]
			}
			rf.mu.Unlock()
		}
		time.Sleep(MONITORTIME)
	}
}

// 监听 follower 的 nextIndex
// 如果 nextIndex 值小于等于 leader 日志长度则进行复制
// 定时监听
func (rf *Raft) monitorNextIndex(server int) {
	set := make(map[Pair]struct{})
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		// 需要加上快照中的长度
		if rf.nextIndex[server] < rf.sliceIndex2logIndex(len(rf.log)) {
			term := rf.currentTerm
			leaderCommit := rf.commitIndex
			start := rf.nextIndex[server]
			// 需要利用快照下标进行映射
			end := rf.sliceIndex2logIndex(len(rf.log)) - 1
			key := Pair{start: start, end: end}
			if _, ok := set[key]; !ok {
				go rf.appendEntries(server, term, leaderCommit, start, end)
				set[key] = struct{}{}
			}
		}
		rf.mu.Unlock()
		time.Sleep(MONITORTIME)
	}
}

func (rf *Raft) appendEntries(server, term, leaderCommit, start, end int) {
	for !rf.killed() {
		// DPrintf("server %d send the logs from %d to %d to server %d", rf.me, start, end, server)
		rf.mu.Lock()
		// 不是 leader
		// 调用 InstallSnapshot 进行复制
		// 判定左边界和右边界
		// nextIndex[i] 比 end 大 (说明已经有更长的日志成功添加到 follower 中)
		if rf.state != Leader || start <= rf.X || rf.nextIndex[server] > end {
			rf.mu.Unlock()
			break
		}
		sliceStart := rf.logIndex2sliceIndex(start)
		sliceEnd := rf.logIndex2sliceIndex(end)
		// DPrintf("server %d send entries to server %d, nextIndex: %v commitIndex: %v lastLogIndex: %d, entries: %v", rf.me, i, rf.nextIndex[i], leaderCommit, end, rf.log[start:end+1])
		prevLog := rf.log[sliceStart-1]
		// 待发送的日志条目
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
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			time.Sleep(SLEEPTIME)
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
}

// 对 server 发送心跳
func (rf *Raft) heartbeat(server, term, commitIndex int) {
	for !rf.killed() {
		var prevLog LogEntry
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		sliceIndex := rf.logIndex2sliceIndex(rf.nextIndex[server] - 1)
		// 快照中已经包含了该下标
		if sliceIndex < 0 {
			prevLog = LogEntry{
				Term:  rf.snapshot.LastIncludedTerm,
				Index: rf.snapshot.LastIncludedIndex,
			}
		} else {
			prevLog = rf.log[sliceIndex]
		}
		rf.mu.Unlock()
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
}

// 广播心跳
func (rf *Raft) boradcastHeartbeat() {
	rf.mu.Lock()
	// DPrintf("Term %d: server %d broadcast the heartbeat", rf.currentTerm, rf.me)
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
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
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		<-time.After(HEARTBEATTIME)
		rf.boradcastHeartbeat()
		rf.broadcastInstallSnapshot()
	}
}

func (rf *Raft) becomeCandidate(term int) {
	rf.state = Candidate
	rf.currentTerm = term
	rf.votedFor = rf.me
	rf.persist()
	// rf.persistCh <- struct{}{}
}

func (rf *Raft) becomeFollower(term int, votedFor int) {
	// 重置选举时间
	// 防止收到投票请求后有人当选 leader 但此时定时器已经触发
	// 自己开始投票，出现需要多发一轮投票的情况
	rf.heartbeatC <- struct{}{}
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.persist()
	// rf.persistCh <- struct{}{}
}

func (rf *Raft) becomeLeader() {
	DPrintf("Term: %v, server %v become leader\n", rf.currentTerm, rf.me)
	rf.state = Leader
	rf.matchedIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.matchedIndex[i] = 0
		rf.nextIndex[i] = rf.sliceIndex2logIndex(len(rf.log))
	}
	// 广播心跳
	go rf.boradcastHeartbeat()
	// 定期广播心跳
	go rf.periodicallyHeartbeat()
	// 监听 commitIndex 更新 leader 的 commitIndex
	go rf.monitorMatchedIndex()
	// 广播快照
	// go rf.broadcastInstallSnapshot()
	// DPrintf("Term: %v, server %v monitorMatchedIndex\n", rf.currentTerm, rf.me)
	for i := range rf.peers {
		ii := i
		if i != rf.me {
			// 监听 nextIndex 确定发送给 follower 的日志条目
			go rf.monitorNextIndex(ii)
			// DPrintf("Term: %v, server %v monitorNextIndex server %d\n", rf.currentTerm, rf.me, ii)
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
	for {
		if rf.killed() {
			// 直接 return 会存在内存泄漏
			// 收集投票的协程无法关闭
			// voteChan <- false
			voteMutex.Signal()
			break
		}
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogTerm:  lastEntryTerm,
			LastLogIndex: lastEntryIndex,
		}
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(server, &args, &reply) // 直到发送成功
		if !ok {
			time.Sleep(SLEEPTIME)
			continue
		}
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

// 收集选票
// term: 选票任期值
// func (rf *Raft) collectVotes(term int, voteMutex *sync.Cond) {
// count := 0
// for i := 0; i < len(rf.peers)-1; i++ {
// 	if val := <-voteChan; val { // 投票成功
// 		count++
// 		if count >= len(rf.peers)/2 && !rf.killed() {
// 			rf.mu.Lock()
// 			if rf.currentTerm == term && rf.state == Candidate {
// 				rf.becomeLeader()
// 			}
// 			rf.mu.Unlock()
// 		}
// 	}
// }

func (rf *Raft) broadcastElection() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate(rf.currentTerm + 1)
	lastEntryTerm, lastEntryIndex := rf.getLastLogEntryTermIndex()
	term := rf.currentTerm
	// DPrintf("Term %d: server %d start election\n", rf.currentTerm, rf.me)
	rf.mu.Unlock()
	// count 表示该 term 期间内获得的投票数
	// voteChan := make(chan bool) // 监听投票结果
	var voteMutex = sync.NewCond(&sync.Mutex{})
	var voteCount = 0
	for i := range rf.peers { // 广播选举
		if i != rf.me {
			ii := i
			// go rf.election(ii, term, lastEntryTerm, lastEntryIndex, voteChan)
			go rf.election(ii, term, lastEntryTerm, lastEntryIndex, &voteCount, voteMutex)
		}
	}
	// go rf.collectVotes(term, voteChan)
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
		DPrintf("index: %d, command: %v", index, command)
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
		rf.persist()
		// rf.persistCh <- struct{}{}
		// go rf.broadcastLogs()
		// DPrintf("Term %d, server %d get the command from client. log: %v\n", rf.currentTerm, rf.me, rf.log)
	}
	return index, term, isLeader
}

// func (rf *Raft) broadcastLogs() {
// 	rf.mu.Lock()
// 	term := rf.currentTerm
// 	leaderCommit := rf.commitIndex
// 	end := rf.sliceIndex2logIndex(len(rf.log) - 1)
// 	rf.mu.Unlock()
// 	rf.appendEntries(term, leaderCommit, end)
// }

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
	// rf.persistCh = make(chan struct{}, 10)
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
	rf.readRaftstate(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()
	// go rf.persist()
	// go rf.applySnapshot()
	return rf
}
