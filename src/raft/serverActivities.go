package raft

import (
	"sort"
	"strconv"
	"time"
)

// follower或candidate 超时未收到心跳触发
// 0. role设置为1
// 1. 增加currentTerm
// 2. voteFor设置为自己
// 3. 重置lastHeartbeatTime
// 4. 向其他server发送RequestVote RPC
func (rf *Raft) toCandidate() {
	rf.roleLock.Lock()
	rf.role = 1
	defer rf.roleLock.Unlock()

	// 更新term
	rf.currentTermLock.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	defer rf.currentTermLock.Unlock()

	PrintLog("-> candidate start election, candidate term ["+strconv.Itoa(currentTerm)+"]", "green", strconv.Itoa(rf.me))

	// 为自己投票
	rf.voteForLock.Lock()
	rf.votedFor = rf.me
	defer rf.voteForLock.Unlock()

	// 重置lastHeartbeatTime
	rf.lastHeartbeatTimeLock.Lock()
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	defer rf.lastHeartbeatTimeLock.Unlock()

	// 获取当前term，me，lastLogIndex，lastLogTerm，这些都是需要发送给其他server的，对这些数据快照，以防不同server收到不同数据
	me := rf.me
	rf.logLock.RLock()
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	} else {
		lastLogIndex = -1
		lastLogTerm = currentTerm
	}
	rf.logLock.RUnlock()

	// 遍历rf的peer，并发地向每个peer发送RequestVote RPC

	for i := 0; i < len(rf.peers); i++ {
		curI := i
		go func() {
			if curI == rf.me { // 跳过自己
				return
			}
			PrintLog("RequestVote send to [Server "+strconv.Itoa(curI)+"]"+" term ["+strconv.Itoa(currentTerm)+"]"+" lastLogIndex ["+strconv.Itoa(lastLogIndex)+"]"+" lastLogTerm ["+strconv.Itoa(lastLogTerm)+"]", "default", strconv.Itoa(rf.me))
			requestVoteArgs := RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(curI, &requestVoteArgs, &requestVoteReply)
			if !ok {
				PrintLog("RequestVote send to [Server "+strconv.Itoa(curI)+"] failed", "yellow", strconv.Itoa(rf.me))
			}
			rf.requestVoteRespHandler(&requestVoteReply)
		}()
	}
}

// leader/candidate/follower发现自己term小于RPC中的term
// 转为follower，重置lastHeartbeatTime
// 如果term更新，重置voteFor，currentTerm，voteCnt
func (rf *Raft) toFollower(term int) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()

	// 重置role和lastHeartbeatTime
	rf.roleLock.Lock()
	defer rf.roleLock.Unlock()
	rf.lastHeartbeatTimeLock.Lock()
	defer rf.lastHeartbeatTimeLock.Unlock()

	rf.role = 0
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	if currentTerm < term { // 对方term更新，更新自己的term，重置voteFor，voteCnt
		PrintLog("-> follower, time expired, update term to ["+strconv.Itoa(term)+"], previous term ["+strconv.Itoa(currentTerm)+"]", "green", strconv.Itoa(rf.me))

		rf.currentTermLock.Lock()
		defer rf.currentTermLock.Unlock()
		rf.voteForLock.Lock()
		defer rf.voteForLock.Unlock()
		rf.voteCntLock.Lock()
		defer rf.voteCntLock.Unlock()

		rf.currentTerm = term
		rf.votedFor = -1
		rf.voteCnt = 0
	}
}

func (rf *Raft) toLeader() {

	rf.currentTermLock.RLock()
	PrintLog("-> leader for term ["+strconv.Itoa(rf.currentTerm)+"]", "green", strconv.Itoa(rf.me))
	rf.currentTermLock.RUnlock()
	// 重置一些状态
	rf.roleLock.Lock()
	rf.lastHeartbeatTimeLock.Lock()
	rf.voteForLock.Lock()
	rf.voteCntLock.Lock()
	rf.nextIndexLock.Lock()
	rf.matchIndexLock.Lock()
	rf.logLock.RLock()

	rf.role = 2
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.curLeader = rf.me
	// 清空nextIndex和matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// 初始化nextIndex[]和matchIndex[]
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1 // 表示当前leader认为其他server的log为空，(选择初始化为https://thesquareplanet.com/blog/students-guide-to-raft/中的-1而不是论文中的0,貌似因为论文中log index从1开始)
	}

	rf.roleLock.Unlock()
	rf.lastHeartbeatTimeLock.Unlock()
	rf.voteForLock.Unlock()
	rf.voteCntLock.Unlock()
	rf.nextIndexLock.Unlock()
	rf.matchIndexLock.Unlock()
	rf.logLock.RUnlock()

	// 启动go routine，定期发送心跳包
	go rf.leaderTick()

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// follower或candidate
// 定期检查是否heartbeat超时，如果当前server认为自己是leader，那么就不需要进行选举
// 否则在electionTimeout时间内没有收到心跳包，就开始选举
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(10 * time.Millisecond)

		// 如果当前角色是leader，那么就不需要听心跳
		rf.roleLock.RLock()
		role := rf.role
		rf.roleLock.RUnlock()
		if role == 2 {
			continue
		}
		// 如果距离上次收到心跳包的时间超过了electionTimeout，那么就需要进行选举
		rf.lastHeartbeatTimeLock.RLock()
		lastHeartbeatTime := rf.lastHeartbeatTime
		rf.lastHeartbeatTimeLock.RUnlock()
		if lastHeartbeatTime < time.Now().UnixMilli()-int64(rf.electionTimeout) {
			rf.toCandidate()
		}
	}
}

func (rf *Raft) leaderTick() {
	for rf.killed() == false {
		rf.roleLock.RLock()
		role := rf.role
		rf.roleLock.RUnlock()

		if role != 2 { // 不再是leader，结束这个go routine
			PrintLog("No longer leader", "green", strconv.Itoa(rf.me))
			return
		}

		// 发送心跳包
		rf.leaderSendAppendEntriesRPC(true)

		// 检查同步情况，更新commitIndex，发送AE RPC
		rf.leaderUpdateCommitIndex()

		time.Sleep(150 * time.Millisecond)
	}
}

// 匹配leader AE RPC中的prevLogIndex 和 prevLogTerm同当前rf log的情况
// 1.rf的log在prevLogIndex位置没有条目，返回(false, -1, len(rf.log), len(rf.log))
// 2.rf的log在prevLogIndex位置term冲突，返回(false, rf.log[prevLogIndex].Term, XIndex, XLen) leader需要在自己日志中对rf这个位置上的term进行搜索，根据leader是否有这个rf的这个term串来执行不同的操作
// 此外，在2的情况下，rf需要截断日志
// 3.匹配成功，append entries
func (rf *Raft) compareAndHandleLog(prevLogTerm int, prevLogIndex int, entries []LogEntry) (bool, int, int, int) {
	XTerm := -1
	XIndex := -1
	XLen := 0

	// 对rf.log进行拷贝，但新切片元素中，只包含Term, 因为在2情况下进行了日志截断，所以这里需要深拷贝
	rf.logLock.RLock()
	log := make([]LogEntry, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		log[i].Term = rf.log[i].Term
	}
	rf.logLock.RUnlock()

	// 1. 当前server log长度不够，XTerm = -1, XIndex=Len(rf.log) leader下次会尝试匹配rf的最后一条日志
	if len(log)-1 < prevLogIndex {
		XTerm, XIndex, XLen = -1, len(log), len(log)
		PrintLog("log compare failed, empty entry at prevLogIndex", "blue", strconv.Itoa(rf.me))
		rf.PrintRaftLog()
		return false, XTerm, XIndex, XLen
	}

	// 2. 匹配成功
	// append entries
	// rf日志可能过长,但是过长部分会直接被entries覆盖
	// fixed:待匹配的index为-1，相当于默认匹配成功，这涵盖了当前rf日志长度为0和不为0的情况
	// fixed:匹配成功放在最前面，因为处理了prevLogIndex为-1的情况
	if prevLogIndex == -1 || log[prevLogIndex].Term == prevLogTerm {
		// 如果是心跳包,或者空entries(表示已经和leader同步了),不修改本地日志（也不截断）
		if len(entries) > 0 { // 空切片len为0
			rf.logLock.Lock()
			rf.log = append(log[:prevLogIndex+1], entries...)
			rf.logLock.Unlock()
			PrintLog("log compare success, append entries", "blue", strconv.Itoa(rf.me))
			rf.PrintRaftLog()
		}
		return true, XTerm, XIndex, XLen
	}

	// 3. 匹配失败，leader需要将nextIndex移动越过当前server冲突term的所有entry
	// If a follower does have prevLogIndex in its log, but the term does not match,
	// it should return conflictTerm = log[prevLogIndex].Term,
	// and then search its log for the first index whose entry has term equal to conflictTerm.
	// 此外，在这种日志不匹配的情况下，rf需要截断日志
	if log[prevLogIndex].Term != prevLogTerm {
		XTerm, XIndex, XLen = rf.getXLogInfo(prevLogIndex)
		PrintLog("log compare failed, mismatch at prevLogIndex", "blue", strconv.Itoa(rf.me))
		rf.PrintRaftLog()
		// 截断日志
		rf.logLock.Lock()
		rf.log = rf.log[:prevLogIndex]
		rf.logLock.Unlock()
		return false, XTerm, XIndex, XLen
	}

	PrintLog("log compare error, unknown error", "red", strconv.Itoa(rf.me))
	return false, XTerm, XIndex, XLen
}

// 获取当前server log指定index的最后一组term的第一条log的index和len
func (rf *Raft) getXLogInfo(index int) (int, int, int) {
	XTerm := -1
	XIndex := 0
	XLen := 0

	rf.logLock.RLock()
	defer rf.logLock.RUnlock()

	// 日志长度为0
	if len(rf.log) == 0 {
		PrintLog("getXLogInfo error, log is empty", "red", strconv.Itoa(rf.me))
		return XTerm, XIndex, XLen
	}

	// index为-1的时候，返回当前server最后一串term的第一个entry信息
	i := index
	if index == -1 {
		i = len(rf.log) - 1
	}
	XTerm = rf.log[i].Term
	for i != 0 {
		if rf.log[i].Term != XTerm {
			XIndex = i + 1
			XLen = len(rf.log) - XIndex
			return XTerm, XIndex, XLen
		}
		i--
	}
	// 日志只有一个term串
	return XTerm, i, len(rf.log)
}

// 定期检查日志同步情况，给需要同步的server发送AE RPC
func (rf *Raft) leaderUpdateCommitIndex() {

	rf.matchIndexLock.RLock()
	matchIndex := make([]int, len(rf.matchIndex))
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)
	rf.matchIndexLock.RUnlock()

	rf.logLock.RLock()
	log := make([]LogEntry, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		log[i].Term = rf.log[i].Term
	}
	rf.logLock.RUnlock()

	// fixed: leader log为空时，不需要更新commitIndex
	if len(log) == 0 {
		return
	}

	rf.commitIndexLock.RLock()
	commitIndex := rf.commitIndex
	rf.commitIndexLock.RUnlock()

	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()

	// 滑动窗口查找matchIndex中大多数
	majorityIndex := -1
	i, windowLen := 0, 0
	for {
		if i+windowLen > len(sortedMatchIndex)-1 {
			break
		}
		if windowLen+1 >= (len(rf.peers)+1)/2 {
			majorityIndex = sortedMatchIndex[i]
		}
		if sortedMatchIndex[i] != sortedMatchIndex[i+windowLen] {
			i = i + windowLen
			windowLen = 0
		}
		windowLen += 1
	}

	matchIndexStr := "["
	for i := 0; i < len(matchIndex); i++ {
		matchIndexStr += strconv.Itoa(matchIndex[i]) + " "
	}
	matchIndexStr += "]"
	// PrintLog("matchIndex: "+matchIndexStr, "purple", strconv.Itoa(rf.me))

	// leader 仅当majorityIndex更新，且为当前term的时候才会提交

	// PrintLog("majorityIndex: "+strconv.Itoa(majorityIndex)+" commitIndex: "+strconv.Itoa(commitIndex)+" currentTerm: "+strconv.Itoa(currentTerm), "purple", strconv.Itoa(rf.me))

	// leader 发送AE RPC
	// TODO 单独goroutine
	// rf.leaderSendAppendEntriesRPC(false)

	if majorityIndex != -1 { // 如果大多数server的日志都为空，那么就不需要更新commitIndex
		cond1 := majorityIndex > commitIndex
		cond2 := log[majorityIndex].Term == currentTerm
		if cond1 && cond2 {
			rf.commitIndexLock.Lock()
			rf.lastAppliedLock.Lock()
			prevCommitIndex := rf.commitIndex
			rf.commitIndex = majorityIndex
			rf.lastApplied = rf.commitIndex
			rf.lastAppliedLock.Unlock()
			rf.commitIndexLock.Unlock()
			rf.sendNewlyCommittedLog(prevCommitIndex)
		}
	}

	rf.leaderSendAppendEntriesRPC(false)
}

func (rf *Raft) sendNewlyCommittedLog(prevCommitIndex int) {

	rf.commitIndexLock.RLock()
	commitIndex := rf.commitIndex
	rf.commitIndexLock.RUnlock()

	rf.logLock.RLock()
	for i := prevCommitIndex + 1; i <= commitIndex; i++ {
		// 发送到kv server
		// TODO 单独goroutine
		// You'll want to have a separate long-running goroutine that sends
		// committed log entries in order on the applyCh. It must be separate,
		// since sending on the applyCh can block; and it must be a single
		// goroutine, since otherwise it may be hard to ensure that you send log
		// entries in log order. The code that advances commitIndex will need to
		// kick the apply goroutine; it's probably easiest to use a condition
		// variable (Go's sync.Cond) for this.
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1}
	}
	rf.logLock.RUnlock()
}
