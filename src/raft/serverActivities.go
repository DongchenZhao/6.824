package raft

import (
	"fmt"
	"sort"
	"strconv"
)

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
			rf.log = append(rf.log[:prevLogIndex+1], entries...)
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

	if majorityIndex != -1 { // 如果大多数server的日志都为空，那么就不需要更新commitIndex

		// 更新commitIndex的条件
		// leader 仅当majorityIndex更新，且为当前term的时候才会提交
		cond1 := majorityIndex > commitIndex            // 条件1: 大多数server的日志都和leader同步了
		cond2 := log[majorityIndex].Term == currentTerm // 条件2: 大多数server的日志都是当前term的
		if cond1 && cond2 {
			rf.commitIndexLock.Lock()
			rf.lastAppliedLock.Lock()
			prevCommitIndex := rf.commitIndex
			rf.commitIndex = majorityIndex
			rf.lastApplied = rf.commitIndex
			rf.lastAppliedLock.Unlock()
			rf.commitIndexLock.Unlock()
			PrintLog("Leader update commitIndex to ["+strconv.Itoa(majorityIndex)+"]", "red", strconv.Itoa(rf.me))
			rf.PrintState()
			rf.sendNewlyCommittedLog(prevCommitIndex)
		}
	}
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
		PrintLog(fmt.Sprint("send newly committed log to kv server, log info: "+rf.LogEntryToStr(i)), "yellow", strconv.Itoa(rf.me))

		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + 1}
	}
	rf.logLock.RUnlock()
}
