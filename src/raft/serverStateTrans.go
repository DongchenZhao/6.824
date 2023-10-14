package raft

import (
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
	rf.roleLock.Unlock()

	// 更新term
	rf.currentTermLock.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.currentTermLock.Unlock()

	PrintLog("-> candidate timeouts,  start election, candidate term ["+strconv.Itoa(currentTerm)+"]", "green", strconv.Itoa(rf.me))
	rf.PrintState()

	// 为自己投票
	rf.voteForLock.Lock()
	rf.voteCntLock.Lock()
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.voteCntLock.Unlock()
	rf.voteForLock.Unlock()

	// 重置lastHeartbeatTime
	rf.lastHeartbeatTimeLock.Lock()
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	rf.lastHeartbeatTimeLock.Unlock()

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
				// PrintLog("RequestVote send to [Server "+strconv.Itoa(curI)+"] failed", "yellow", strconv.Itoa(rf.me))
			}
			rf.requestVoteRespHandler(&requestVoteReply)
		}()
	}
}

// leader/candidate/follower发现自己term小于RPC中的term
// 转为follower，重置lastHeartbeatTime
// 如果term更新，重置voteFor，currentTerm，voteCnt
func (rf *Raft) toFollower(term int, resetTimeout bool) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()

	// 重置role和lastHeartbeatTime
	// TODO
	rf.roleLock.Lock()
	defer rf.roleLock.Unlock()
	rf.role = 0
	if resetTimeout {
		rf.lastHeartbeatTimeLock.Lock()
		rf.lastHeartbeatTime = time.Now().UnixMilli()
		rf.lastHeartbeatTimeLock.Unlock()
	}

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
	rf.lastHeartbeatTime = time.Now().UnixMilli() // 也可以不重置
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
	// leader更新自己的nextIndex和matchIndex
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

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
			rf.leaderUpdateCommitIndex()
			continue
		}
		// 如果距离上次收到心跳包的时间超过了electionTimeout，那么就需要进行选举
		rf.lastHeartbeatTimeLock.RLock()
		lastHeartbeatTime := rf.lastHeartbeatTime
		rf.lastHeartbeatTimeLock.RUnlock()

		rf.electionTimeoutLock.RLock()
		electionTimeout := rf.electionTimeout
		rf.electionTimeoutLock.RUnlock()

		if lastHeartbeatTime < time.Now().UnixMilli()-int64(electionTimeout) {
			// 重新随机选举超时时间
			// TODO
			//rf.electionTimeoutLock.Lock()
			//rf.electionTimeout = 400 + rand.Intn(200)
			//rf.electionTimeoutLock.Unlock()
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
		rf.leaderSendAppendEntriesRPC()

		time.Sleep(120 * time.Millisecond)
	}
}
