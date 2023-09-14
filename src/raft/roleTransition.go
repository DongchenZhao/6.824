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
	// TODO lab 2B
	//lastLogIndex := len(rf.log) - 1
	//lastLogTerm := rf.log[lastLogIndex].Term
	lastLogIndex := 0
	lastLogTerm := currentTerm
	rf.logLock.RUnlock()

	// 遍历rf的peer，并发地向每个peer发送RequestVote RPC

	for i := 0; i < len(rf.peers); i++ {
		curI := i
		go func() {
			if curI == rf.me { // 跳过自己
				return
			}
			PrintLog("RequestVote send to [Server "+strconv.Itoa(curI)+"]", "default", strconv.Itoa(rf.me))
			requestVoteArgs := RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(curI, &requestVoteArgs, &requestVoteReply)
			if !ok {
				PrintLog("RequestVote send to [Server "+strconv.Itoa(curI)+"] failed", "red", strconv.Itoa(rf.me))
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
	if currentTerm != term { // 对方term更新，更新自己的term，重置voteFor，voteCnt
		PrintLog("-> follower, update term to ["+strconv.Itoa(term)+"], previous term ["+strconv.Itoa(currentTerm)+"]", "green", strconv.Itoa(rf.me))

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

	PrintLog("-> leader", "green", strconv.Itoa(rf.me))
	// 重置一些状态
	rf.roleLock.Lock()
	defer rf.roleLock.Unlock()
	rf.lastHeartbeatTimeLock.Lock()
	defer rf.lastHeartbeatTimeLock.Unlock()
	rf.voteForLock.Lock()
	defer rf.voteForLock.Unlock()
	rf.voteCntLock.Lock()
	defer rf.voteCntLock.Unlock()
	rf.currentTermLock.Lock()
	defer rf.currentTermLock.Unlock()

	rf.role = 2
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.curLeader = rf.me
	// 启动go routine，定期发送心跳包
	go rf.leaderTick()

}
