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
	"strconv"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] 也就是serverId
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// ------ persistent state on all servers ------
	currentTerm int
	votedFor    int
	log         []LogEntry

	// ------ volatile state on all servers ------
	commitIndex int
	lastApplied int

	// ------ volatile state on leaders ------
	nextIndex  []int
	matchIndex []int

	// ------ other ------
	role              int // 0: follower, 1: candidate, 2: leader
	lastHeartbeatTime int64
	electionTimeout   int
	voteCnt           int
	curLeader         int

	// ------ locks ------
	// ------2A ------
	roleLock              sync.RWMutex
	lastHeartbeatTimeLock sync.RWMutex
	currentTermLock       sync.RWMutex
	voteForLock           sync.RWMutex
	voteCntLock           sync.RWMutex
	curLeaderLock         sync.RWMutex
	// ------2B ------
	logLock sync.RWMutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.currentTermLock.RLock()
	term = rf.currentTerm
	rf.currentTermLock.RUnlock()

	rf.roleLock.RLock()
	isleader = rf.role == 2
	rf.roleLock.RUnlock()

	return term, isleader
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		rf.currentTermLock.RLock()
		currentTerm := rf.currentTerm
		rf.currentTermLock.RUnlock()
		if role != 2 { // 不再是leader，结束这个go routine
			// PrintLog("No longer leader", "green", strconv.Itoa(rf.me))
			return
		}
		// 并发发送心跳包
		for i := 0; i < len(rf.peers); i++ {
			curI := i
			go func() {
				if curI == rf.me { // 跳过自己
					return
				}
				appendEntriesArgs := AppendEntriesArgs{currentTerm, rf.me, 0, 0, nil, 0}
				appendEntriesReply := AppendEntriesReply{}
				PrintLog("heartbeat send to [Server "+strconv.Itoa(curI)+"]", "default", strconv.Itoa(rf.me))
				ok := rf.sendAppendEntries(curI, &appendEntriesArgs, &appendEntriesReply)
				if !ok {
					PrintLog("heartbeat send to [Server "+strconv.Itoa(curI)+"] failed", "red", strconv.Itoa(rf.me))
				}
				// 处理心跳包的reply
				rf.appendEntriesRespHandler(&appendEntriesReply)
			}()
		}

		time.Sleep(150 * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	PrintLog("Starting server", "default", strconv.Itoa(rf.me))

	//初始化一些field，这里不考虑并发问题
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	// 重置一下状态(其实只重置了时钟)
	rf.toFollower(0)

	// 初始化electionTimeout，设置为500-800ms之间的随机数
	rf.electionTimeout = 500 + rand.Intn(300)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
