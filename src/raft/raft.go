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
	"log"
	"math/rand"
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
	roleLock              sync.RWMutex
	lastHeartbeatTimeLock sync.RWMutex
	currentTermLock       sync.RWMutex
	logLock               sync.RWMutex
	voteForLock           sync.RWMutex
	voteCntLock           sync.RWMutex
	curLeaderLock         sync.RWMutex
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// an RPC handler should ignore RPCs with old terms

	// 更新时钟
	rf.lastHeartbeatTimeLock.Lock()
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	rf.lastHeartbeatTimeLock.Unlock()

	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()
	rf.voteForLock.RLock()
	votedFor := rf.votedFor
	rf.voteForLock.RUnlock()

	// 如果自己term更小，无论自己是哪种身份，转为follower，然后刷新term和voteFor
	if args.Term > currentTerm {
		rf.toFollower(args.Term)
		currentTerm = args.Term
		votedFor = -1
	}

	// 1.如果自己有更大的term，拒绝投票
	// 2.如果自己已经投过票，也拒绝投票
	// 3.如果自己日志更新，也拒绝投票
	// 隐含了自己是term更大的leader的情况
	if args.Term < currentTerm || votedFor != -1 {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}

	// TODO 自己的log更新，拒绝投票

	// 同意投票
	// 隐含条件是自己term和requestVoteArgs.Term相等
	// 更新voteFor
	rf.voteForLock.Lock()
	defer rf.voteForLock.Unlock()

	rf.votedFor = args.CandidateId

	reply.Term = args.Term
	reply.VoteGranted = true
	log.Printf("Server No.%d vote for server No.%d", rf.me, args.CandidateId)
}

func (rf *Raft) requestVoteRespHandler(reply *RequestVoteReply) {

	// 自己不是candidate了，忽略reply
	rf.roleLock.RLock()
	role := rf.role
	rf.roleLock.RUnlock()
	if role != 1 {
		return
	}

	// 忽略过期term的RPC reply
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()
	if reply.Term < currentTerm {
		return
	}

	// 处理reply.VoteGranted false的情况
	if !reply.VoteGranted && reply.Term > currentTerm {
		rf.toFollower(reply.Term)
		return
	}

	// 得到批准投票，统计票数
	if reply.VoteGranted {
		rf.voteCntLock.Lock()
		rf.voteCnt++
		voteCnt := rf.voteCnt
		rf.voteCntLock.Unlock()
		if voteCnt > len(rf.peers)/2 { // 获得大多数选票，成为leader
			rf.toLeader()
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()
	if args.Term < currentTerm { // 过期leader，拒绝
		log.Printf("\u001B[31mServer No.%d reject AppendEntries RPC from leader No.%d\u001B[0m", rf.me, args.LeaderId)
		reply.Term = currentTerm
		reply.Success = false
	} else { // leader term更大，或两者相等，转为follower（即使当前role已经是follower）
		rf.toFollower(args.Term)
		reply.Term = args.Term
		reply.Success = true
	}
}

// 处理AppendEntries RPC的reply
func (rf *Raft) appendEntriesRespHandler(reply *AppendEntriesReply) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()

	if reply.Term > currentTerm { // 过期leader，转为follower
		rf.toFollower(reply.Term)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

// follower或candidate 超时未收到心跳触发
// 0. role设置为1
// 1. 增加currentTerm
// 2. voteFor设置为自己
// 3. 重置lastHeartbeatTime
// 4. 向其他server发送RequestVote RPC
func (rf *Raft) toCandidate() {
	// 超时，改变角色，重置voteFor，开始选举
	log.Printf("Server No.%d start election", rf.me)
	rf.roleLock.Lock()
	rf.role = 1
	defer rf.roleLock.Unlock()

	// 更新term
	rf.currentTermLock.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	defer rf.currentTermLock.Unlock()

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
	//println(555)

	for i := 0; i < len(rf.peers); i++ {
		curI := i
		go func() {
			if curI == rf.me { // 跳过自己
				return
			}
			log.Printf("Server No.%d send RequestVote RPC to server No.%d", rf.me, curI)
			requestVoteArgs := RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(curI, &requestVoteArgs, &requestVoteReply)
			if !ok {
				log.Printf("\033[31mServer No.%d send RequestVote RPC to server No.%d failed\033[0m", rf.me, curI)
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

	// 重置一些状态
	rf.roleLock.Lock()
	defer rf.roleLock.Unlock()
	rf.currentTermLock.Lock()
	defer rf.currentTermLock.Unlock()
	rf.lastHeartbeatTimeLock.Lock()
	defer rf.lastHeartbeatTimeLock.Unlock()
	rf.voteForLock.Lock()
	defer rf.voteForLock.Unlock()
	rf.voteCntLock.Lock()
	defer rf.voteCntLock.Unlock()

	rf.role = 0
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	if currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.voteCnt = 0
	}
}

func (rf *Raft) toLeader() {

	log.Printf("Server No.%d become leader", rf.me)
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

func (rf *Raft) leaderTick() {
	for rf.killed() == false {
		rf.roleLock.RLock()
		role := rf.role
		rf.roleLock.RUnlock()
		rf.currentTermLock.RLock()
		currentTerm := rf.currentTerm
		rf.currentTermLock.RUnlock()
		if role != 2 { // 不再是leader，结束这个go routine
			log.Printf("Server No.%d is no longer leader", rf.me)
			return
		}
		// 并发发送心跳包
		for i := 0; i < len(rf.peers); i++ {
			curI := i
			go func() {
				if curI == rf.me { // 跳过自己
					return
				}
				// log.Printf("Server No.%d send heartbeat to server No.%d", rf.me, curI)
				appendEntriesArgs := AppendEntriesArgs{currentTerm, rf.me, 0, 0, nil, 0}
				appendEntriesReply := AppendEntriesReply{}
				log.Printf("leader No.%d send heartbeat to server No.%d", rf.me, curI)
				ok := rf.sendAppendEntries(curI, &appendEntriesArgs, &appendEntriesReply)
				if !ok {
					log.Printf("\033[31mServer No.%d send heartbeat to server No.%d failed\033[0m", rf.me, curI)
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
	log.Printf("Starting server No.%d", rf.me)

	//初始化一些field，这里不考虑并发问题
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	//
	//// 初始化lastHeartbeatTime，设置为当前时间的毫秒数
	//rf.lastHeartbeatTimeLock.Lock()
	//rf.lastHeartbeatTime = time.Now().UnixMilli()
	//rf.lastHeartbeatTimeLock.Unlock()

	rf.toFollower(0)

	// 初始化electionTimeout，设置为500-800ms之间的随机数
	rf.electionTimeout = 500 + rand.Intn(300)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
