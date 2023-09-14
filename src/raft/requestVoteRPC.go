package raft

import (
	"strconv"
	"time"
)

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
	// 隐含条件是自己term和requestVoteArgs.Term相等（上面currentTerm更小的时候已经和candidate的term同步了）
	// 更新voteFor和时钟
	rf.voteForLock.Lock()
	defer rf.voteForLock.Unlock()

	// 只有决定投票的时候才更新时钟（https://thesquareplanet.com/blog/students-guide-to-raft/）
	rf.lastHeartbeatTimeLock.Lock()
	rf.lastHeartbeatTime = time.Now().UnixMilli()
	rf.lastHeartbeatTimeLock.Unlock()

	rf.votedFor = args.CandidateId

	reply.Term = args.Term
	reply.VoteGranted = true
	PrintLog(" vote for [Server "+strconv.Itoa(args.CandidateId)+"]", "default", strconv.Itoa(rf.me))
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
