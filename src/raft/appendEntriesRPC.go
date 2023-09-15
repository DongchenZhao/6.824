package raft

import (
	"fmt"
	"strconv"
)

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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntriesReqHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()
	if args.Term < currentTerm { // 过期leader，拒绝
		PrintLog(fmt.Sprintf("Reject AppendEntries RPC from [Leader %d]", args.LeaderId), "yellow", strconv.Itoa(rf.me))
		reply.Term = currentTerm
		reply.Success = false
	} else {
		// leader term更大，或两者相等，转为follower（即使当前role已经是follower）
		// 转化为follower的时候重置了election timer，但上面拒绝leader不会重置election timer
		// 此时，即使Success=false也不会使leader转为follower，只表示rf和leader日志不匹配
		rf.toFollower(args.Term)
		reply.Term = args.Term

		// log replication part
		compareSuccess, XTerm, XIndex, XLen := rf.compareAndHandleLog(args.PrevLogTerm, args.PrevLogIndex)
		reply.Success = compareSuccess

	}
}

// 处理AppendEntries RPC的reply
func (rf *Raft) appendEntriesRespHandler(reply *AppendEntriesReply) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()

	if reply.Term > currentTerm { // 当前server是过期leader，转为follower
		rf.toFollower(reply.Term)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesReqHandler", args, reply)
	return ok
}
