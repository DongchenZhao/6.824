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
	Term     int
	Success  bool
	ServerId int
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) AppendEntriesReqHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()
	reply.ServerId = rf.me
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
		compareSuccess, XTerm, XIndex, XLen := rf.compareAndHandleLog(args.PrevLogTerm, args.PrevLogIndex, args.Entries)
		reply.Success = compareSuccess
		reply.XTerm = XTerm
		reply.XIndex = XIndex
		reply.XLen = XLen

		if !compareSuccess {
			return
		}

		// 匹配成功，Figure2中AppendEntriesRPC的4步骤已在rf.compareAndHandleLog中完成(),对commitIndex做操作
		rf.commitIndexLock.RLock()
		commitIndex := rf.commitIndex
		rf.commitIndexLock.RUnlock()

		// AE RPC中的commitIndex更大，更新rf的commitIndex
		// 这也避免了rf接收到陈旧AE RPC的情况
		if args.LeaderCommit > commitIndex {
			rf.logLock.Lock()
			logLen := len(rf.log)
			rf.logLock.Unlock()
			min := args.LeaderCommit
			if logLen < args.LeaderCommit {
				min = logLen
			}
			rf.commitIndexLock.Lock()
			rf.lastAppliedLock.Lock()

			rf.commitIndex = min
			rf.lastApplied = rf.commitIndex

			rf.lastAppliedLock.Unlock()
			rf.commitIndexLock.Unlock()

			rf.sendNewlyCommittedLog(commitIndex)
		}
	}
}

// 处理AppendEntries RPC的reply
func (rf *Raft) appendEntriesRespHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.currentTermLock.RLock()
	currentTerm := rf.currentTerm
	rf.currentTermLock.RUnlock()

	if reply.Term > currentTerm { // 当前server是过期leader，转为follower
		rf.toFollower(reply.Term)
	}

	// 2B part
	rf.matchIndexLock.RLock()
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	rf.matchIndexLock.RUnlock()

	rf.logLock.Lock()
	log := make([]LogEntry, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		log[i].Term = rf.log[i].Term
	}
	rf.logLock.Unlock()

	// 对陈旧AE RPC的响应进行处理，
	// 当前leader已经更新了nextIndex，但是仍然收到陈旧的冲突response，
	// 陈旧的resp中包含冲突的日志index小于leader知道的对方已匹配日志
	knownMatchIndex := matchIndex[reply.ServerId]
	if (!reply.Success) && reply.XIndex < knownMatchIndex {
		PrintLog(fmt.Sprintf("Expired FAILURE AE RPC response from [Server %d]", reply.ServerId), "yellow", strconv.Itoa(rf.me))
		return
	} else if (args.PrevLogIndex + len(args.Entries)) < knownMatchIndex {
		PrintLog(fmt.Sprintf("Expired SUCCESS AE RPC response from [Server %d]", reply.ServerId), "yellow", strconv.Itoa(rf.me))
		return
	}

	// 1. 对方日志太短
	if !reply.Success && reply.XTerm == -1 {
		rf.nextIndexLock.Lock()
		defer rf.nextIndexLock.Unlock()
		rf.nextIndex[reply.ServerId] = reply.XIndex
	}
	// 2. 日志在prevLogIndex处冲突
	// 分情况讨论
	// 这种情况下，XLen貌似没用
	if !reply.Success {
		// leader查找自己日志中是否有XTerm
		hasXTerm := false
		nextIndex := -1
		for i := len(log) - 1; i >= 0; i-- {
			if log[i].Term == reply.XTerm {
				hasXTerm = true
				nextIndex = i
				break
			}
		}
		if hasXTerm { // 2.1 leader有term相关信息，nextIndex设置为term串最后一个entry的index + 1
			if nextIndex == -1 {
				PrintLog("nextIndex error", "red", strconv.Itoa(rf.me))
			}
			rf.nextIndexLock.Lock()
			defer rf.nextIndexLock.Unlock()
			rf.nextIndex[reply.ServerId] = nextIndex

		} else { // 2.2 leader没有term相关信息，nextIndex设置为XIndex
			rf.nextIndexLock.Lock()
			defer rf.nextIndexLock.Unlock()
			rf.nextIndex[reply.ServerId] = reply.XIndex
		}
	}
	// 3. 对方成功匹配日志
	// 根据AE RPC中entries的情况，可以推算出对方日志复制情况
	// 根据推算出的情况，更新nextIndex和matchIndex
	// 这里已经排除了陈旧RPC
	if reply.Success {
		rf.nextIndexLock.Lock()
		nextIndex := args.PrevLogIndex + len(args.Entries) + 1
		rf.nextIndex[reply.ServerId] = nextIndex
		rf.nextIndexLock.Unlock()

		rf.matchIndexLock.Lock()
		rf.matchIndex[reply.ServerId] = nextIndex - 1
		rf.matchIndexLock.Unlock()

		// 将nextIndex转为字符串
		rf.nextIndexLock.RLock()
		nextIndexStr := "["
		for i := 0; i < len(rf.nextIndex); i++ {
			nextIndexStr += strconv.Itoa(rf.nextIndex[i]) + " "
		}
		nextIndexStr += "]"
		PrintLog("AE RPC SUCCESS, nextIndex: "+nextIndexStr, "purple", strconv.Itoa(rf.me))
		rf.nextIndexLock.RUnlock()
		// 集群日志同步状态检查会定期触发，这里不需要再触发
		// TODO
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesReqHandler", args, reply)
	return ok
}

func (rf *Raft) leaderSendAppendEntriesRPC(isHeartBeat bool) {

	rf.nextIndexLock.RLock()
	rf.logLock.RLock()
	rf.currentTermLock.RLock()
	rf.commitIndexLock.RLock()

	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex

	nextIndex := make([]int, len(rf.nextIndex))
	copy(nextIndex, rf.nextIndex)

	log := make([]LogEntry, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		log[i].Term = rf.log[i].Term
	}

	rf.commitIndexLock.RUnlock()
	rf.nextIndexLock.RUnlock()
	rf.logLock.RUnlock()
	rf.currentTermLock.RUnlock()

	// leader发送含有entries的AE RPC
	for i := 0; i < len(rf.peers); i++ {
		curI := i
		go func() {
			if curI == rf.me { // 跳过自己
				return
			}
			//获取要发送的prevLogIndex和prevLogTerm
			prevLogIndex := nextIndex[curI] - 1
			prevLogTerm := currentTerm
			if prevLogIndex >= 0 { // 考虑一开始leader日志为空的情况
				prevLogTerm = log[prevLogIndex].Term
			}

			// 获取要发送的entries
			var entries []LogEntry
			if isHeartBeat {
				entries = nil
			} else {
				if prevLogIndex == len(rf.log)-1 {
					return
				}
				rf.logLock.RLock()
				entries = make([]LogEntry, len(rf.log)-prevLogIndex-1)
				for j := prevLogIndex + 1; j < len(rf.log); j++ {
					entries[j-prevLogIndex-1] = rf.log[j]
				}
				rf.logLock.RUnlock()
			}

			appendEntriesArgs := AppendEntriesArgs{currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, leaderCommit}
			appendEntriesReply := AppendEntriesReply{}

			// 打印消息日志
			if isHeartBeat {
				PrintLog("heartbeat send to [Server "+strconv.Itoa(curI)+"]", "default", strconv.Itoa(rf.me))
			} else {
				// 将entries内容拼成字符串，打印prevLogIndex, prevLogTerm和entriesStr
				entriesStr := "["
				for i := 0; i < len(entries); i++ {
					entriesStr += strconv.Itoa(entries[i].Term) + " "
				}
				entriesStr += "]"
				PrintLog("Sending appendEntries to [Server "+strconv.Itoa(curI)+"]"+"prevLogIndex: "+strconv.Itoa(prevLogIndex)+" prevLogTerm: "+strconv.Itoa(prevLogTerm)+" entries: "+entriesStr, "purple", strconv.Itoa(rf.me))
			}

			ok := rf.sendAppendEntries(curI, &appendEntriesArgs, &appendEntriesReply)
			if !ok {
				PrintLog("appendEntries send to [Server "+strconv.Itoa(curI)+"] failed", "yellow", strconv.Itoa(rf.me))
			}
			// 处理心跳包的reply
			rf.appendEntriesRespHandler(&appendEntriesArgs, &appendEntriesReply)
		}()
	}
}
