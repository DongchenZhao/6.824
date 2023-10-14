package raft

import (
	"log"
	"strconv"
)

func selectColor(inputColor string) string {
	color := "\033[0m"
	switch inputColor {
	case "red":
		color = "\033[31m"
	case "green":
		color = "\033[32m"
	case "yellow":
		color = "\033[33m"
	case "blue":
		color = "\033[34m"
	case "purple":
		color = "\033[35m"
	case "skyblue":
		color = "\033[36m"
	case "default":
		color = "\033[0m"
	default:
		color = "\033[0m"
	}
	return color
}

func (rf *Raft) LogEntryToStr(index int) string {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	rf.logLock.RLock()
	if index >= len(rf.log) {
		rf.logLock.RUnlock()
		return ""
	}
	logStr := "[LogEntry] [index: " + strconv.Itoa(index) + "] - [Term: " + strconv.Itoa(rf.log[index].Term) + "] - [Command: "

	if rf.log[index].Command == nil {
		logStr += "nil]"
	} else if str, ok := rf.log[index].Command.(string); ok {
		if len(str) > 10 {
			str = str[:10]
		}
		logStr += "" + str + "]"
	} else if str, ok := rf.log[index].Command.(int); ok {
		str := strconv.Itoa(str)
		if len(str) > 10 {
			str = str[:10]
		}
		logStr += "" + str + "]"
	} else {
		logStr += "unknown]"
	}

	rf.logLock.RUnlock()
	return logStr
}

func (rf *Raft) PrintState() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	rf.nextIndexLock.RLock()
	rf.matchIndexLock.RLock()
	rf.logLock.RLock()
	rf.commitIndexLock.RLock()
	rf.currentTermLock.RLock()

	nextIndexStr := "["
	for i := 0; i < len(rf.nextIndex); i++ {
		nextIndexStr += strconv.Itoa(rf.nextIndex[i]) + " "
	}
	nextIndexStr += "]"

	matchIndexStr := "["
	for i := 0; i < len(rf.matchIndex); i++ {
		matchIndexStr += strconv.Itoa(rf.matchIndex[i]) + " "
	}
	matchIndexStr += "]"

	logStr := "["
	for i := 0; i < len(rf.log); i++ {
		logStr += strconv.Itoa(rf.log[i].Term) + " "
	}
	logStr += "]"

	commitIndex := rf.commitIndex

	currentTerm := rf.currentTerm

	rf.nextIndexLock.RUnlock()
	rf.matchIndexLock.RUnlock()
	rf.commitIndexLock.RUnlock()
	rf.currentTermLock.RUnlock()
	rf.logLock.RUnlock()

	stateStr := "            [STATE]"

	rf.roleLock.RLock()
	switch rf.role {
	case 0:
		stateStr += "[Follower " + strconv.Itoa(rf.me) + "]" + " [Term " + strconv.Itoa(currentTerm) + "]" + " [CommitIndex " + strconv.Itoa(commitIndex) + "]" + " [Log " + logStr + "]"
	case 1:
		stateStr += "[Candidate " + strconv.Itoa(rf.me) + "]" + " [Term " + strconv.Itoa(currentTerm) + "]" + " [CommitIndex " + strconv.Itoa(commitIndex) + "]" + " [Log " + logStr + "]"
	case 2:
		stateStr += "[Leader " + strconv.Itoa(rf.me) + "]" + " [Term " + strconv.Itoa(currentTerm) + "]" + " [CommitIndex " + strconv.Itoa(commitIndex) + "]" + " [Log " + logStr + "]" + " [NextIndex " + nextIndexStr + "]" + " [MatchIndex " + matchIndexStr + "]"
	}
	rf.roleLock.RUnlock()

	PrintLog(stateStr, "purple")
}

func PrintLog(content string, options ...string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	color := "" // default color
	serverId := "-1"
	if len(options) > 0 {
		color = options[0]
	}
	if len(options) > 1 {
		serverId = options[1]
	}

	//if color == "purple" {
	//	return
	//}

	selectedColor := selectColor(color)
	if serverId == "-1" {
		log.Println(selectedColor + content + "\033[0m")
		return
	}
	log.Println(selectedColor + "[Server " + serverId + "]: " + content + "\033[0m")
}

func (rf *Raft) PrintRaftLog() {
	rf.logLock.RLock()
	rfLog := make([]LogEntry, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		rfLog[i].Term = rf.log[i].Term
		rfLog[i].Command = rf.log[i].Command
	}
	rf.logLock.RUnlock()

	if len(rfLog) == 0 {
		PrintLog("[SERVER LOG]: EMPTY", "red", strconv.Itoa(rf.me))
		return
	}

	logStr := "[SERVER LOG]: "

	for i := 0; i < len(rfLog); i++ {
		logStr += strconv.Itoa(rfLog[i].Term)
		if rfLog[i].Command == nil {
			logStr += "[nil]"
			continue
		} else if str, ok := rfLog[i].Command.(string); ok {
			if len(str) > 10 {
				str = str[:10]
			}
			logStr += "[" + str + "]"
		} else if str, ok := rfLog[i].Command.(int); ok {
			str := strconv.Itoa(str)
			if len(str) > 10 {
				str = str[:10]
			}
			logStr += "[" + str + "]"
		} else {
			logStr += "[unknown]"
		}

	}
	PrintLog(logStr, "red", strconv.Itoa(rf.me))

}

func PrintSplitLine(content string) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	line := "==============================="
	log.Printf("\n%s\n", line+content+line)
}
