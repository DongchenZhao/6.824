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
	selectedColor := selectColor(color)
	log.Println(selectedColor + "[Server " + serverId + "]: " + content + "\033[0m")
}

func (rf *Raft) PrintRaftLog() {
	rf.logLock.RLock()
	rfLog := make([]LogEntry, len(rf.log))
	for i := 0; i < len(rf.log); i++ {
		rfLog[i].Term = rf.log[i].Term
	}
	rf.logLock.RUnlock()

	if len(rfLog) == 0 {
		PrintLog("[SERVER LOG]: EMPTY", "red", strconv.Itoa(rf.me))
		return
	}

	logStr := "[SERVER LOG]: "

	for i := 0; i < len(rfLog); i++ {
		logStr += strconv.Itoa(rfLog[i].Term) + " "
	}
	PrintLog(logStr, "red", strconv.Itoa(rf.me))

}
