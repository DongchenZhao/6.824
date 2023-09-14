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

func (rf *Raft) PrintRaftLog(serverId int) {
	rf.logLock.RLock()
	rfLog := rf.log
	rf.logLock.RUnlock()
	colorList := []string{"red", "green", "yellow", "blue", "purple", "skyblue"}
	// 遍历log每一条，相同term用同种颜色打印
	for i := 0; i < len(rfLog); i++ {
		curTerm := rfLog[i].Term
		curTermStr := strconv.Itoa(curTerm)
		curColor := selectColor(colorList[curTerm%len(colorList)])
		if i == 0 {
			log.Print("[Server " + strconv.Itoa(serverId) + "] logs: ")
		}
		log.Print(curColor + curTermStr + "\033[0m" + "  ")
	}
}

func (rf *Raft) compareLog(prevLogIndex int, prevLogTerm int) bool {
	rf.logLock.RLock()
	defer rf.logLock.RUnlock()
	if prevLogIndex >= len(rf.log) {
		return false // TODO return XTerm, XIndex, XLen
	}
	if rf.log[prevLogIndex].Term != prevLogTerm {
		return false
	}
	return true
}
