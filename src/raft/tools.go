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

// TODO 修改
func (rf *Raft) compareLog(prevLogIndex int, prevLogTerm int) (bool, int, int, int) {
	XTerm := -1
	XIndex := -1
	XLen := 0
	rf.logLock.RLock()
	defer rf.logLock.RUnlock()

	// 当前server log长度不够，返回rf.log的最后一组term的第一条的index（日志长度为0在getXLogInfo中有处理）
	if len(rf.log)-1 < prevLogIndex {
		XTerm, XIndex, XLen = rf.getXLogInfo(-1)
		return false, XTerm, XIndex, XLen
	}

	// 匹配成功，但当前server日志可能过长
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return true, XTerm, XIndex, XLen
	}

	// 匹配失败，leader需要将nextIndex移动越过当前server冲突term的所有entry
	if rf.log[prevLogIndex].Term != prevLogTerm {
		XTerm, XIndex, XLen = rf.getXLogInfo(prevLogIndex)
		return false, XTerm, XIndex, XLen
	}

	return false, XTerm, XIndex, XLen
}

// 获取当前server log指定index的最后一组term的第一条log的index和len
func (rf *Raft) getXLogInfo(index int) (int, int, int) {
	XTerm := -1
	XIndex := -1
	XLen := 0

	rf.logLock.RLock()
	defer rf.logLock.RUnlock()

	// 日志长度为0
	if len(rf.log) == 0 {
		return XTerm, XIndex, XLen
	}

	// index为-1的时候，返回当前server最后一串term的第一个entry信息
	i := index
	if index == -1 {
		i = len(rf.log) - 1
	}
	XTerm = rf.log[i].Term
	for i != 0 {
		if rf.log[i].Term != XTerm {
			XIndex = i + 1
			XLen = len(rf.log) - XIndex
			return XTerm, XIndex, XLen
		}
		i--
	}
	// 日志只有一个term串
	return XTerm, i, len(rf.log)
}
