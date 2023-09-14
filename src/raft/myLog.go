package raft

import "log"

func PrintLog(content string, options ...string) {
	color := "\033[0m" // default color
	serverId := "-1"
	if len(options) > 0 {
		color = options[0]
	}
	if len(options) > 1 {
		serverId = options[1]
	}
	switch color {
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
	log.Println(color + "[Server " + serverId + "]: " + content + "\033[0m")
}
