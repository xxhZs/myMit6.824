package raft

import (
	"log"
	"strconv"
)

// Debugging
const Debug = 1

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {

		// 设置写入文件的log日志的格式
		format = "id:" + strconv.Itoa(rf.me) + format
		log.Printf(format, a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
