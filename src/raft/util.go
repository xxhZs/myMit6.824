package raft

import (
	"log"
	"strconv"
)

// Debugging
const Debug = 1

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		format = "id:" + strconv.Itoa(rf.me) + format
		log.Printf(format, a...)
	}
	return
}
