package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {

		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{len(rf.log), rf.me, rf.State, rf.currentTetm}, a...)
		fmt.Printf(format, a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
