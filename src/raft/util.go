package raft

import (
	"fmt"
)

// Debugging
const Debug = 1

func DPrintf(len int, me int, state string, term int, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {

		format = "%v: [peer %v (%v) at Term %v] " + format + "\n"
		a = append([]interface{}{len, me, state, term}, a...)
		fmt.Printf(format, a...)
	}
	return
}
