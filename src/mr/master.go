package mr

import (
	"fmt"
	"log"
	"os"
)
import "net"
import "net/rpc"
import "net/http"

var Jobid int = 0

type JobType int

const (
	Map JobType = iota
	Reduce
)

type Job struct {
	JobType     JobType
	JobId       int
	JobFileName string
	ReduceId    int
	ReduceNum   int
}

type Master struct {
	// Your definitions here.
	// 使用channel来存放
	JobChannelMap    chan *Job
	JobChannelReduce chan *Job
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) FromWorker(args *ExampleArgs, reply *Job) error {
	fmt.Print(args.X)
	*reply = *<-m.JobChannelMap
	return nil
}

func getJobId() (a int) {
	a = Jobid
	Jobid++
	return a
}
func getReduceId() (a int) {
	return 0
}
func (m *Master) MasterFindMap(filename []string, reduceNum int) {
	for _, v := range filename {
		job := Job{
			JobType:     Map,
			JobFileName: v,
			ReduceId:    getReduceId(),
			JobId:       getJobId(),
			ReduceNum:   reduceNum,
		}
		m.JobChannelMap <- &job
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		JobChannelMap:    make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
	}
	m.MasterFindMap(files, nReduce)
	m.server()

	// Your code here.

	return &m
}
