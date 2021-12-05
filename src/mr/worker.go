package mr

import (
	"6.824lab/raft"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	alive := true
	for alive {
		job := CallMaster()
		//if job == nil
		switch job.JobType {
		case Map:
			doMap(job, mapf)
			raft.DPrintf("domap", job)
			CallMasterEnd(job)
			break
		case Reduce:
			doReduce(job, reducef)
			raft.DPrintf("doreduce", job)
			CallMasterEnd(job)
			break
		case KillJob:
			raft.DPrintf("killjob", job)
			alive = false
			break
		default:
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
	}

}

//向master发送结束信号
func CallMasterEnd(job Job) {
	reply := ExampleReply{}
	args := job
	call("Master.JobDone", &args, &reply)
}
func doMap(job Job, mapf func(string, string) []KeyValue) {
	resultMap := MakeMap(job, mapf)
	for i := 0; i < job.ReduceNum; i++ {
		oname := "mr1-out-" + strconv.Itoa(job.JobId) + "-" + strconv.Itoa(i)
		file, err := os.Create(oname)
		defer file.Close()
		encoder := json.NewEncoder(file)
		for _, kv := range resultMap[i] {
			err = encoder.Encode(kv)
		}
		if err != nil {
			raft.DPrintf("Encoder failed", err.Error())
		}
	}
}
func doReduce(job Job, reducef func(string, []string) string) {
	jobId := job.JobId
	jobName := readFileByJson(job.JobFileName)

	sort.Sort(ByKey(jobName))
	oname := "mr-out-" + strconv.Itoa(jobId)
	ofile, _ := os.Create(oname)
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(jobName) {
		j := i + 1
		for j < len(jobName) && jobName[j].Key == jobName[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, jobName[k].Value)
		}
		output := reducef(jobName[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", jobName[i].Key, output)

		i = j
	}

	ofile.Close()

}
func readFileByJson(jobFile []string) []KeyValue {
	kva := []KeyValue{}
	for _, job := range jobFile {
		jobfile, _ := os.Open(job)
		json := json.NewDecoder(jobfile)
		for {
			var kv KeyValue
			if err := json.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		jobfile.Close()
	}
	return kva
}

//使用该方法和master进行通信，获取文件
func CallMaster() Job {
	reply := Job{}
	args := ExampleArgs{}
	call("Master.FromWorker", &args, &reply)
	return reply
}

// map的使用策略
func MakeMap(job Job, mapf func(string, string) []KeyValue) [][]KeyValue {
	raft.DPrintf("打日誌看一下错误的job", job)
	file, err := os.Open(job.JobFileName[0])
	if err != nil {
		log.Fatalf("cannot open %v err", job.JobFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v err", job.JobFileName)
	}
	file.Close()
	kva := mapf(job.JobFileName[0], string(content))
	rn := job.ReduceNum
	HashKv := make([][]KeyValue, rn)
	for _, kva1 := range kva {
		HashKv[ihash(kva1.Key)%rn] = append(HashKv[ihash(kva1.Key)%rn], kva1)
	}
	return HashKv
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
