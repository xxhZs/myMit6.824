package mr

import (
	"6.824lab/raft"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"
import _ "6.824lab/kvraft"

var mu sync.Mutex

type JobType int

const (
	None JobType = iota
	Reduce
	KillJob
	Map
)

type Status int

const (
	Start Status = iota
	Using
	End
)

type Job struct {
	JobType     JobType
	JobId       int
	JobFileName []string
	ReduceNum   int
}

type Master struct {
	// Your definitions here.
	// 使用channel来存放
	JobChannelMap    chan *Job
	JobChannelReduce chan *Job
	ReduceNum        int
	MapNum           int
	FirstJobId       int
	MasterStatus     MasterStatus
	JobMetaMap       JobMetaMap
}

//这个是所谓的job元数据，主要用来保存job的一些信息
type JobMetaInfo struct {
	startTime time.Time
	status    Status
	JobPtr    *Job
}
type JobMetaMap struct {
	MetaMap map[int]*JobMetaInfo
}
type MasterStatus int

const (
	MapPhase = iota
	ReducePhase
	Waiting
)

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
	mu.Lock()
	defer mu.Unlock()
	raft.DPrintf("worker来领任务了")
	switch m.MasterStatus {
	case MapPhase:
		if len(m.JobChannelMap) > 0 {
			*reply = *<-m.JobChannelMap
			if !m.JobMetaMap.sendMetaJob(reply.JobId) {
				raft.DPrintf("找个job map开始运行了:", reply)
			}
		} else {
			//说明任务全部完成了，要更新状态了
			a, _ := m.JobMetaMap.checkJobNum()
			if a {
				raft.DPrintf("map任务完成，切换任务：", reply)
				m.nextStatus()
				*reply = Job{
					JobType: None,
				}
			}
		}
		break
	case ReducePhase:
		if len(m.JobChannelReduce) > 0 {
			*reply = *<-m.JobChannelReduce
			if !m.JobMetaMap.sendMetaJob(reply.JobId) {
				raft.DPrintf("找个job reduce开始运行了:", reply)
			}
		} else {
			//说明任务全部完成了，要更新状态了
			_, a := m.JobMetaMap.checkJobNum()
			if a {
				raft.DPrintf("reduce任务完成，切换任务：", reply)
				m.nextStatus()
				*reply = Job{
					JobType: None,
				}
			}
		}
		break
	default:
		raft.DPrintf("没有任务给worker")
		reply.JobType = KillJob
	}
	return nil
}

func (m *Master) getReduceId() (a int) {
	jobId := m.FirstJobId
	m.FirstJobId += 1
	return jobId
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
	ret = m.MasterStatus == Waiting
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
		ReduceNum:        nReduce,
		MapNum:           len(files),
		FirstJobId:       0,
		MasterStatus:     MapPhase,
		JobMetaMap: JobMetaMap{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
	}
	m.MasterFindMap(files, nReduce)
	m.server()

	go m.isTimeOut()
	// Your code here.

	return &m
}

//map方法的封装
func (m *Master) MasterFindMap(filename []string, reduceNum int) {
	for _, v := range filename {
		job := Job{
			JobType:     Map,
			JobFileName: []string{v},
			JobId:       m.getReduceId(),
			ReduceNum:   reduceNum,
		}
		jobMetaInfo := JobMetaInfo{
			status: Start,
			JobPtr: &job,
		}
		//将job元信息放入map
		m.putMap(&jobMetaInfo)
		//将job放入队列中
		raft.DPrintf("将map job加入队列：", job)
		m.JobChannelMap <- &job
	}
}

//reduce
func (m *Master) MasterFindReduce() {
	for i := 0; i < m.ReduceNum; i++ {
		job := Job{
			JobType:     Reduce,
			JobId:       m.getReduceId(),
			JobFileName: getReduceFileName(i, "main"),
		}
		jobMetaInfo := JobMetaInfo{
			status: Start,
			JobPtr: &job,
		}
		//将job元信息放入map
		m.putMap(&jobMetaInfo)
		//将job放入队列中
		raft.DPrintf("将reduce job加入队列：", job)
		m.JobChannelReduce <- &job
	}
}
func (m *Master) putMap(jobMetaInfo *JobMetaInfo) bool {
	jobId := jobMetaInfo.JobPtr.JobId
	if m.JobMetaMap.MetaMap[jobId] == nil {
		raft.DPrintf("将job元信息", jobId, "加入队列")
		m.JobMetaMap.MetaMap[jobId] = jobMetaInfo
	} else {
		raft.DPrintf("这个job元信息已经存在：", jobId)
		return false
	}
	return true
}

//更新状态
func (j *JobMetaMap) sendMetaJob(jobId int) bool {
	jobMetaInfo, err := j.MetaMap[jobId]
	if !err || jobMetaInfo.status != Start {
		raft.DPrintf("这个job已经不在start", jobId)
		return false
	}
	jobMetaInfo.startTime = time.Now()
	jobMetaInfo.status = Using
	return true
}

//master的状态变化,其实主要是map转换为reduce，以及reduce转换为waiting
func (m *Master) nextStatus() {
	if m.MasterStatus == MapPhase {
		m.MasterFindReduce()
		m.MasterStatus = ReducePhase
	} else if m.MasterStatus == ReducePhase {
		m.MasterStatus = Waiting
	} else {
		raft.DPrintf("master状态不对")
	}
}

/*
* 通过reduceID来找到相对应的文件名
 */
func getReduceFileName(reduceId int, path string) []string {
	path1, _ := os.Getwd()
	rd, err := ioutil.ReadDir(path1)
	if err != nil {
		raft.DPrintf("错误：getReduceFileName打开目录错误", path)
		return nil
	}
	var res []string
	for _, fi := range rd {
		if !fi.IsDir() {
			fiName := fi.Name()
			if strings.HasPrefix(fiName, "mr1-out-") && strings.HasSuffix(fiName, strconv.Itoa(reduceId)) {
				fullName := fiName
				res = append(res, fullName)
			}
		}
	}
	return res
}
func (j JobMetaMap) checkJobNum() (bool, bool) {
	mapNoDo := 0
	mapDo := 0
	reduceNoDo := 0
	reduceDo := 0
	for _, job := range j.MetaMap {
		if job.JobPtr.JobType == Map {
			if job.status == End {
				mapDo += 1
			} else {
				mapNoDo += 1
			}
		} else {
			if job.status == End {
				reduceDo += 1
			} else {
				reduceNoDo += 1
			}
		}
	}
	return (mapDo > 0 && mapNoDo == 0), (reduceDo > 0 && reduceNoDo == 0)
}

// 任务完成的方法
func (m *Master) JobDone(args *Job, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case Map:
		job, OK := m.JobMetaMap.MetaMap[args.JobId]
		if OK && job.status == Using {
			job.status = End
			raft.DPrintf("job map已经完成：", job.JobPtr.JobFileName)
		} else {
			raft.DPrintf("错误：job map已经完成或者没有开始", job.JobPtr.JobFileName)
		}
		break
	case Reduce:
		job, OK := m.JobMetaMap.MetaMap[args.JobId]
		if OK && job.status == Using {
			job.status = End
			raft.DPrintf("job reduce已经完成：", job)
		} else {
			raft.DPrintf("错误：job reduce已经完成或者没有开始", job)
		}
		break
	default:
		raft.DPrintf("错误： job错误")
	}
	return nil
}

//超时管理的方法
func (m *Master) isTimeOut() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if m.MasterStatus == Waiting {
			mu.Unlock()
			break
		}

		for _, job := range m.JobMetaMap.MetaMap {
			if job.status == Using {
				raft.DPrintf("job依旧在运行", job, time.Now().Sub(job.startTime))
			}
			if job.status == Using && time.Now().Sub(job.startTime) > time.Second*5 {
				raft.DPrintf("job超时了", job, time.Now().Sub(job.startTime))
				if job.JobPtr.JobType == Map {
					m.JobChannelMap <- job.JobPtr
					job.status = Start
				} else if job.JobPtr.JobType == Reduce {
					m.JobChannelReduce <- job.JobPtr
					job.status = Start
				}
			}

		}
		mu.Unlock()
	}
}
