package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824lab/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824lab/labrpc"

// import "bytes"
// import "6.824lab/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	IsSnap       bool
	SnapShot     []byte
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//补充的结构体:论文中存在的
	//服务器上持久存在的
	currentTetm int        //服务器上最后的任期号
	votedFor    int        // 当前获取选票的候选人id
	log         []LogEntry // 日志的目录集，每一个条目包含一个用户状态机的指令和收到的任期号

	commitIndex int //已经提交的最大索引
	lastApplied int // 最后被应用到状态机的日志条目索引值

	nextIndex  []int //要发送的下一个index
	matchIndex []int //已经复制的日志最高索引值

	//自己添加的必须有的结构
	State string      //目前的状态
	timer *time.Timer // 时钟计时，心跳计时用
	//electionTimeout time.Duration //超时时间 200-400ms
	votesCount int // 投票计数
	//心跳计时与超时选举计时
	electionTimer *time.Timer
	heaterTimer   *time.Timer
	applyCh       chan ApplyMsg
	//实现快照添加
	SnaplastIndex int
	SnaplastTerm  int

	IsSendSnap []bool
}

//快照需要的辅助反法
func (rf *Raft) getAbsolutionLog(nowIndex int) int {
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "nowIndex%v+ rf.SnaplastIndex%v", nowIndex, rf.SnaplastIndex)
	if rf.SnaplastIndex == 0 {
		return nowIndex
	}
	return nowIndex + rf.SnaplastIndex + 1
}
func (rf *Raft) getNowLogIndex(Absolution int) int {
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "Absolution%v- rf.SnaplastIndex%v", Absolution, rf.SnaplastIndex)
	if rf.SnaplastIndex == 0 {
		return Absolution
	}
	return Absolution - rf.SnaplastIndex - 1
}
func (rf *Raft) getLenLog() int {
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "len(rf.log)%v+ rf.SnaplastIndex%v", len(rf.log), rf.SnaplastIndex)
	if rf.SnaplastIndex == 0 {
		return len(rf.log)
	}
	return len(rf.log) + rf.SnaplastIndex + 1
}

//日志信息
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	fmt.Println("试一下")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTetm
	isleader = rf.State == "Leader"
	//DPrintf(123,123,rf.State,rf.currentTetm,"获取当前的状态，当前的任期：%v状态：%v id %v", term, rf.State, rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "序列化 %v,%v", rf.currentTetm, rf.votedFor)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTetm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.Persister.SaveRaftState(data)
}

//
// restore previously persisted state.·
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "反序列化")
	var currentTerm, votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "错误反序列化")
		return
	} else {
		rf.currentTetm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//选举结构
	Term          int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
	LastShopIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//选举返回结果

	Term        int
	VoteGranted int
	// 下面的是优化用的
	XTerm  int
	XIndex int
	XLen   int
}

//
// example RequestVote RPC handler.
//
/**
 这里是投票以后的动作
判断日志，本地的最新要比args的小，或者相等，但是长度本地要短，才会给你投票
1. 比较term args.term <  currentTerm false;
2. args.term >  currentTerm 直接变follower？
3. args.term =  currentTerm
	3.1 args.index > len(peers) :日志长度比较，true
	3.2 其他为false
4.当然还是要判断有没有投过票，主要能不能投票，这里可以通过日志看！！！！！
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到上面的投票信息 %v,%v", args, reply)
	voteGranted := 1
	//日志判断
	term := rf.SnaplastTerm
	if len(rf.log) > 0 {
		term = Max(term, rf.log[len(rf.log)-1].Term)
	}

	if (term > args.LastLogTerm) ||
		((term == args.LastLogTerm) && rf.getLenLog()-1 > args.LastLogIndex) {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "通过日志比较，当前的候选人 %v 无效 term%v", args.CandidateID, term)
		voteGranted = 0
	}

	//if(args.LastShopIndex < rf.SnaplastIndex){
	//	DPrintf(len(rf.log),rf.me,rf.State,rf.currentTetm,"快照不对 %v argshop:%v,Rfsnap:%v",args.CandidateID,args.LastShopIndex,rf.SnaplastIndex)
	//	voteGranted = 0
	//}
	if args.Term < rf.currentTetm {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来的任期 %v 太久远了，不给你投票", args.CandidateID)
		reply.Term = rf.currentTetm
		reply.VoteGranted = 0
		return
	}
	if args.Term > rf.currentTetm {
		reply.Term = args.Term
		rf.currentTetm = args.Term
		rf.convertTo("Follower")
		if voteGranted == 1 {
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来的选举有效:%v，已经变成follow:%v，给你投票", args.CandidateID, rf.me)
			rf.electionTimer.Reset(rf.getTimeOut())
			rf.votedFor = args.CandidateID
		} else {
			rf.votedFor = -1
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来的选举有效%v，已经变成follow%v，但是日志无效，不给投票", args.CandidateID, rf.me)
		}
		reply.VoteGranted = voteGranted
		rf.persist()
		return
	}
	if args.Term == rf.currentTetm {
		if rf.votedFor == -1 && voteGranted == 1 {
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来的选举有效%v，给投票%v", args.CandidateID, rf.me)
			rf.votedFor = args.CandidateID
			rf.persist()
			rf.electionTimer.Reset(rf.getTimeOut())
			rf.convertTo("Follower")
		} else {
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来的选举无效%v，不给投票%v", args.CandidateID, rf.me)
			reply.Term = rf.currentTetm
		}
		if rf.votedFor == args.CandidateID {
			reply.VoteGranted = 1
		} else {
			reply.VoteGranted = 0
		}
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", &args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来进行同步日志 %v", command)
	if rf.State != "Leader" {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "不是leader，不同步 %v", rf.State)
		return index, term, isLeader
	}
	nlog := LogEntry{Index: len(rf.log), Command: command, Term: rf.currentTetm}
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来进行同步日志 %v", nlog)
	isLeader = rf.State == "Leader"
	logcopy := append(rf.log, nlog)
	rf.log = make([]LogEntry, len(logcopy))
	copy(rf.log, logcopy)
	index = rf.getLenLog()
	term = rf.currentTetm
	rf.persist()
	//if rf.State == "Leader" {
	//	rf.sendLogAppendEntries(-1)
	//}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.heaterTimer.Stop()
	rf.electionTimer.Stop()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// 初始化的方法
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.me = me
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "初始化raft")
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTetm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "peers有 %v", len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.IsSendSnap = make([]bool, len(peers))
	rf.applyCh = applyCh
	//自己添加的必须有的结构
	rf.State = "Follower"

	//rf.leader = make(chan int)
	//初始化快照
	rf.SnaplastIndex = 0
	rf.SnaplastTerm = -1
	//此处进行超时等待调用 2
	if rf.heaterTimer != nil {
		rf.heaterTimer.Stop()
	}
	rf.heaterTimer = time.NewTimer(rf.getTimeOut())
	rf.electionTimer = time.NewTimer(HeartbeatInterval)
	go rf.selectTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) getTimeOut() time.Duration {
	timerD := time.Millisecond * time.Duration(100+rand.Intn(100))
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "超时时间 TimeOut %v", timerD)
	return timerD
}
func (rf *Raft) getElection() {
	defer rf.persist()
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "超时了，进行选举机制")
	//只有不是leader才会触发选举
	rf.State = "Candidate"
	rf.currentTetm += 1
	rf.electionTimer.Reset(rf.getTimeOut())
	rf.votedFor = rf.me
	rf.votesCount = 1
	//rf.isVote = true
	args := RequestVoteArgs{
		Term:          rf.currentTetm,
		CandidateID:   rf.me,
		LastLogIndex:  rf.getLenLog() - 1,
		LastShopIndex: rf.SnaplastIndex,
		LastLogTerm:   rf.SnaplastTerm,
	}
	if len(rf.log) > 0 {
		args.LastLogTerm = Max(rf.log[rf.getNowLogIndex(args.LastLogIndex)].Term, args.LastLogTerm)
	}
	//此处进行发送选举
	for serverNum := 0; serverNum < len(rf.peers); serverNum++ {
		if serverNum == rf.me {
			continue
		}
		//并发发送，快速
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送选举信息%v-->%v", rf.me, serverNum)
		//发送rpc选举信息
		go func(server int, args RequestVoteArgs) {
			var reply RequestVoteReply
			flag := rf.sendRequestVote(server, args, &reply)
			if flag {
				//处理结果，此处要进行各种操作
				//DPrintf(len(rf.log),rf.me,rf.State,rf.currentTetm,"发送成功%v-->%v", rf.me, serverNum)
				rf.setReplyVote(reply, server)
			}
		}(serverNum, args)
		//1
		//return这个raft已经不是leader
	}
}

/*
 此处的逻辑是，看投票结果和term
1. 返回的term < currentTerm 这个回应超时了
2. 返回的term > currentTerm 变为follower
3. 投票有效
*/
func (rf *Raft) setReplyVote(reply RequestVoteReply, server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "获取选举信息")
	if rf.currentTetm > reply.Term {
		//过时了
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "选举rpc过时 %v投票给%v", server, rf.me)
		return
	}
	// 条件满足
	if rf.State == "Candidate" && reply.VoteGranted == 1 {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "返回投票成功")
		rf.votesCount += 1
		if rf.votesCount >= len(rf.peers)/2+1 {
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "升级为leader，更新日志%v", rf.me)
			//被选举为leader,这个时候就要重置两个日志文件，进行发送了
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.IsSendSnap[i] = false
				//DPrintf("nextindex的长度 %v",len(rf.nextIndex))9
				//DPrintf("nextindex的长度 %v",len(rf.nextIndex))9
				rf.nextIndex[i] = rf.getLenLog()
				rf.matchIndex[i] = -1
				// 发送心跳！！
			}
			//1
			rf.convertTo("Leader")
		}
	} else {
		if reply.Term > rf.currentTetm {
			rf.currentTetm = reply.Term
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "选举失败重新计时%v", rf.me)
			rf.electionTimer.Reset(rf.getTimeOut())
			rf.convertTo("Follower")
			rf.persist()
		}
	}
}

var HeartbeatInterval time.Duration = time.Millisecond * time.Duration(50)

//重构计时方法
func (rf *Raft) selectTimer() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.State == "Follower" {
				//转移为follwer
				rf.convertTo("Candidate")
			} else {
				//开启选举
				rf.getElection()
			}
			rf.mu.Unlock()
		case <-rf.heaterTimer.C:
			rf.mu.Lock()
			if rf.State == "Leader" {
				rf.sendLogAppendEntries(-1)
				DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "leader%v,%v", rf.currentTetm, rf.State)
				rf.heaterTimer.Reset(HeartbeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entrys       []LogEntry
	LeaderCommit int
}
type InstallSnapshot struct {
	Term          int
	LeaderId      int
	Data          []byte
	SnaplastIndex int
	SnaplastTerm  int
}

func (rf *Raft) sendInstallSnapshot(serverNum int) {
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送快照")
	//appendEntries := rf.getAppendEntries(serverNum)
	installSnapshot := InstallSnapshot{
		Term:          rf.currentTetm,
		LeaderId:      rf.me,
		SnaplastIndex: rf.SnaplastIndex,
		SnaplastTerm:  rf.SnaplastTerm,
		Data:          rf.Persister.ReadSnapshot(),
	}
	go func(server int, args InstallSnapshot) {
		var reply RequestVoteReply
		rf.mu.Lock()
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送快照 %v ---> %v , %v", rf.me, server, args)
		rf.mu.Unlock()
		flag := rf.sendInstall(server, args, &reply)
		if !flag {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送快照 %v ---> %v , %v", rf.me, server, args)
		//if rf.currentTetm != args.Term{
		//	return
		//}
		if reply.Term != rf.currentTetm {
			return
		}
		if reply.VoteGranted == 0 {
			return
		}
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送快照成功 %v ---> %v , %v", rf.me, server, args)
		rf.IsSendSnap[server] = false
		rf.nextIndex[server] = args.SnaplastIndex + 1
		rf.matchIndex[server] = args.SnaplastIndex
		go rf.CommitLog()
	}(serverNum, installSnapshot)
}

func (rf *Raft) sendInstall(server int, args InstallSnapshot, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.Install", args, reply)
	return ok
}
func (rf *Raft) Install(args InstallSnapshot, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//这里会进行抛弃后续，然后下一个append来同步
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到快照")
	reply.Term = rf.currentTetm
	if args.Term < rf.currentTetm {
		return
	}
	if args.Term > rf.currentTetm {
		rf.currentTetm = args.Term
		rf.votedFor = args.LeaderId
		reply.VoteGranted = 0
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "任期不对")
		rf.electionTimer.Reset(rf.getTimeOut())
		rf.convertTo("Follower")
		rf.persist()
	}
	if args.SnaplastIndex <= rf.SnaplastIndex {
		return
	}
	//这里就是要看是哪种情况，要么是不够index，要么是超过index
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "args.SnaplastIndex %v", args.SnaplastIndex)
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "更新快照")
	if args.SnaplastIndex+1 < rf.getLenLog() {
		//这里代表的是多余，把后面的内容删除，让下一个append来更新
		if args.SnaplastTerm == rf.log[rf.getNowLogIndex(args.SnaplastIndex)].Term {
			//这里说明日志一致，没必要全删
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "rf.getNowLogIndex %v,%v", rf.getNowLogIndex(args.SnaplastIndex),
				rf.log[rf.getNowLogIndex(args.SnaplastIndex)])
			rf.log = append(make([]LogEntry, 0), rf.log[rf.getNowLogIndex(args.SnaplastIndex)+1:]...)
		} else {
			rf.log = make([]LogEntry, 0)
		}
	} else {
		rf.log = make([]LogEntry, 0)
	}
	rf.SnaplastIndex = args.SnaplastIndex
	rf.SnaplastTerm = args.SnaplastTerm
	rf.lastApplied = Max(rf.SnaplastIndex, rf.lastApplied)
	rf.commitIndex = Max(rf.SnaplastIndex, rf.commitIndex)
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "快照成功 %v,%v,%v", rf.SnaplastIndex, rf.SnaplastTerm, rf.lastApplied)
	reply.VoteGranted = 1
	rf.encodeRaftSnap(args.Data)
	msg := ApplyMsg{
		IsSnap: true,
	}
	rf.applyCh <- msg
}
func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) sendLogAppendEntries(serverNum1 int) {
	if serverNum1 == -1 {
		//发送日志
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送同步日志")
		for serverNum := 0; serverNum < len(rf.peers); serverNum++ {
			if serverNum == rf.me {
				continue
			}
			if rf.State != "Leader" {
				return
			}
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "next:%v,ser:%v,snap:%v", rf.nextIndex, serverNum, rf.SnaplastIndex)
			if (rf.nextIndex[serverNum] <= rf.SnaplastIndex && rf.SnaplastIndex != 0) || rf.IsSendSnap[serverNum] {
				rf.sendInstallSnapshot(serverNum)
				continue
			}
			appendEntries := rf.getAppendEntries(serverNum)
			//发送append
			go func(server int, args *AppendEntries) {
				rf.mu.Lock()
				DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送同步日志 %v ---> %v , %v", rf.me, server, args)
				rf.mu.Unlock()
				var reply RequestVoteReply
				flag := rf.sendAppendEntries(server, args, &reply)
				if flag {
					rf.handleAppendEntries(server, reply, *args)
				}
				//没有发送成功就再次发送
			}(serverNum, &appendEntries)
		}
	} else {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "重新发送日志%v ---> %v", rf.me, serverNum1)
		if rf.State != "Leader" {
			return
		}
		if (rf.nextIndex[serverNum1] <= rf.SnaplastIndex && rf.SnaplastIndex != 0) || rf.IsSendSnap[serverNum1] {
			rf.sendInstallSnapshot(serverNum1)
			return
		}
		appendEntries := rf.getAppendEntries(serverNum1)
		//发送append
		go func(server int, args *AppendEntries) {
			rf.mu.Lock()
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "发送同步日志 %v ---> %v , %v", rf.me, server, args)
			rf.mu.Unlock()
			var reply RequestVoteReply
			flag := rf.sendAppendEntries(server, args, &reply)
			if flag {
				rf.handleAppendEntries(server, reply, *args)
			}
			//没有发送成功就再次发送
			//DPrintf(len(rf.log),rf.me,rf.State,rf.currentTetm,"重新发送日志%v ---> %v", rf.me, server)
		}(serverNum1, &appendEntries)
	}

}
func (rf *Raft) getAppendEntries(serverNum1 int) AppendEntries {
	appendEntries := AppendEntries{
		Term:         rf.currentTetm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	appendEntries.PrevLogIndex = rf.nextIndex[serverNum1] - 1
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "测试 %v", rf.nextIndex)
	if rf.getNowLogIndex(appendEntries.PrevLogIndex) >= 0 {
		appendEntries.PrevLogTerm = rf.log[rf.getNowLogIndex(appendEntries.PrevLogIndex)].Term
	}
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "rf.getNowLogIndex(appendEntries.PrevLogIndex) %v rf.SnaplastTerm %v",
		rf.getNowLogIndex(appendEntries.PrevLogIndex), rf.SnaplastTerm)
	if rf.getNowLogIndex(appendEntries.PrevLogIndex) == -1 && rf.SnaplastIndex != 0 {
		appendEntries.PrevLogTerm = rf.SnaplastTerm
	}
	if rf.getNowLogIndex(rf.nextIndex[serverNum1]) < len(rf.log) {
		appendEntries.Entrys = rf.log[rf.getNowLogIndex(rf.nextIndex[serverNum1]):]
	}
	return appendEntries
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *RequestVoteReply) bool {
	//
	ok := rf.peers[server].Call("Raft.GetAppendEntries", args, reply)
	return ok
}
func (rf *Raft) handleAppendEntries(serverNum int, reply RequestVoteReply, args AppendEntries) {
	//获得返回值，发送成功和发送失败
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State != "Leader" {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "这个raft已经不是leader %v", rf.State)
		return
	}
	if reply.Term > rf.currentTetm {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "这个raft（leader）过时了，变成follower")
		//无论如何，这里都要变成follower
		rf.currentTetm = reply.Term
		rf.convertTo("Follower")
		rf.persist()
		return
	}
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "%v", reply.VoteGranted)
	//这里可能只是一个心跳
	if reply.VoteGranted == 1 {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "更新日志成功，判断是否进行提交,%v", rf.nextIndex[serverNum]-1)
		//更新成功
		if rf.nextIndex[serverNum] > args.PrevLogIndex+len(args.Entrys)+1 {
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "虽然成功，但是这个rpc是乱序的")
			return
		}
		rf.nextIndex[serverNum] = args.PrevLogIndex + len(args.Entrys) + 1
		rf.matchIndex[serverNum] = rf.nextIndex[serverNum] - 1
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "测试： %v,%v", rf.matchIndex, rf.nextIndex)
		if rf.nextIndex[serverNum] > rf.getLenLog() { //debug
			rf.nextIndex[serverNum] = rf.getLenLog()
			rf.matchIndex[serverNum] = rf.nextIndex[serverNum] - 1
		}
		//这里要提交日志
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "判断是否提交")
		curentIndex := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= rf.matchIndex[serverNum] {
				curentIndex++
			}
		}
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "判断是否提交 %v", curentIndex)
		if curentIndex >= len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[serverNum] &&
			rf.log[rf.getNowLogIndex(rf.matchIndex[serverNum])].Term == rf.currentTetm {
			//&& rf.log[rf.matchIndex[serverNum]].Term == rf.currentTetm
			rf.commitIndex = rf.matchIndex[serverNum]
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "提交日志，%v", rf.commitIndex)
			go rf.CommitLog()
		}
		return
	} else if reply.VoteGranted == 0 {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "日志不同步 reply %v nextindex %v %v", reply, rf.nextIndex, serverNum)
		if reply.XIndex > rf.nextIndex[serverNum] || reply.XIndex < rf.matchIndex[serverNum] {
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "rpc过时了")
			return
		}

		//重新发送,此处进行优化
		if reply.XTerm == -1 || rf.getNowLogIndex(reply.XIndex) < 0 {
			//此时说明是follower少槽位
			rf.nextIndex[serverNum] = reply.XIndex
		} else {
			//找到这个任期对应的
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "找到任期对应的槽位 %v", reply.XTerm)
			index := reply.XIndex //80
			for ; index < args.PrevLogIndex; index++ {
				if rf.log[rf.getNowLogIndex(index)].Term == reply.Term { //32
					break
				}
			}
			if index == args.PrevLogIndex {
				rf.nextIndex[serverNum] = reply.XIndex
			} else {
				rf.nextIndex[serverNum] = index
			}
		}
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "日志不同步，更新nextindex后继续发送，%v ,%v", rf.nextIndex[serverNum], serverNum)
		rf.sendLogAppendEntries(serverNum)
	}
}
func (rf *Raft) CommitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.getLenLog() {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "来的commit不对")
		return
	}
	//DPrintf(len(rf.log),rf.me,rf.State,rf.currentTetm,"日志 %v",rf.log)
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "日志提交, lastApplied %v , commitIndex %v", rf.lastApplied, rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ { //commit日志到与Leader相同
		// 很重要的是要index要加1 因为计算的过程start返回的下标不是以0开始的
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "日志提交, %v %v", i, rf.log[rf.getNowLogIndex(i)])
		rf.applyCh <- ApplyMsg{
			CommandIndex: i + 1,
			Command:      rf.log[rf.getNowLogIndex(i)].Command,
			CommandValid: true,
		}
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "日志提交完成, %v %v", i, rf.log[rf.getNowLogIndex(i)])
	}
	rf.lastApplied = rf.commitIndex
}

/**
1.首先可能是一个空包心跳，只需要进行更新状态就好
2.是日志信息
	2.1 首先判断携带的任期比较，假如来的任期比较晚，说明这个leader已经过期了，返回失败
	2.2 然后任期相同或者小，说明这个leader有效，可以进行日志更新，携带index和term，读取follower index处的term，进行比较
		2.2.1 如果follower这个位置的term == 来的term，说明没有问题，进行更新
		2.2.2 如果follower这个位置的term <> 来的term,说明有问题,不更新
*/
func (rf *Raft) GetAppendEntries(args *AppendEntries, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//现有条目冲突，就不添加，不冲突就添加
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到日志信息 %v", args)
	reply.Term = rf.currentTetm
	if rf.getNowLogIndex(args.PrevLogIndex) < -1 {
		reply.VoteGranted = 0
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "快照后收到日志信息,但是在快照前发送的")
		return
	}
	if rf.currentTetm > args.Term {
		reply.VoteGranted = 0
		//说明这个leader无效,好像什么都不用干,因为下面会变
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到日志信息,leader 无效")
		return
	}
	if rf.getNowLogIndex(args.PrevLogIndex) >= 0 &&
		(rf.getLenLog()-1 < args.PrevLogIndex || rf.log[rf.getNowLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm) {
		rf.electionTimer.Reset(rf.getTimeOut())
		rf.convertTo("Follower")
		//这里进行优化，主要是通过XTerm，XIndex和Xlen进行优化，
		//Xterm ，follower中与leader冲突的log对应的任期
		//xindex，对应人气好为xterm的第一条log条目的槽位号
		reply.VoteGranted = 0
		//每次都删除
		if rf.getLenLog()-1 < args.PrevLogIndex {
			//日志空白
			reply.XTerm = -1
			reply.XIndex = rf.getLenLog()
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到日志信息,日志不对应 len(rf.log)%v", len(rf.log))
		} else {
			reply.XTerm = rf.log[rf.getNowLogIndex(args.PrevLogIndex)].Term
			index := rf.getNowLogIndex(args.PrevLogIndex)
			for {
				if index == 0 || rf.log[index-1].Term != reply.XTerm {
					break
				}
				index--
			}
			reply.XIndex = rf.getAbsolutionLog(index)
			DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到日志信息,日志不对应两个Term %v %v", rf.log[rf.getNowLogIndex(args.PrevLogIndex)].Term, args.PrevLogTerm)
		}
		return
	}
	if args.Entrys == nil {
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到心跳 ")
		if args.Term >= rf.currentTetm {
			//rf.electionTimer.Stop()
			rf.electionTimer.Reset(rf.getTimeOut())
			if rf.commitIndex < args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
			}
			reply.VoteGranted = 2
			rf.currentTetm = args.Term
			rf.votedFor = args.LeaderId
			rf.convertTo("Follower")
			go rf.CommitLog()
			rf.persist()
		}
	} else {
		rf.electionTimer.Reset(rf.getTimeOut())
		rf.convertTo("Follower")
		rf.votedFor = args.LeaderId
		rf.currentTetm = args.Term
		//同步日志
		logCopy := rf.log[:rf.getNowLogIndex(args.PrevLogIndex+1)]
		//恶心的很
		logCopy2 := make([]LogEntry, len(args.Entrys))
		copy(logCopy2, args.Entrys)
		logCopy = append(logCopy, logCopy2...)
		rf.log = make([]LogEntry, len(logCopy))
		copy(rf.log, logCopy)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
		reply.VoteGranted = 1
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "收到日志信息,同步成功")
		go rf.CommitLog() // 堵塞
		rf.persist()
	}

}

func (rf *Raft) convertTo(state string) {
	//此处进行状态转移
	if state == rf.State {
		return
	}
	rf.State = state
	switch state {
	case "Leader":
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "转移为Leader")
		rf.electionTimer.Stop()
		rf.sendLogAppendEntries(-1)
		rf.heaterTimer.Reset(HeartbeatInterval)
	case "Follower":
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "转移为follower")
		rf.heaterTimer.Stop()
		rf.electionTimer.Reset(rf.getTimeOut())
		rf.votedFor = -1
	case "Candidate":
		DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "转移为Candidate")
		rf.getElection()
	}
}

/*
快照
*/
func (rf *Raft) TakeRaftSnapShot(applyRaftLogIndex int, byte2 []byte) {
	//要进行的工作主要是更新各种参数，然后进行更改
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(len(rf.log), rf.me, rf.State, rf.currentTetm, "raft kuaizhao")
	index := rf.getNowLogIndex(applyRaftLogIndex)
	//index := applyRaftLogIndex - rf.SnaplastIndex
	if applyRaftLogIndex <= rf.SnaplastIndex {
		return
	}
	rf.SnaplastTerm = rf.log[rf.getNowLogIndex(applyRaftLogIndex)].Term
	rf.SnaplastIndex = applyRaftLogIndex
	rf.log = append(make([]LogEntry, 0), rf.log[index+1:]...)
	rf.lastApplied = Max(rf.SnaplastIndex, rf.lastApplied)
	//rf.commitIndex = Max(rf.SnaplastIndex, rf.commitIndex)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		rf.IsSendSnap[i] = true
	}
	rf.encodeRaftSnap(byte2)
}

/*
快照持久化，不和其他持久化一起的原因是防止多次持久化
*/
func (rf *Raft) encodeRaftSnap(byte2 []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTetm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.SnaplastTerm)
	e.Encode(rf.SnaplastIndex)
	rf.Persister.SaveStateAndSnapshot(w.Bytes(), byte2)
}

func (rf *Raft) HasCurrentTermInLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (len(rf.log) - 1) > 0 {
		return rf.log[len(rf.log)-1].Term == rf.currentTetm
	}
	return true
}
