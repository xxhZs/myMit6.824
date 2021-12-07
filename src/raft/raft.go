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
	persister *Persister          // Object to hold this peer's persisted state
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
	heater     bool
}

//日志信息
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTetm
	isleader = rf.State == "Leader"
	rf.DPrintf("获取当前的状态，当前的任期：%v状态：%v id %v", term, rf.State, rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//选举结构
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//选举返回结果

	Term        int
	VoteGranted bool
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
	rf.DPrintf("收到上面的投票信息 %v,%v", args, reply)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteGranted := true
	//日志判断
	if len(rf.log) > 0 {
		if (rf.log[len(rf.log)-1].Term > args.LastLogIndex) ||
			((rf.log[len(rf.log)-1].Term == args.LastLogIndex) && len(rf.log)-1 > args.LastLogIndex) {
			rf.DPrintf("通过日志比较，当前的候选人 %v 有效", args.CandidateID)
			voteGranted = false
		}
	}
	if args.Term < rf.currentTetm {
		rf.DPrintf("来的任期 %v 太久远了，不给你投票", args.CandidateID)
		reply.Term = rf.currentTetm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTetm {

		rf.State = "Follower"
		rf.currentTetm = args.Term
		rf.votedFor = -1
		if voteGranted {
			rf.DPrintf("来的选举有效:%v，已经变成follow:%v，给你投票", args.CandidateID, rf.me)
			rf.votedFor = args.CandidateID
		} else {
			rf.DPrintf("来的选举有效%v，已经变成follow%v，但是日志无效，不给投票", args.CandidateID, rf.me)
		}
		rf.GetTimeout()
		reply.Term = args.Term
		reply.VoteGranted = voteGranted
		return
	}
	if args.Term == rf.currentTetm {
		if rf.votedFor == -1 && voteGranted {
			rf.DPrintf("来的选举有效%v，给投票%v", args.CandidateID, rf.me)
			rf.votedFor = args.CandidateID
		} else {
			rf.DPrintf("来的选举有效%v，不给投票%v", args.CandidateID, rf.me)
		}
		reply.Term = rf.currentTetm
		reply.VoteGranted = (rf.votedFor == args.CandidateID)
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
	isLeader := true

	// Your code here (2B).

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
	rf.persister = persister
	rf.me = me
	rf.DPrintf("初始化raft")
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTetm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.DPrintf("peers有 %v", len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	//自己添加的必须有的结构
	rf.State = "Follower"

	//此处进行超时等待调用 2
	rf.GetTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//超时等待的方法
func (rf *Raft) GetTimeout() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.DPrintf("follower：进入超时等待")
	if rf.timer != nil {
		rf.timer.Stop()
	}
	rf.timer = time.AfterFunc(rf.getTimeOut(), func() {
		rf.TimeoutVote()
	})
}
func (rf *Raft) getTimeOut() time.Duration {
	timerD := time.Millisecond * time.Duration(200+rand.Intn(200))
	rf.DPrintf("TimeOut %v", timerD)
	return timerD
}
func (rf *Raft) TimeoutVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("超时了，进行选举机制")
	if rf.State != "Leader" {
		//只有不是leader才会触发选举
		rf.State = "Candidate"
		rf.currentTetm += 1
		rf.votedFor = rf.me
		rf.votesCount = 1
		//rf.isVote = true
		args := RequestVoteArgs{
			Term:         rf.currentTetm,
			CandidateID:  rf.me,
			LastLogIndex: len(rf.log) - 1,
		}
		if len(rf.log) > 0 {
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
		}
		//此处进行发送选举
		for serverNum := 0; serverNum < len(rf.peers); serverNum++ {
			if serverNum == rf.me {
				continue
			}
			//并发发送，快速
			rf.DPrintf("发送选举信息%v-->%v", rf.me, serverNum)
			//发送rpc选举信息
			go func(server int, args RequestVoteArgs) {
				var reply RequestVoteReply
				flag := rf.sendRequestVote(server, args, &reply)
				if flag {
					//处理结果，此处要进行各种操作
					rf.mu.Lock()
					rf.setReplyVote(reply)
					rf.mu.Unlock()
				}
			}(serverNum, args)
		}
		//1
	}
	rf.GetTimeout()
}

/*
 此处的逻辑是，看投票结果和term
1. 返回的term < currentTerm 这个回应超时了
2. 返回的term > currentTerm 变为follower
3. 投票有效
*/
func (rf *Raft) setReplyVote(reply RequestVoteReply) {
	rf.DPrintf("获取选举信息")
	if reply.Term < rf.currentTetm {
		//说明此时可能是超时的follwer
		rf.DPrintf("返回的选举已经超期")
		return
	}
	if reply.Term > rf.currentTetm {
		rf.DPrintf("返回的任期比本地长，所以本机变成follower: %v", rf.me)
		//变成fllower
		rf.State = "Follower"
		rf.currentTetm = reply.Term
		rf.votedFor = -1
		rf.GetTimeout()
		return
	}
	// 条件满足
	if rf.State == "Candidate" && reply.VoteGranted {
		rf.DPrintf("返回投票成功")
		rf.votesCount += 1
		if rf.votesCount >= len(rf.peers)/2+1 {
			rf.DPrintf("升级为leader，更新日志%v", rf.me)
			//被选举为leader,这个时候就要重置两个日志文件，进行发送了
			rf.State = "Leader"
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				//DPrintf("nextindex的长度 %v",len(rf.nextIndex))
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1
			}
			//1
			rf.GetTimeout()
		}
		return
	}
}
