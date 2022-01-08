package shardmaster

import (
	"6.824lab/raft"
	"fmt"
	"math"
	"time"
)
import "6.824lab/labrpc"
import "sync"
import "6.824lab/labgob"

const Debug = 0

func DPrintf(format string, kvId int, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		format = "[shardMaster peer %v] " + format
		a = append([]interface{}{kvId}, a...)
		fmt.Printf(format+"\n", a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config // indexed by config num
	lastReply map[int64]int
	chMap     map[int]chan Op
}
type Op struct {
	// Your data here.
	OpType    string
	Args      interface{}
	Cid       int64
	RequestID int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("join加入%v", sm.me, args)
	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}
	op := Op{OpType: "Join", Cid: args.Cid, Args: *args, RequestID: args.RequestId}
	reply.WrongLeader = !sm.getCh(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("leave加入%v", sm.me, args)
	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}
	op := Op{OpType: "Leave", Cid: args.Cid, Args: *args, RequestID: args.RequestId}
	reply.WrongLeader = !sm.getCh(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("move加入%v", sm.me, args)
	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}
	op := Op{OpType: "Move", Cid: args.Cid, Args: *args, RequestID: args.RequestId}
	reply.WrongLeader = !sm.getCh(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("query加入%v", sm.me, args)
	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	sm.mu.Lock()
	lastReply, ok := sm.lastReply[args.Cid]
	if ok && lastReply > args.RequestId {
		DPrintf("query来过了", sm.me, args)
		reply.Config = sm.configs[args.Num]
		reply.Err = OK
		return
	}
	sm.mu.Unlock()
	op := Op{Cid: args.Cid, Args: *args, OpType: "Query", RequestID: args.RequestId}
	isok := sm.getCh(op)
	if !isok {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	DPrintf("query返回config", sm.me)
	if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
	} else {
		reply.Config = sm.configs[len(sm.configs)-1]
	}
	sm.mu.Unlock()
}
func (sm *ShardMaster) getCh(op Op) bool {
	DPrintf("getCh运行 %v", sm.me, op)
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}
	sm.mu.Lock()
	opCh, ok := sm.chMap[index]
	if !ok {
		DPrintf("没有chmap", sm.me)
		opCh = make(chan Op, 1)
		sm.chMap[index] = opCh
	}
	sm.mu.Unlock()
	select {
	case ap := <-opCh:
		DPrintf("返回是否成功提交", sm.me)
		return op.Cid == ap.Cid && op.RequestID == ap.RequestID
	case <-time.After(600 * time.Millisecond):
		return false
	}
}
func (sm *ShardMaster) isLeader() bool {
	_, leader := sm.rf.GetState()
	return leader
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.chMap = make(map[int]chan Op)
	sm.lastReply = make(map[int64]int)
	go sm.waitCommit()
	// Your code here.

	return sm
}
func (sm *ShardMaster) waitCommit() {
	DPrintf("循环获取raft的提交", sm.me)
	for {
		select {
		case msg := <-sm.applyCh:
			op := msg.Command.(Op)
			sm.mu.Lock()
			DPrintf("收到op %v", sm.me, op)
			requedtId, ok := sm.lastReply[op.Cid]
			DPrintf("%v,%v", sm.me, requedtId, ok)
			sm.mu.Unlock()
			if (!ok || op.RequestID > requedtId) && op.OpType != "Query" {
				DPrintf("更改config", sm.me)
				sm.updateConfig(op)
			}
			sm.mu.Lock()
			opCh, Ok := sm.chMap[msg.CommandIndex]
			if !Ok {
				opCh = make(chan Op, 1)
				sm.chMap[msg.CommandIndex] = opCh
			}
			opCh <- op
			sm.mu.Unlock()
		}
	}
}
func (sm *ShardMaster) updateConfig(op Op) {
	cfg := sm.crateNextconfig()
	switch op.OpType {
	case "Join":
		DPrintf("join执行结束", sm.me)
		joinArgs := op.Args.(JoinArgs)
		for gid, i := range joinArgs.Servers {
			newServer := make([]string, len(i))
			copy(newServer, i)
			cfg.Groups[gid] = newServer
			sm.balanceGroup(&cfg, op.OpType, gid)
		}
	case "Leave":
		DPrintf("leave执行结束", sm.me)
		leaveArg := op.Args.(LeaveArgs)
		for _, gid := range leaveArg.GIDs {
			delete(cfg.Groups, gid)
			sm.balanceGroup(&cfg, op.OpType, gid)
		}
	case "Move":
		DPrintf("move执行结束", sm.me)
		moveArg := op.Args.(MoveArgs)
		if _, arg := cfg.Groups[moveArg.GID]; arg {
			cfg.Shards[moveArg.Shard] = moveArg.GID
		} else {
			return
		}
	}
	sm.configs = append(sm.configs, cfg)
}
func (sm *ShardMaster) balanceGroup(cfg *Config, types string, gid int) {
	//group对应的share
	DPrintf("负载均衡 %v", sm.me, cfg)
	shardCount := sm.groupById(cfg)
	switch types {
	case "Join":
		avg := NShards / len(cfg.Groups)
		DPrintf("负载均衡后join %v", sm.me, cfg)
		for i := 0; i < avg; i++ {
			//把最大的块的gid的share给最新的gid
			maxGid := sm.getMaxShardGid(shardCount)
			cfg.Shards[shardCount[maxGid][0]] = gid
			shardCount[maxGid] = shardCount[maxGid][1:]
		}
	case "Leave":
		DPrintf("负载均衡后leave %v", sm.me, cfg)
		shareList, ok := shardCount[gid]
		if !ok {
			return
		}
		delete(shardCount, gid)
		if len(cfg.Groups) == 0 {
			cfg.Shards = [NShards]int{}
			return
		}
		for _, v := range shareList {
			minGid := sm.getMinShardGid(shardCount)
			cfg.Shards[v] = minGid
			shardCount[minGid] = append(shardCount[minGid], v)
		}
	}

}
func (sm *ShardMaster) crateNextconfig() Config {
	DPrintf("获取下一个config", sm.me)
	lastCfg := sm.configs[len(sm.configs)-1]
	nextCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: make(map[int][]string)}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append([]string{}, servers...)
	}
	return nextCfg
}

/**
这里是把gid的所有share放到一个map里
*/
func (sm *ShardMaster) groupById(cfg *Config) map[int][]int {
	shardCount := map[int][]int{}
	for k, _ := range cfg.Groups {
		shardCount[k] = []int{}
	}
	for k, v := range cfg.Shards {
		shardCount[v] = append(shardCount[v], k)
	}
	return shardCount
}
func (sm *ShardMaster) getMaxShardGid(shardCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardCount {
		if max < len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}
func (sm *ShardMaster) getMinShardGid(shardCount map[int][]int) int {
	min := math.MaxInt32
	var gid int
	for k, v := range shardCount {
		if min > len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}
