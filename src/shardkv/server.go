package shardkv

// import "6.824lab/shardmaster"
import (
	"6.824lab/labrpc"
	"6.824lab/shardmaster"
	"time"
)
import "6.824lab/raft"
import "sync"
import "6.824lab/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	shareId   int
	configNum int
	Data      interface{}

	//get put用
	Key       string
	Value     string
	OpType    string
	ClientId  int64
	RequestId int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cfg         *shardmaster.Config
	mck         *shardmaster.Clerk
	shardManger map[int]string // OK PULL NULL
	gcManager   map[int]bool
	//toSendShard map[int]map[int]map[string]string
	db        map[int]map[string]string // 数据
	lastReply map[int]map[int64]int     // 防止重复执行
	chMap     map[int]chan Op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	command := Op{
		shareId:   args.ShareId,
		Key:       args.Key,
		OpType:    "GET",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if !kv.isHave(args.ConfigNum, args.ShareId) {
		reply.Err = ErrWrongGroup
		return
	}
	//是leader，所以需要进行请求处理,判断是不是重复请求
	kv.mu.Lock()
	lastReply, ok := kv.lastReply[args.ShareId][args.ClientId]
	if ok && lastReply > args.RequestId {
		reply.Value = kv.db[args.ShareId][args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//从raft进行操作
	isok, appliOp := kv.getCh(command)
	if !isok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Value = appliOp.Value
}

func (kv *ShardKV) getCh(op Op) (bool, Op) {

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, op
	}
	kv.mu.Lock()
	opCh, ok := kv.chMap[index]
	if !ok {
		opCh = make(chan Op, 1)
		kv.chMap[index] = opCh
	}
	kv.mu.Unlock()
	//监听消息
	select {
	case ap := <-opCh:
		return op.ClientId == ap.ClientId && op.RequestId == ap.RequestId, ap
	case <-time.After(600 * time.Millisecond):
		return false, op
	}
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.isHave(args.ConfigNum, args.ShareId) {
		reply.Err = ErrWrongGroup
		return
	}
	commod := Op{
		shareId:   args.ShareId,
		Key:       args.Key,
		Value:     args.Value,
		OpType:    args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, _ := kv.getCh(commod)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}
func (kv *ShardKV) isHave(configNum int, shareId int) bool {
	states, ok := kv.shardManger[shareId]
	if ok {
		return states == "CanUse" && configNum == kv.cfg.Num
	}
	return false
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

//拉取pull的代码,使用一个go进行轮训，然后还要考虑到leader的下台
//主要包括与clerk拉取配置
//与其他的集群拉取share
/**
拉取clerk的配置
*/
func (kv *ShardKV) pullCfg() {
	kv.mu.Lock()
	if kv.isAllPull() {
		//拉取配置
		currentCfgNum := kv.cfg.Num
		kv.mu.Unlock()
		nextCfg := kv.mck.Query(currentCfgNum + 1)

		if nextCfg.Num == currentCfgNum+1 {
			//拉取配置，然后更新
			kv.rf.Start(Op{
				OpType: "PullMsg",
				Data:   &nextCfg,
			})
		}
	} else {
		kv.mu.Unlock()
	}
}
func (kv *ShardKV) isAllPull() bool {
	ready := true
	for _, status := range kv.shardManger {
		if status != "CanUser" {
			ready = false
			break
		}
	}
	return ready
}
func (kv *ShardKV) updateCfg(newCfg *shardmaster.Config) CommandResponse {
	if newCfg.Num != kv.cfg.Num+1 {
		return CommandResponse{"ErrOutDated"}
	}
	//更新
	for shard, gid := range newCfg.Shards {
		_, ok := kv.shardManger[shard]
		if ok && gid != kv.gid {
			//有，但是gid和本地集群不同，说明这个块不应该在本地，那么把他加入到gc中
			delete(kv.shardManger, shard)
			kv.gcManager[shard] = true
		}
		if !ok && gid == kv.gid {
			//没有，但是gid相同,要么是第一次，要么要pull
			if kv.cfg.Num == 0 {
				kv.shardManger[shard] = "CanUse"
			} else {
				kv.shardManger[shard] = "NeedPull"
			}
			if _, ok := kv.db[shard]; !ok {
				kv.db[shard] = make(map[string]string)
			}

		}
	}
	kv.cfg = newCfg
	return CommandResponse{"OK"}
}

/**
从Raft获取通知的go
*/
func (kv *ShardKV) waitCommit() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.IsSnap {
				//kv.installSnapshot(msg.SnapShot)
				continue
			}
			op := msg.Command.(Op)
			if op.OpType == "PullMsg" {
				kv.updateCfg(op.Data.(*shardmaster.Config))
				continue
			}
			kv.mu.Lock()
			//kv.applyRaftLogIndex = msg.CommandIndex
			requedtId, ok := kv.lastReply[op.shareId][op.ClientId]
			if op.OpType == "GET" {
				op.Value = kv.db[op.shareId][op.Key]
			} else {
				if !ok || op.RequestId > requedtId {
					switch op.OpType {
					case "Put":
						kv.db[op.shareId][op.Key] = op.Value
					case "Append":
						kv.db[op.shareId][op.Key] += op.Value
					}
					kv.lastReply[op.shareId][op.ClientId] = op.RequestId
				}
			}
			opCh, Ok := kv.chMap[msg.CommandIndex]
			if !Ok {
				opCh = make(chan Op, 1)
				kv.chMap[msg.CommandIndex] = opCh
			}
			opCh <- op
			kv.mu.Unlock()
		}
	}
}

//开始从其他集群pull shard
/**
接受pull的方法
*/
func (kv *ShardKV) getPull(args *ConfigMsgArgs, reply *ConfigMsgReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Shards = args.Shards
	reply.ConfigNum = kv.cfg.Num

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > kv.cfg.Num {
		reply.Err = ErrRequest
		return
	}
	for _, shard := range args.Shards {
		if _, ok := kv.shardManger[shard]; !ok {
			reply.Err = ErrNoKey
			return
		}
		reply.DB[shard], reply.Cid2Seq[shard] = kv.getDeepCopy(shard)
	}
	reply.Err = OK
}
func (kv *ShardKV) getDeepCopy(shard int) (map[string]string, map[int64]int) {
	ddb := make(map[string]string)
	replt := make(map[int64]int)
	for key, value := range kv.db[shard] {
		ddb[key] = value
	}
	for key, value := range kv.lastReply[shard] {
		replt[key] = value
	}
	return ddb, replt
}

/**
发送pull的方法
*/
func (kv *ShardKV) pullShard() {
	kv.mu.Lock()
	GidShard := kv.getGidShards()
	for gid, shard := range GidShard {
		go kv.requestShardData(gid, shard)
	}
	kv.mu.Unlock()
}
func (kv *ShardKV) getGidShards() map[int][]int {
	GidShard := make(map[int][]int)
	for key, value := range kv.shardManger {
		if value == "NeedPull" {
			if _, ok := GidShard[kv.cfg.Shards[key]]; !ok {
				GidShard[kv.cfg.Shards[key]] = []int{key}
			} else {
				GidShard[kv.cfg.Shards[key]] = append(GidShard[kv.cfg.Shards[key]], key)
			}
		}
	}
	return GidShard
}
func (kv *ShardKV) requestShardData(gid int, shardIds []int) {
	//获取shardid相关group
	group := kv.cfg.Groups[gid]
	args := ConfigMsgArgs{
		Shards:    shardIds,
		ConfigNum: kv.cfg.Num,
	}
	for _, value := range group {
		srv := kv.make_end(value)
		var Reply ConfigMsgReply
		ok := srv.Call("ShardKV.getPull", &args, &Reply)
		if ok && Reply.Err == OK {
			//成功
			kv.mu.Lock()
			if Reply.ConfigNum < kv.cfg.Num {
				kv.mu.Unlock()
				return
			}
			for _, shardIds := range Reply.Shards {
				if status, ok := kv.shardManger[shardIds]; !ok {
					kv.mu.Unlock()
					return
				} else if status != "NeedPull" {
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
			kv.rf.Start(Op{
				OpType: "UpdateShares",
				Data:   Reply,
			})
			return
		}
	}
}
func (kv *ShardKV) updateShare() {

}
