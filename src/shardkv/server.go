package shardkv

// import "6.824lab/shardmaster"
import (
	"6.824lab/labrpc"
	"6.824lab/shardmaster"
	"bytes"
	"fmt"
	"time"
)
import "6.824lab/raft"
import "sync"
import "6.824lab/labgob"

func DPrintf(format string, gid int, id int, a ...interface{}) (n int, err error) {
	Debug := 1
	if Debug > 0 {
		format = "[shardkv group %v peer %v] " + format
		a = append([]interface{}{gid, id}, a...)
		fmt.Printf(format+"\n", a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ShareId int
	Data    interface{}

	//get put用
	Key       string
	Value     string
	OpType    string
	ClientId  int64
	RequestId int
}

type ShardKV struct {
	Persister    *raft.Persister
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cfg         shardmaster.Config
	lastCfg     shardmaster.Config
	mck         *shardmaster.Clerk
	shardManger map[int]string // OK PULL NULL
	gcManager   map[int]bool
	//toSendShard map[int]map[int]map[string]string
	db                map[int]map[string]string // 数据
	lastReply         map[int]map[int64]int     // 防止重复执行
	chMap             map[int]chan Op
	killCh            chan int
	applyRaftLogIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("进行get，args：%v", kv.gid, kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("get不是leader", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("构建command", kv.gid, kv.me)
	command := Op{
		ShareId:   args.ShareId,
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
		DPrintf("已经get成功，无需raft就可以返回,value %v", kv.gid, kv.me, reply)
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
	DPrintf("get成功，value %v", kv.gid, kv.me, reply)
}

func (kv *ShardKV) getCh(op Op) (bool, Op) {
	DPrintf("getch", kv.gid, kv.me)
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("不是leader了%v", kv.gid, kv.me)
		return false, op
	}
	kv.mu.Lock()
	opCh, ok := kv.chMap[index]
	if !ok {
		DPrintf("没有chMap", kv.gid, kv.me)
		opCh = make(chan Op, 1)
		kv.chMap[index] = opCh
	}
	kv.mu.Unlock()
	//监听消息
	select {
	case ap := <-opCh:
		DPrintf("监听到op，raft已经返回", kv.gid, kv.me)
		return op.ClientId == ap.ClientId && op.RequestId == ap.RequestId, ap
	case <-time.After(600 * time.Millisecond):
		DPrintf("getch超时", kv.gid, kv.me)
		return false, op
	}
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("putAppend执行,args:%v", kv.gid, kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("putAppend不是leade", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.isHave(args.ConfigNum, args.ShareId) {
		reply.Err = ErrWrongGroup
		return
	}
	commod := Op{
		ShareId:   args.ShareId,
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
	DPrintf("putAppend成功", kv.gid, kv.me)
}
func (kv *ShardKV) isHave(configNum int, shareId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	states, ok := kv.shardManger[shareId]
	if ok {
		DPrintf("ishave，%v,%v==%v?", kv.gid, kv.me, states, configNum, kv.cfg.Num)
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
	close(kv.killCh)
	//close(kv.applyCh)
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
	labgob.Register(shardmaster.Config{})
	labgob.Register(ConfigMsgReply{})
	labgob.Register(CommandResponse{})
	labgob.Register(ConfigMsgArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	//kv.cfg.Num = 0
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.shardManger = make(map[int]string)
	kv.gcManager = make(map[int]bool)
	kv.db = make(map[int]map[string]string)    // 数据
	kv.lastReply = make(map[int]map[int64]int) // 防止重复执行
	kv.chMap = make(map[int]chan Op)
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan int)

	kv.Persister = persister
	snap := kv.Persister.ReadSnapshot()
	if snap != nil && len(snap) > 0 {
		kv.installSnapshot(snap)
	}

	go kv.waitCommit()
	go kv.daemon(kv.pullCfg, 100)
	go kv.daemon(kv.pullShard, 100)
	go kv.daemon(kv.pushGC, 100)
	go kv.snapshotMonitor()
	go kv.daemon(kv.checkLeaderNewestLog, 100)
	return kv
}

func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		select {
		case <-kv.killCh:
			DPrintf("func结束", kv.gid, kv.me)
			return
		default:
			do()
			time.Sleep(time.Duration(sleepMS) * time.Millisecond)
		}
	}
}

//拉取pull的代码,使用一个go进行轮训，然后还要考虑到leader的下台
//主要包括与clerk拉取配置
//与其他的集群拉取share
/**
拉取clerk的配置
*/
func (kv *ShardKV) pullCfg() {
	kv.mu.Lock()
	DPrintf("拉取clerk的cfg配置", kv.gid, kv.me)
	if kv.isAllPull() {
		//拉取配置
		currentCfgNum := kv.cfg.Num
		kv.mu.Unlock()
		nextCfg := kv.mck.Query(currentCfgNum + 1)
		DPrintf("拉取的是否有效 cfg%v,%v,有效就到raft", kv.gid, kv.me, nextCfg, currentCfgNum)
		if nextCfg.Num == currentCfgNum+1 {
			//拉取配置，然后更新
			DPrintf("拉取有效", kv.gid, kv.me, nextCfg, currentCfgNum)
			kv.rf.Start(Op{
				OpType: "PullMsg",
				Data:   nextCfg,
			})
		}
	} else {
		kv.mu.Unlock()
	}
}
func (kv *ShardKV) isAllPull() bool {
	ready := true
	for _, status := range kv.shardManger {
		if status != "CanUse" {
			ready = false
			break
		}
	}
	DPrintf("是否全部canuse，之后可以拉取cfg %v %v", kv.gid, kv.me, ready, kv.shardManger)
	return ready
}
func (kv *ShardKV) updateCfg(newCfg shardmaster.Config) CommandResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("修改cfg 序号为%v , %v", kv.gid, kv.me, kv.cfg.Num, newCfg)
	if newCfg.Num != kv.cfg.Num+1 {
		DPrintf("不进行同步cfg", kv.gid, kv.me)
		return CommandResponse{"ErrOutDated"}
	}
	//更新
	for shard, gid := range newCfg.Shards {
		_, ok := kv.shardManger[shard]
		if ok && gid != kv.gid {
			//有，但是gid和本地集群不同，说明这个块不应该在本地，那么把他加入到gc中
			DPrintf("gid与本地不同，这个块不在改集群，加入gc ，shard%v", kv.gid, kv.me, shard)
			delete(kv.shardManger, shard)
			kv.gcManager[shard] = true
		}
		if !ok && gid == kv.gid {
			//没有，但是gid相同,要么是第一次，要么要pull
			DPrintf("进行更改，shard%v", kv.gid, kv.me, shard)
			if kv.cfg.Num == 0 {
				kv.shardManger[shard] = "CanUse"
			} else {
				kv.shardManger[shard] = "NeedPull"
			}
			if _, ok := kv.db[shard]; !ok {
				kv.db[shard] = make(map[string]string)
			}
			if _, ok := kv.lastReply[shard]; !ok {
				kv.lastReply[shard] = make(map[int64]int)
			}
			DPrintf("更改完成，shard%v shardManger %v", kv.gid, kv.me, shard, kv.shardManger)
			delete(kv.gcManager, shard)

		}
	}
	DPrintf("更新cfg，cfg的序号%v", kv.gid, kv.me, newCfg.Num)
	kv.lastCfg = kv.cfg
	kv.cfg = newCfg
	return CommandResponse{"OK"}
}

/**
从Raft获取通知的go
*/
func (kv *ShardKV) waitCommit() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.IsSnap {
				kv.installSnapshot(msg.SnapShot)
				continue
			}
			op := msg.Command.(Op)
			DPrintf("op%v", kv.gid, kv.me, op)
			kv.mu.Lock()
			//提交的index
			kv.applyRaftLogIndex = msg.CommandIndex
			kv.mu.Unlock()
			if op.OpType == "PullMsg" {
				DPrintf("收到pullmsg %v", kv.gid, kv.me, op)
				kv.updateCfg(op.Data.(shardmaster.Config))
				continue
			}
			op = msg.Command.(Op)
			if op.OpType == "UpdateShares" {
				DPrintf("收到UpdateShares %v", kv.gid, kv.me, op)
				kv.updateShare(op.Data.(ConfigMsgReply))
				continue
			}
			op = msg.Command.(Op)
			if op.OpType == "UpdateGc" {
				DPrintf("收到UpdateGc %v", kv.gid, kv.me, op)
				kv.deleteGCShard(op.Data.(ConfigMsgReply))
				continue
			}
			if op.OpType == "EmptyEntry" {
				continue
			}
			kv.mu.Lock()
			cid, ok := kv.lastReply[op.ShareId]
			requedtId, ok1 := cid[op.ClientId]
			DPrintf("last %v", kv.gid, kv.me, kv.lastReply)
			DPrintf("requedtId%v, ok%v , ok1%v", kv.gid, kv.me, requedtId, ok, ok1)
			if op.OpType == "GET" {
				DPrintf("收到Get %v", kv.gid, kv.me, kv.db)
				op.Value = kv.db[op.ShareId][op.Key]
				//clientId := make(map[int64]int)
				//clientId[op.ClientId] = op.RequestId
				//kv.lastReply[op.ShareId] = clientId
			} else {
				DPrintf("收到PutAppend", kv.gid, kv.me)
				if !ok1 || !ok || op.RequestId > requedtId {
					switch op.OpType {
					case "Put":
						DPrintf("收到Put", kv.gid, kv.me)
						kv.db[op.ShareId][op.Key] = op.Value
					case "Append":
						DPrintf("收到Append", kv.gid, kv.me)
						kv.db[op.ShareId][op.Key] += op.Value
					}
					clientId := make(map[int64]int)
					clientId[op.ClientId] = op.RequestId
					kv.lastReply[op.ShareId] = clientId
					DPrintf("last %v", kv.gid, kv.me, kv.lastReply)
				}
			}
			opCh, Ok := kv.chMap[msg.CommandIndex]
			if !Ok {
				opCh = make(chan Op, 1)
				kv.chMap[msg.CommandIndex] = opCh
			}
			DPrintf("opCh", kv.gid, kv.me)
			opCh <- op
			kv.mu.Unlock()
		}
	}
}

//开始从其他集群pull shard
/**
接受pull的方法
*/
func (kv *ShardKV) GetPull(args *ConfigMsgArgs, reply *ConfigMsgReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("收到pull shard命令", kv.gid, kv.me)
	reply.Shards = args.Shards
	reply.ConfigNum = kv.cfg.Num

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("我不是leader", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > kv.cfg.Num {
		DPrintf("cfg序号已经比我本地的新了", kv.gid, kv.me)
		reply.Err = ErrRequest
		return
	}
	reply.DB = make(map[int]map[string]string, len(args.Shards))
	reply.Cid2Seq = make(map[int]map[int64]int, len(args.Shards))
	for _, shard := range args.Shards {
		if _, ok := kv.db[shard]; !ok {
			DPrintf("本地没有这个块%v", kv.gid, kv.me, shard)
			reply.Err = ErrNoKey
			return
		}
		reply.DB[shard], reply.Cid2Seq[shard] = kv.getDeepCopy(shard, true, ConfigMsgReply{})
	}
	DPrintf("pullshard成功，返回结果%v", kv.gid, kv.me, reply)
	reply.Err = OK
}
func (kv *ShardKV) getDeepCopy(shard int, isKv bool, reply ConfigMsgReply) (map[string]string, map[int64]int) {
	DPrintf("深度复制", kv.gid, kv.me)
	ddb := make(map[string]string)
	replt := make(map[int64]int)
	dbKv := kv.db
	lastReplyKv := kv.lastReply
	if !isKv {
		dbKv = reply.DB
		lastReplyKv = reply.Cid2Seq
	}
	for key, value := range dbKv[shard] {
		ddb[key] = value
	}
	for key, value := range lastReplyKv[shard] {
		replt[key] = value
	}
	return ddb, replt
}

/**
发送pull的方法
*/
func (kv *ShardKV) pullShard() {
	kv.mu.Lock()
	DPrintf("pull块", kv.gid, kv.me)
	GidShard := kv.getGidShards()
	for gid, shard := range GidShard {
		go kv.requestShardData(gid, shard)
	}
	kv.mu.Unlock()
}
func (kv *ShardKV) getGidShards() map[int][]int {
	GidShard := make(map[int][]int)
	DPrintf("得到gid对应的shard,cfg%v , lastcfg%v,shardManger%v", kv.gid, kv.me, kv.cfg, kv.lastCfg, kv.shardManger)
	for key, value := range kv.shardManger {
		if value == "NeedPull" {
			if _, ok := GidShard[kv.lastCfg.Shards[key]]; !ok {
				GidShard[kv.lastCfg.Shards[key]] = []int{key}
			} else {
				GidShard[kv.lastCfg.Shards[key]] = append(GidShard[kv.lastCfg.Shards[key]], key)
			}
		}
	}
	return GidShard
}

//发送的方法
func (kv *ShardKV) requestShardData(gid int, shardIds []int) {
	//获取shardid相关group
	DPrintf("发送pullshard", kv.gid, kv.me)
	group := kv.lastCfg.Groups[gid]
	args := ConfigMsgArgs{
		Shards:    shardIds,
		ConfigNum: kv.cfg.Num,
	}
	DPrintf("group %v", kv.gid, kv.me, group)
	for _, value := range group {
		srv := kv.make_end(value)
		var Reply ConfigMsgReply
		DPrintf("发送pullshard -->%v，arg%v", kv.gid, kv.me, value, args)
		ok := srv.Call("ShardKV.GetPull", &args, &Reply)
		if ok && Reply.Err == OK {
			//成功
			DPrintf("发送成功 %v ， %v", kv.gid, kv.me, value, Reply)
			kv.mu.Lock()
			if Reply.ConfigNum < kv.cfg.Num {
				DPrintf("replynum太迟了", kv.gid, kv.me)
				kv.mu.Unlock()
				return
			}
			for _, shardIds := range Reply.Shards {
				if status, ok := kv.shardManger[shardIds]; !ok {
					DPrintf("shardManger中没有这个 %v", kv.gid, kv.me, shardIds)
					kv.mu.Unlock()
					return
				} else if status != "NeedPull" {
					DPrintf("shardManger中这个状态不对%v %v", kv.gid, kv.me, shardIds, status)
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
			DPrintf("raft", kv.gid, kv.me, shardIds)
			kv.rf.Start(Op{
				OpType: "UpdateShares",
				Data:   Reply,
			})
			return
		}
	}
}

func (kv *ShardKV) updateShare(reply ConfigMsgReply) CommandResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//判断是否过期
	DPrintf("修改share", kv.gid, kv.me)
	if reply.ConfigNum < kv.cfg.Num {
		return CommandResponse{ErrWrongGroup}
	}
	for _, shardId := range reply.Shards {
		if status, ok := kv.shardManger[shardId]; !ok {
			DPrintf("shardManger中没有这个 %v", kv.gid, kv.me, shardId)
			break
		} else if status != "NeedPull" {
			DPrintf("shardManger中这个状态不对%v %v", kv.gid, kv.me, shardId, status)
			break
		}

		db, replt := kv.getDeepCopy(shardId, false, reply)
		kv.db[shardId] = db
		kv.lastReply[shardId] = replt
		DPrintf("last %v", kv.gid, kv.me, kv.lastReply)
		kv.shardManger[shardId] = "CanUse"
		DPrintf("更新shard成功 shardId%v cfg%v db%v ", kv.gid, kv.me, shardId, kv.cfg, kv.db)
	}
	return CommandResponse{OK}
}

/**
gc相关操作
*/
//将需要gc的shard进行push
func (kv *ShardKV) pushGC() {
	DPrintf("pushGC", kv.gid, kv.me)
	gidShards := kv.getGcShard()
	for key, value := range gidShards {
		kv.mu.Lock()
		num := kv.cfg.Num
		group := kv.cfg.Groups[key]
		kv.mu.Unlock()
		go kv.QueryGc(value, num, group)
	}
}
func (kv *ShardKV) getGcShard() map[int][]int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shards := make(map[int][]int, len(kv.gcManager))
	DPrintf("gc：得到gid对应的shard,cfg%v,shardManger%v", kv.gid, kv.me, kv.cfg, kv.shardManger)
	for key := range kv.gcManager {
		if kv.gcManager[key] == true {
			aaa := kv.cfg.Shards[key]
			if aaa != kv.gid {
				_, ok := shards[aaa]
				if ok {
					shards[aaa] = append(shards[aaa], key)
				} else {
					shards[aaa] = []int{key}
				}
			}
		}
	}
	return shards
}
func (kv *ShardKV) QueryGc(shard []int, num int, group []string) {
	args := ConfigMsgArgs{}
	args.Shards = shard
	args.ConfigNum = num
	//发送信息
	DPrintf("发送GC", kv.gid, kv.me)
	for si := 0; si < len(group); si++ {
		src := kv.make_end(group[si])
		var reply ConfigMsgReply
		ok := src.Call("ShardKV.RequestGc", &args, &reply)
		DPrintf("发送GC%v %v", kv.gid, kv.me, args, group[si])
		if ok && reply.Err == OK {
			//成功
			DPrintf("发送GC成功%v", kv.gid, kv.me, reply)
			kv.mu.Lock()
			if reply.ConfigNum < kv.cfg.Num {
				kv.mu.Unlock()
				DPrintf("reply的num比kv的小", kv.gid, kv.me)
				return
			}
			for _, shardIds := range reply.Shards {
				if status, ok := kv.gcManager[shardIds]; !ok {
					kv.mu.Unlock()
					return
				} else if !status {
					kv.mu.Unlock()
					return
				}
			}
			kv.mu.Unlock()
			kv.rf.Start(Op{
				OpType: "UpdateGc",
				Data:   reply,
			})
			return
		}
	}
}

// 集群处理来的gc通知
func (kv *ShardKV) RequestGc(args *ConfigMsgArgs, reply *ConfigMsgReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	DPrintf("响应Gc", kv.gid, kv.me)
	reply.ConfigNum = kv.cfg.Num
	reply.Shards = args.Shards
	if !isLeader {
		DPrintf("不是leader", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	if kv.cfg.Num < args.ConfigNum {
		reply.Err = ErrRequest
		DPrintf("本地cfg版本低", kv.gid, kv.me)
		return
	} else if kv.cfg.Num > args.ConfigNum {
		reply.Err = OK
		DPrintf("本地cfg版本高", kv.gid, kv.me)
		return
	} else {
		//对shard进行循环鉴定，看要不要进行gc
		for _, shardId := range args.Shards {
			if status, Ok := kv.shardManger[shardId]; !Ok || status != "CanUse" {
				DPrintf("不可以gc %v,%v", kv.gid, kv.me, Ok, status)
				reply.Err = ErrRequest
				return
			}
		}
	}
	reply.Err = OK
}
func (kv *ShardKV) deleteGCShard(reply ConfigMsgReply) CommandResponse {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.ConfigNum < kv.cfg.Num {
		return CommandResponse{ErrWrongGroup}
	}
	for _, shardId := range reply.Shards {
		DPrintf("删除gc %v", kv.gid, kv.me, shardId)
		if _, Ok := kv.gcManager[shardId]; !Ok {
			return CommandResponse{OK}
		}
		delete(kv.gcManager, shardId)
		delete(kv.db, shardId)
		delete(kv.lastReply, shardId)
	}
	return CommandResponse{OK}
}

/*
*server层上的快照生成
 */
func (kv *ShardKV) takeSnapshot() {
	DPrintf("快照同步", kv.gid, kv.me)
	_, isleade := kv.rf.GetState()
	if !isleade {
		DPrintf("当前不是leader", kv.gid, kv.me)
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.lastReply)
	e.Encode(kv.shardManger)
	//e.Encode(kv.gcManager)
	e.Encode(kv.cfg)
	e.Encode(kv.lastCfg)
	applyRaftLogIndex := kv.applyRaftLogIndex
	kv.mu.Unlock()
	//同步到raft层
	DPrintf("快照同步到raft", kv.gid, kv.me)
	//applyRaftLogIndex -1 ,应该是底层提交的最大日志
	kv.rf.TakeRaftSnapShot(applyRaftLogIndex-1, w.Bytes())
}

/*
server层上的快照保存
*/
func (kv *ShardKV) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("快照保存", kv.gid, kv.me)
	if snapshot != nil {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.db)
		d.Decode(&kv.lastReply)
		d.Decode(&kv.shardManger)
		//d.Decode(&kv.gcManager)
		d.Decode(&kv.cfg)
		d.Decode(&kv.lastCfg)
	}
}

/*
判断快照的主方法
*/
func (kv *ShardKV) snapshotMonitor() {
	for {
		if kv.maxraftstate == -1 {
			return
		}
		size := kv.rf.Persister.RaftStateSize()
		if size >= kv.maxraftstate {
			DPrintf("开启快照 %v,%v", kv.gid, kv.me, size, kv.maxraftstate)
			kv.takeSnapshot()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) checkLeaderNewestLog() {
	if !kv.rf.HasCurrentTermInLog() {
		kv.rf.Start(Op{OpType: "EmptyEntry"})
	}
}
