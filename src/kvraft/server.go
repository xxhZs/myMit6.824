package kvraft

import (
	"6.824lab/labgob"
	"6.824lab/labrpc"
	"6.824lab/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, kvId int, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		format = "[peer %v] " + format
		a = append([]interface{}{kvId}, a...)
		fmt.Printf(format+"\n", a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string // 数据
	lastReply map[int64]int     // 防止重复执行
	chMap     map[int]chan Op
	//实现快照
	applyRaftLogIndex int
	Persister         *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Kv 执行get,%v", kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.IsLeader = false
		return
	}
	//是leader，所以需要进行请求处理,判断是不是重复请求
	kv.mu.Lock()
	lastReply, ok := kv.lastReply[args.ClientId]
	if ok && lastReply > args.RequestId {
		DPrintf("指令过期%v,%v", kv.me, lastReply, args.RequestId)
		reply.Value = kv.db[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//从raft进行操作
	command := Op{
		Key:       args.Key,
		OpType:    "GET",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	isok, appliOp := kv.getCh(command)
	if !isok {
		reply.Err = ErrWrongLeader
		reply.IsLeader = false
		return
	}
	reply.IsLeader = true
	reply.Err = OK
	reply.Value = appliOp.Value
}

func (kv *KVServer) getCh(op Op) (bool, Op) {

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
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Kv 执行PutAppend,%v", kv.me, args)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.IsLeader = false
		return
	}
	reply.IsLeader = true

	//这里要不要判断重复呢？
	//kv.mu.Lock()
	//lastReply, ok := kv.lastReply[args.ClientId]
	//if ok && lastReply > args.RequestId {
	//	DPrintf("指令过期%v,%v", kv.me, lastReply, args.RequestId)
	//	reply.Err = ErrWrongLeader
	//	reply.IsLeader = false
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()
	commod := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, _ := kv.getCh(commod)
	if !ok {
		reply.Err = ErrWrongLeader
		reply.IsLeader = false
		return
	}
	reply.Err = OK
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.chMap = make(map[int]chan Op)
	kv.db = make(map[string]string)
	kv.lastReply = make(map[int64]int)
	//初始化shop
	kv.Persister = persister
	snap := kv.Persister.ReadSnapshot()
	if snap != nil && len(snap) > 0 {
		kv.installSnapshot(snap)
	}

	go kv.waitCommit()
	go kv.snapshotMonitor()

	return kv
}
func (kv *KVServer) waitCommit() {
	DPrintf("循环等待提交", kv.me)
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.IsSnap {
				kv.installSnapshot(msg.SnapShot)
				continue
			}
			op := msg.Command.(Op)
			DPrintf("收到提交 Op%v", kv.me, op)
			kv.mu.Lock()
			DPrintf("com:%v", kv.me, msg)
			kv.applyRaftLogIndex = msg.CommandIndex
			requedtId, ok := kv.lastReply[op.ClientId]
			if op.OpType == "GET" {
				op.Value = kv.db[op.Key]
				DPrintf("get到的库 %v", kv.me, kv.db[op.Key])
			} else {
				if !ok || op.RequestId > requedtId {
					switch op.OpType {
					case "Put":
						DPrintf("put 写入库", kv.me)
						kv.db[op.Key] = op.Value
						DPrintf("put以后的库 %v", kv.me, kv.db[op.Key])
					case "Append":
						DPrintf("append 写入库", kv.me)
						kv.db[op.Key] += op.Value
						DPrintf("append以后的库 %v", kv.me, kv.db[op.Key])
					}
					kv.lastReply[op.ClientId] = op.RequestId
				}
			}
			DPrintf("getCh", kv.me)
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

/*
*server层上的快照生成
 */
func (kv *KVServer) takeSnapshot() {
	_, isleade := kv.rf.GetState()
	if !isleade {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.lastReply)
	applyRaftLogIndex := kv.applyRaftLogIndex
	kv.mu.Unlock()
	//同步到raft层
	kv.rf.TakeRaftSnapShot(applyRaftLogIndex-1, w.Bytes())
}

/*
server层上的快照保存
*/
func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot != nil {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		d.Decode(&kv.db)
		d.Decode(&kv.lastReply)
	}
}

/*
判断快照的主方法
*/
func (kv *KVServer) snapshotMonitor() {
	for {
		if kv.maxraftstate == -1 {
			return
		}
		if kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
			kv.takeSnapshot()
		}
		time.Sleep(100 * time.Millisecond)
	}
}
