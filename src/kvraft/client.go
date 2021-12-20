package kvraft

import "6.824lab/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	ClientId   int64
	RequestId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.ClientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("Clerk执行Get制定，key为 %v", -1, key)
	requresId := ck.RequestId + 1
	for {
		arg := GetArgs{
			Key:       key,
			ClientId:  ck.ClientId,
			RequestId: requresId,
		}
		var reply GetReply
		DPrintf("Clerk向KVServer发送get，key为 %v", -1, key)
		flag := ck.servers[ck.lastLeader].Call("KVServer.Get", &arg, &reply)
		DPrintf("Clerk处理get KV结果", -1)
		if flag == false || reply.IsLeader == false {
			DPrintf("不是leader，换一个", -1)
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}
		DPrintf("get成功执行", -1)
		ck.RequestId = requresId
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("Clerk执行PutAppend，key为 %v value，%v op %v", -1, key, value, op)
	requestId := ck.RequestId + 1
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.ClientId,
			RequestId: requestId,
		}
		var reply PutAppendReply
		DPrintf("Clerk发送PutAppend，key为 %v", -1, args)
		flag := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("Clerk收到结果PutAppend %v", -1, reply)
		if flag == false || reply.IsLeader == false {
			DPrintf("不是leader", -1)
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}
		DPrintf("PutAppend成功执行", -1)
		ck.RequestId = requestId
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
