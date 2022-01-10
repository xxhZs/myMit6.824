package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824lab/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"
import "6.824lab/shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm        *shardmaster.Clerk
	config    shardmaster.Config
	make_end  func(string) *labrpc.ClientEnd
	cid       int64
	RequestId int

	// You will have to modify this struct.
	gidLeader map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.cid = nrand()
	ck.RequestId = 0
	ck.gidLeader = make(map[int]int)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	requresId := ck.RequestId + 1
	for {
		shard := key2shard(key)
		args.ShareId = shard
		args.ConfigNum = ck.config.Num
		args.ClientId = ck.cid
		args.RequestId = requresId
		gid := ck.config.Shards[shard]
		if _, ok := ck.gidLeader[gid]; !ok {
			ck.gidLeader[gid] = 0
		}
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			flag := 0
			leaderId := ck.gidLeader[gid]
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[leaderId])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.gidLeader[gid] = leaderId
					ck.RequestId = requresId
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				flag++
				if flag > 20 {
					break
				}
				// ... not ok, or ErrWrongLeader
				leaderId = (leaderId + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	requresId := ck.RequestId + 1
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ShareId = shard
		args.ConfigNum = ck.config.Num
		args.ClientId = ck.cid
		args.RequestId = requresId
		if _, ok := ck.gidLeader[gid]; !ok {
			ck.gidLeader[gid] = 0
		}
		if servers, ok := ck.config.Groups[gid]; ok {
			flag := 0
			leaderId := ck.gidLeader[gid]
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[leaderId])
				var reply PutAppendReply
				fmt.Println("成功mm125", args, reply, ok)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				fmt.Println("成功mm ", args, reply, ok)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.RequestId = requresId
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				flag++
				if flag > 20 {
					break
				}
				leaderId = (leaderId + 1) % len(servers)
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
