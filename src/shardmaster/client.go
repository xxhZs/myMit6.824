package shardmaster

//
// Shardmaster clerk.
//

import "6.824lab/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	//lastLeader int
	ClientId  int64
	RequestId int
	leaderID  int
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
	// Your code here.
	ck.RequestId = 0
	//ck.lastLeader = 0
	ck.ClientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	requestId := ck.RequestId + 1
	args.Num = num
	args.RequestId = ck.RequestId
	args.Cid = ck.ClientId
	for {
		// try each known server.
		leaderId := ck.leaderID
		for si := 0; si < len(ck.servers); si++ {
			srv := ck.servers[leaderId]
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderID = leaderId
				return reply.Config
			}
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		//ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		ck.RequestId = requestId
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.RequestId = ck.RequestId
	ck.RequestId++
	args.Cid = ck.ClientId
	for {
		// try each known server.
		leaderId := ck.leaderID
		for si := 0; si < len(ck.servers); si++ {
			srv := ck.servers[leaderId]
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderID = leaderId
				return
			}
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		//ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.RequestId = ck.RequestId
	ck.RequestId++
	args.Cid = nrand()
	for {
		// try each known server.
		leaderId := ck.leaderID
		for si := 0; si < len(ck.servers); si++ {
			srv := ck.servers[leaderId]
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderID = leaderId
				return
			}
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		//ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.RequestId = ck.RequestId
	args.Cid = nrand()
	ck.RequestId++
	for {
		// try each known server.
		leaderId := ck.leaderID
		for si := 0; si < len(ck.servers); si++ {
			srv := ck.servers[leaderId]
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leaderID = leaderId
				return
			}
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		//ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
