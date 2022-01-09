package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRequest     = "ErrRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int
	ShareId   int
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int
	ShareId   int
	ConfigNum int
}

type GetReply struct {
	Err   Err
	Value string
}

// 避免重试的代价，此处选取pull方法， 通过接受端向发送端发送pull请求，然后获取内容的方法
type ConfigMsgArgs struct {
	Shards    []int
	ConfigNum int
}
type ConfigMsgReply struct {
	Err       Err
	ConfigNum int
	Shards    []int
	DB        map[int]map[string]string
	Cid2Seq   map[int]map[int64]int
}
type CommandResponse struct {
	Err string
}
