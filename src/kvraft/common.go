package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrApplyTimeout = "ErrApplyTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Seq      int64
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Seq      int64
	Key      string
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
