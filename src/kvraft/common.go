package kvraft

const (
	OK = iota
	ErrWrongLeader
)

type Err int

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Id     int64
	SeqNum int64
	Op     string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Id  int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
