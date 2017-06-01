package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Type string
	// Field names must start with capital letters,
	// otherwise RPC will break.
	From string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	From string
}

type GetReply struct {
	Err   Err
	Value string
}

type CopyArgs struct {
	Data map[string]string
}

type CopyReply struct {
	Err Err
}

type ForwardArgs struct {
	Key string
	Value string
	Type string
}

type ForwardReply struct {
	Err Err
}

// Your RPC definitions here.
