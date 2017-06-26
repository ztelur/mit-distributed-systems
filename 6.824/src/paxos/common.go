package paxos


const (
	OK             = "OK"
  REJECT         = "REJECT"
)

type Status string


// Put or Append

type PrepareArgs struct {
	Seq int
	Num int64
}

type PrepareReply struct {
	Err string
	Num int64
	Value interface{}
}

type AcceptArgs struct {
	Seq int
	Num int64
	Value interface{}
}

type AcceptReply struct {
	Err string
}

type DecidedArgs struct {
	Seq int
	Num int64
	Value interface{}
	Me int
	Done int
}

type DecidedReply struct {

}
