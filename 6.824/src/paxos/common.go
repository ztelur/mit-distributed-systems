package paxos


const (
	OK             = "OK"
  REJECT         = "REJECT"
)

type Status string


// Put or Append
type PrepareArgs struct {
  Seq int
}

type PrepareReply struct {
	Status Status // ok , reject
  Accepted_seq int
  Accepted_value interface{}
}
