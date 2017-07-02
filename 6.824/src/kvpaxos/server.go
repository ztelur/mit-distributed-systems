package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	Key string
	Value string
	Op string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	database map[string]string
	currentSeq int
	clientRequest map[int64]bool
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//先判断是否接受过这个请求，at most once
	kv.mu.Lock()
	kv.mu.Unlock()

	handled := kv.clientRequest[args.Id]
	if handled {
		reply.Value = kv.database[args.Key]
		reply.Err = OK
		return nil
	}

	op := Op{Id: args.Id, Key: args.Key, Op: "GET"}
	kv.doPaxos(op)

	v, ok = kv.database[args.Key]
	if ok {
		reply.Value = v
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (kv *KVPaxos) doPaxos(op Op) {
		var accept Op
		for { //因为现在使用的是base paxos，每次没有让自己的提案没有被chosen，那么就继续
			//这里需要判断currentSeq的状态吗？
			// if kv.px.Status(currentSeq)
			//这里还是基于base paxos的，每个paxos instance都会对同一个seq进行提议，可能会导致很多冲突
			
			status, v := kv.px.Status(currentSeq) //因为Get请求可能同时很多请求，currentSeq不能重复
			if status == paxos.Decided {
				accept = v.(op)
			} else {
				kv.px.Start(kv.currentSeq, op)
				accept = kv.wait(kv.currentSeq)
			}
			//do operation
			kv.handleDatabase(accept)
			if v.Id == op.Id {
				break
			}

		}
}

func (kv *KVPaxos) handleDatabase(op Op) {
	kv.clientRequest[op.Id] = true
	if op.Op == "Put" {
		kv.database[op.Key] = op.Value
	} else if op.Op == "Append" {
		v, ok := kv.database[op.Key]
		if ok {
			kv.database[op.Key] = v + op.Value
		} else {
			kv.database[op.Key] = op.Value
		}
	}

	kv.px.Done(kv.currentSeq)
	kv.currentSeq += 1
}


func (kv *KVPaxos) wait(seq int) Op {
	break := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			return v.(op)
		}
		time.Sleep(break)
		if break < 10 * time.Second {
			break *= 2
		}
	}
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.clientRequest[args.Id]
	if ok {
		reply.Err = OK
		return nil
	}

	op := Op{Id: args.Id, Key: args.Key, Value: args.Value, Op: args.Op}
	kv.doPaxos(op)

	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
