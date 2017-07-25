package shardmaster

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

//manage a sequence of numbered configurations,每个configurations都有一系列replica group和shards的对应关系

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	currentSeq int //paxos sequence
	configs []Config // indexed by config num
	currentConfigsIndex int64 //当前config的index
}


type Op struct {
	// Your data here.
	Id int64
	GID     int64
	Servers []string
	Shard int
	Num int
	Op string
}

// client只会在对整个server group 的一个发送，如果第一个就成功了，那么它要通知其他的server,就是通过paxos
//注册一个新的replica group, 参数是replica id和server ip list
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.lock.Lock()
	defer sm.lock.Unlock()

	//check gid是否存在，如果存在直接返回。否在发送paxos请求。



	return nil
}

//之前注册的一个shard group id ,应该创建一个新的configuration并且把一个shard分配给一个新的remaining group中
//新的configuration应该将shard尽可恩能的分配到replica group中。
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	return nil
}

//shard number and a GID 这个shard被分配给这个replica group.可以用于load-balance,当某个shard比较火时进行。
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	return nil
}

// 查询某个configuration的详细配置，-1或在参数太大，就返回最新的配置
//第一个configuration的id为1
// It should contain no groups, and all shards should be assigned to GID zero
// shard会多于replica group,一个group会服务多个shard,为了流量的平衡
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	return nil
}


func (sm *ShardMaster) doPaxos(op Op) {
	var accept Op
	for {
		status, v := sm.px.Status(currentSeq)
		if status == paxos.Decided {
			accept = v.(op)
		} else {
			sm.px.Start(sm.currentSeq, op)
			accept = sm.wait(sm.currentSeq)
		}
	}
}


func (sm *ShardMaster) wait(seq int) Op {
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


// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
