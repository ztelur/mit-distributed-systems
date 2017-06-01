package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	data map[string]string
	view viewservice.View

}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//先check一下自己是不是primary
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	key := args.Key
	value, exist := pb.data[key]
	if exist {
		fmt.Printf("Server:Get the key is %v the value is %v \n", key, value)
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	//通知Backup


	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//先check一下自己是不是primary
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	key := args.Key
	value := args.Value
	action := args.Type

	fmt.Printf("Server:Put %v %v %v \n", key, value, action)
	if action == "Put" {
		pb.data[key] = value
	} else if action == "Append" {
		val, exist := pb.data[key]
		if exist {
			pb.data[key] = val + value
		} else {
			pb.data[key] = value
		}
	}

	if pb.view.Backup != "" {
		forwardArgs := &ForwardArgs{key, value, action}
		var forwardReply ForwardReply
		for {
			ok := call(pb.view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
			fmt.Printf("the forward to %v and reply is %v", pb.view.Backup, forwardReply.Err)
			if ok {
				if forwardReply.Err == OK {
					break
				}
			} else {
				break;
			}
			time.Sleep(viewservice.PingInterval)
		}
	}

	return nil
}



func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	//先看一下自己是不是backup
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	key := args.Key
	value := args.Value
	action := args.Type

	fmt.Printf("Server:Forward %v %v %v \n", key, value, action)
	if action == "Put" {
		pb.data[key] = value
	} else if action == "Append" {
		val, exist := pb.data[key]
		if exist {
			pb.data[key] = val + value
		} else {
			pb.data[key] = value
		}
	}

	reply.Err = OK

	return nil
}

func (pb *PBServer) CopyData(args *CopyArgs, reply *CopyReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.data = args.Data
	reply.Err = OK
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	view, _ := pb.vs.Ping(pb.view.Viewnum)
	if pb.view.Viewnum != view.Viewnum {
		if pb.view.Primary == pb.me && view.Backup != "" {
			//传输data
			args := &CopyArgs{pb.data}
			var reply CopyReply
			for {
				call(view.Backup, "PBServer.CopyData", args, &reply)
				if reply.Err == OK {
					break
				}
				time.Sleep(viewservice.PingInterval)
			}
		}
	}
	pb.view.Primary = view.Primary
	pb.view.Backup = view.Backup
	pb.view.Viewnum = view.Viewnum
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.view.Viewnum = 0
	pb.view.Primary = ""
	pb.view.Backup = ""
	pb.data = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
