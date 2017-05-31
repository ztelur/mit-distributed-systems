package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	client map[string]time.Time
	view View
	preView View
	isAcked bool
	primary string
	backup string
	num uint
	preTime time.Time
	// Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	me := args.Me
	viewnum := args.Viewnum
	log.Printf("new ping from %v, %d", me, viewnum)
	//update isAcked
	//每个view的primary都要acked,如果它的viewnum不匹配，那么就不算是acked
	if viewnum == 0 { //init or reboot
		_, exist := vs.client[me]
		if exist {
			//说明client reboot了．
			log.Printf("clinet reboot")
			if me == vs.view.Primary {
				log.Printf("clinet reboot primary")
				vs.view.Primary = ""
			} else if me == vs.view.Backup {
				vs.view.Backup = ""
			}
		}
		//
		vs.client[me] = time.Now()
	} else {
		_, exist := vs.client[me]
		if exist {
			//需要注意的是，只有当view的num相同时，才能认为是ack
			if me == vs.view.Primary && viewnum == vs.view.Viewnum {
				log.Printf("isAcked from %v", me)
				vs.isAcked = true
			}
		}
		vs.client[me] = time.Now()
	}

	if vs.isAcked {
		vs.createNewView()
	}
	reply.View = vs.view
	return nil
}

//从client中构建新的view
func (vs *ViewServer)createNewView() {
	//只有发生变动是才要继续
	log.Printf("new view")
	if vs.view.Primary == "" && vs.view.Backup == "" { //直接从client map中构建
		log.Printf("primary and backup is null")
		primaryCandiate := ""
		candidateTime := time.Now()
		backupCandidate := ""
		for key, value := range vs.client {
			log.Printf("the key and value is %v , %v", key, value)
			if value.Before(candidateTime) {
				backupCandidate = primaryCandiate
				primaryCandiate = key
				candidateTime = value
			}
		}
		vs.view.Viewnum += 1
		vs.view.Primary = primaryCandiate
		vs.view.Backup = backupCandidate
		vs.isAcked = false
	} else if (vs.view.Primary == "") { // backup替代primary,然后从map中挑选一个
		log.Printf("primary is null")
		candidateTime := time.Now()
		backupCandidate := ""
		for key, value := range vs.client {
			if value.Before(candidateTime) && key != vs.view.Backup {
				backupCandidate = key
				candidateTime = value
			}
		}
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = backupCandidate
		vs.view.Viewnum += 1
		vs.isAcked = false
	} else if (vs.view.Backup == "") {
		log.Printf("backup is null")
		candidateTime := time.Now()
		backupCandidate := ""
		for key, value := range vs.client {
			if value.Before(candidateTime) && key != vs.view.Primary {
				backupCandidate = key
				candidateTime = value
			}
		}
		if backupCandidate != "" {
			vs.view.Backup = backupCandidate
			vs.view.Viewnum += 1
			vs.isAcked = false
		}
	}

	log.Printf("the current view is %v %v %v ", vs.view.Viewnum, vs.view.Primary, vs.view.Backup)
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.view
	log.Printf("the get view is %v,%v,%v", vs.view.Viewnum, vs.view.Primary, vs.view.Backup)

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	//if primary reboot or crash, isAcked should be true
	vs.mu.Lock()
	defer vs.mu.Unlock()
	log.Printf("tick")

	if !vs.isAcked {
		return
	}
	nowTime := time.Now()
	if vs.view.Primary != "" {
		log.Printf("the primary is " + vs.view.Primary)
		pingTime, _ := vs.client[vs.view.Primary]
		if nowTime.Sub(pingTime) > PingInterval * DeadPings { //primary reboot or crash
			log.Printf("the primary is crash" + vs.view.Primary + " the %v",nowTime.Sub(pingTime))
			// 只有当view的acked,才会删除，否在一直等待
			if vs.isAcked {
				log.Printf("primary crash and remove it")
				delete(vs.client, vs.view.Primary)
				vs.view.Primary = ""
			} else {
				log.Printf("not acked so still watied")
			}
		}
	}

	if vs.view.Backup != "" {
		pingTime, _ := vs.client[vs.view.Backup]
		if nowTime.Sub(pingTime) > PingInterval * DeadPings {
			delete(vs.client, vs.view.Backup)
			vs.view.Backup = ""
		}
	}

	for key, value := range vs.client {
		if nowTime.Sub(value) > PingInterval * DeadPings {
				delete(vs.client, key)
		}
	}

	// Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.client = make(map[string]time.Time)
	vs.isAcked = true
	vs.view = View{}
	vs.view.Primary = ""
	vs.view.Backup = ""
	vs.view.Viewnum = 0
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
