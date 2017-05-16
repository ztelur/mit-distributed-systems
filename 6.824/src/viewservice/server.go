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
	primary := vs.primary
	backup := vs.backup
	client := vs.client
	isAcked := vs.isAcked
	view := vs.view

	clientName := args.Me
	viewNum := args.Viewnum
	log.Printf("new ping from %v, %d", clientName, viewNum)
	//update isAcked
	//每个view的primary都要acked,如果它的viewnum不匹配，那么就不算是acked
	if clientName == primary && viewNum == vs.view.Viewnum {
		log.Printf("isAcked from %v", clientName)
		vs.isAcked = true
	}

	if viewNum != vs.view.Viewnum { //init or reboot
		_, ok := client[clientName]
		if ok {
			//说明client reboot了．
			log.Printf("clinet reboot")
			if clientName == primary {
				log.Printf("clinet reboot primary")
				vs.primary = ""
				vs.isAcked = true
			}

			if clientName == backup {
				vs.backup = ""
			}
			client[clientName] = time.Now()
		} else {
			client[clientName] = time.Now()
		}
	} else {

		_, ok := client[clientName]
		if ok {
			client[clientName] = time.Now()
		} else {
			//不会发生
		}
	}

	if !isAcked { //isAcked为false,那么直接返回旧的view,否在要新建立一个view

	} else {
		vs.createNewView()
	}
	reply.View = View{}
	reply.View.Viewnum = view.Viewnum
	reply.View.Primary = view.Primary
	reply.View.Backup = view.Backup

	vs.mu.Unlock()
	return nil
}

//从client中构建新的view
func (vs *ViewServer)createNewView() {
	//只有发生变动是才要继续
	log.Printf("new view")
	if vs.primary == "" && vs.backup == "" { //直接从client map中构建
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
		vs.primary = primaryCandiate
		vs.backup = backupCandidate

	} else if (vs.primary == "") { // backup替代primary,然后从map中挑选一个
		log.Printf("primary is null")
		candidateTime := time.Now()
		backupCandidate := ""
		for key, value := range vs.client {
			if value.Before(candidateTime) && key != vs.backup {
				backupCandidate = key
				candidateTime = value
			}
		}
		vs.primary = vs.backup
		vs.backup = backupCandidate
	} else if (vs.backup == "") {
		log.Printf("backup is null")
		candidateTime := time.Now()
		backupCandidate := ""
		for key, value := range vs.client {
			if value.Before(candidateTime) && key != vs.primary {
				backupCandidate = key
				candidateTime = value
			}
		}
		vs.backup = backupCandidate
	}
	if vs.view.Primary != vs.primary || vs.view.Backup != vs.backup {
		vs.preView = vs.view
		vs.view = View{}
		vs.view.Viewnum = vs.num
		vs.num = vs.num + 1
		vs.isAcked = false
		vs.view.Primary = vs.primary
		vs.view.Backup = vs.backup
		log.Printf("change view");
	}
	log.Printf("the current view is %v %v %v ", vs.view.Viewnum, vs.primary, vs.backup)
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	// Your code here.
	if true {
		reply.View = View{}
		reply.View.Viewnum = vs.view.Viewnum
		reply.View.Primary = vs.view.Primary
		reply.View.Backup = vs.view.Backup
	} else {
		reply.View = View{}
		reply.View.Viewnum = vs.preView.Viewnum
		reply.View.Primary = vs.preView.Primary
		reply.View.Backup = vs.preView.Backup
	}
	vs.mu.Unlock()
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
	log.Printf("tick")

	nowTime := time.Now()
	if vs.primary != "" {
		log.Printf("the primary is " + vs.primary)
		pingTime, _ := vs.client[vs.primary]
		if nowTime.Sub(pingTime) > PingInterval * DeadPings { //primary reboot or crash
			log.Printf("the primary is crash" + vs.primary + " the %v",nowTime.Sub(pingTime))
			// 只有当view的acked,才会删除，否在一直等待
			if vs.isAcked {
				log.Printf("primary crash and remove it")
				delete(vs.client, vs.primary)
				vs.primary = ""
			} else {
				log.Printf("not acked so still watied")
			}
		}
	}

	if vs.backup != "" {
		pingTime, _ := vs.client[vs.backup]
		if nowTime.Sub(pingTime) > PingInterval * DeadPings {
			delete(vs.client, vs.backup)
			vs.backup = ""
		}
	}

	for key, value := range vs.client {
		if nowTime.Sub(value) > PingInterval * DeadPings {
				delete(vs.client, key)
		}
	}
	vs.preTime = time.Now()
	vs.mu.Unlock()
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
	vs.primary = ""
	vs.backup = ""
	vs.num = 1
	vs.preTime = time.Now()
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
