package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "time"
import "crypto/rand"
import "math/big"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary string
	me string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.me = me
	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
		fmt.Printf("Get %v \n", key)
		// Your code here.
		//先call缓存的primary,如果失败了,说明primary已经没有了，fetchNewViewService信息，然后再去call
	for {
		args := &GetArgs{key, ck.me}
		var reply GetReply
		ret := call(ck.primary, "PBServer.Get", args, &reply)

		if !ret {
			// continue
			ck.fetchNewServiceInfo()
			continue
		}
		fmt.Printf("Client: Get %v %v \n", key, reply.Err)
		if reply.Err == ErrWrongServer {
			fmt.Printf("Client: wrong server\n")
			ck.fetchNewServiceInfo()
			continue
		} else if reply.Err == ErrNoKey {
			return ""
		} else {
			fmt.Printf("Client: Get %v %v %v \n", key, reply.Value)
			return reply.Value
		}
		time.Sleep(viewservice.PingInterval)
	}

	return "???"
}


func (ck *Clerk) fetchNewServiceInfo() {
	view, ok := ck.vs.Get()
	if ok {
		ck.primary = view.Primary
	} else {
		ck.primary = ""
	}
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// Your code here.
	fmt.Printf("put %v %v %v cureent primary is %v \n", key, value, op, ck.primary)
	for {
		args := &PutAppendArgs{key, value, op, ck.me}
		var reply PutAppendReply
		ret := call(ck.primary, "PBServer.PutAppend", args, &reply)


		if !ret {
			// continue
			ck.fetchNewServiceInfo()
			fmt.Printf("fetch new service primary is %v \n", ck.primary)
			continue
		}

		if reply.Err == ErrWrongServer {
			ck.fetchNewServiceInfo()
			continue
		} else if reply.Err == ErrNoKey {
			//append faile,put not will occur
			return
		} else {
			//success
			return
		}
		time.Sleep(viewservice.PingInterval)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
