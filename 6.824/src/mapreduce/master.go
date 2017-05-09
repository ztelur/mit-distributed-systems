package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func handle(worker string, file string) {
	args := &DoJobArgs{}
	args.File = file
	args.Operation = Map
	args.JobNumber = 1
	args.NumOtherPhase = 1
	var reply DoJobReply
	ok := call(worker, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("Do map: RPC %s \n", worker)
	}


}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	for {
		newWorker := <- mr.registerChannel
		go handle(newWorker, mr.file)
	}
	return mr.KillWorkers()
}
