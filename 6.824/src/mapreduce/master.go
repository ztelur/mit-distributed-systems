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
	doneChannel := make(chan int)

	for i:= 0; i< mr.nMap; i++ {
		go func(jobnumber int) {
			for {
				worker := <-mr.idleWorkerChannel
				args := &DoJobArgs{mr.file, Map, jobnumber, mr.nReduce}
				reply := &DoJobReply{}
				ok := call(worker, "Worker.DoJob", args, reply)
				if ok == true {
					mr.idleWorkerChannel <- worker
					doneChannel <- jobnumber
					return
				}
			}
		}(i)
	}

	for i:=0; i< mr.nMap; i++ {
		<- doneChannel
	}

	fmt.Printf("the reduce is %d \n", mr.nReduce)

	for j:= 0; j < mr.nReduce; j++ {
		go func(jobnumber int) {
			for {
				worker := <-mr.idleWorkerChannel
				args := &DoJobArgs{mr.file, Reduce, jobnumber, mr.nMap}
				reply := &DoJobReply{}
				ok := call(worker, "Worker.DoJob", args, reply)
				if ok == true {
					mr.idleWorkerChannel <- worker
					doneChannel <- jobnumber
					return
				}
			}
		}(j)
	}

	for j := 0; j < mr.nReduce; j++ {
		<- doneChannel
	}

	return mr.KillWorkers()
}
