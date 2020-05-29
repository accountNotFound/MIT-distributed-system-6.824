package mr

import (
	"net/rpc"
	"time"
)

const _WaitTime	=time.Duration(1000)*time.Millisecond

// Worker for test
func Worker(){
	log("worker start")
	var conn, err=rpc.DialHTTP(_RPCtype, _MasterSocket)
	if err!=nil{
		log("fatal: connect to master err %v", err)
	}
	defer conn.Close()
	for{
		var noArgs	bool
		var job 	Job
		err=conn.Call("Master.AquireJob", &noArgs, &job)
		if err!=nil{
			log("worker disconnect to master, exit, err %v", err)
			return
		}
		switch job.Phase{
		case _Mapping:
			execMap(job)
			conn.Call("Master.SubmitJob", &job, nil)
		case _Reducing:
			execReduce(job)
			conn.Call("Master.SubmitJob", &job, nil)
		case _Done:
			log("worker exit")
			return
		default:
			log("worker waiting")
			time.Sleep(_WaitTime)
		}
	}
}

func execMap(job Job){
	time.Sleep(time.Second*3)
	log("worker execute job(%v, %d)", job.Phase, job.ID)
}
func execReduce(job Job){
	time.Sleep(time.Second*8)
	log("worker execute job(%v, %d)", job.Phase, job.ID)
}
