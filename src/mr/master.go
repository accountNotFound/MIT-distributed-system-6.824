package mr

import (
	"sync"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type tPhase uint8

const _RPCtype			="tcp"
const _MasterSocket		="127.0.0.1:1234"
const _MaxExecTimeOut	=time.Duration(5)*time.Second

const (
	_Init	tPhase=iota
	_Mapping
	_Mapped
	_Reducing
	_Reduced
	_Done
)

// Master struct public
type Master struct{
	mtx		sync.Mutex
	phase	tPhase
	files	[]string
	nReduce	int
	waits	*tQueue
	dones	*tSet
}
// MakeMaster return immediately
func MakeMaster(files []string, nReduce int) *Master{
	var m=Master{
		phase:		_Init,
		files:		files,
		nReduce:	nReduce,
	}
	rpc.Register(&m)
	rpc.HandleHTTP()
	var sock, err=net.Listen(_RPCtype, _MasterSocket)
	if err!=nil{
		log("fatal: master net listen err %v", err)
		return nil
	}
	go http.Serve(sock, nil)
	log("master start")
	return &m
}
// Done to check if master exits
func (m *Master) Done() bool{
	if m.phase==_Done { log("master exit") }
	return m.phase==_Done
}

// should be called with lock
func (m *Master) update(){
	switch m.phase{
	case _Init:
		log("start mapping")
		m.waits=newQueue(len(m.files))
		for i:=0;i<len(m.files);i++{ m.waits.put(i) }
		m.dones=newSet(len(m.files))
		m.phase++
		fallthrough
	case _Mapping:
		if m.dones.size()<len(m.files){ return }
		m.phase++
		fallthrough
	case _Mapped:
		log("start reducing")
		m.waits=newQueue(m.nReduce)
		for i:=0;i<m.nReduce;i++{ m.waits.put(i) }
		m.dones=newSet(m.nReduce)
		m.phase++
		fallthrough
	case _Reducing:
		if m.dones.size()<m.nReduce{ return }
		m.phase++
		fallthrough
	case _Reduced:
		log("all done")
		m.phase++
		fallthrough		
	case _Done:
		return
	}
}

// AquireJob from master, return immediately
// rpc by worker
func (m *Master) AquireJob(noArgs *bool, job *Job) error{
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.update()
	if m.waits.size()==0 && m.phase==_Done{
		log("send Done message")
		job.Phase=_Done
		return nil
	}else if m.waits.size()==0 && m.phase!=_Done{
		log("send None message")
		job.Phase=_Init
		return nil
	}
	var id=m.waits.get()
	*job=Job{
		Phase:		m.phase,
		ID:			id,
		MapNum:		len(m.files),
		ReduceNum:	m.nReduce,
	}
	if m.phase==_Mapping{ job.Name=m.files[id] }
	log("send job(%v, %d)", m.phase, id)

	go func(phase tPhase, id int){
		time.Sleep(_MaxExecTimeOut)
		m.mtx.Lock()
		defer m.mtx.Unlock()
		if m.phase!=phase{ return }
		if m.dones.has(id){ return }
		log("job(%v, %d) time out", phase, id)
		m.waits.put(id)
	}(m.phase, id)
	
	return nil
}

// SubmitJob to master, return immediately
// rpc by worker
// default thinking the job execute result is success
func (m *Master) SubmitJob(job *Job, noReply *bool) error{
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.update()
	if m.phase!=job.Phase{ return nil }
	if m.dones.has(job.ID){ return nil }
	log("get job(%v, %d)", job.Phase, job.ID)
	m.dones.put(job.ID)
	return nil
}