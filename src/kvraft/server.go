package kvraft

import (
	"../labgob"
	"../labrpc"
	"fmt"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const _AGREE_TIME_OUT=time.Millisecond*200


const useDebug=0
var timeStamp=0

func dump(format string, a ...interface{}){
	if useDebug>0{
		timeStamp++
		fmt.Printf("%d: ", timeStamp)
		fmt.Printf(format+"\n", a...)
	}
	return
}

// Op is public 
type Op struct{
	Args 		*Args
	ReplyMsg	*chan *Reply
	LeaderID	int
}

// KVServer is public 
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db		map[string] string
	ch		map[int64] Reply
}

// ID of current KV server, rpc 
func (kv *KVServer) ID(noArgs *bool, reply *int){
	*reply=kv.me
}

// Get operation, rpc by Clerk
func (kv *KVServer) Get(args *Args, reply *Reply){
	// Your code here.
	kv.handle(args, reply)
}
// Put operation, rpc by Clerk
func (kv *KVServer) Put(args *Args, reply *Reply){
	kv.handle(args, reply)
}
// Append operation, rpc by Clerk
func (kv *KVServer) Append(args *Args, reply *Reply){
	kv.handle(args, reply)
}

// Kill current KVServer
func (kv *KVServer) Kill() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	dump("S(%d) be killed", kv.me)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer service and return immediately
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 256)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db=make(map[string] string)
	kv.ch=make(map[int64] Reply)

	go func(){
		for !kv.killed(){
			var op=(<-kv.applyCh).Command.(Op)
			kv.mu.Lock()
			var res=kv.checkAndExecute(op.Args)
			if op.LeaderID==kv.me{ *op.ReplyMsg<-res }
			kv.mu.Unlock()
		}
	}()
	dump("S(%d) start", kv.me)

	return kv
}

func (kv *KVServer) handle(args *Args, reply *Reply){
	dump("S(%d) recieve %s", kv.me, args.String())	
	kv.mu.Lock()
	var res, msg=kv.checkAndStart(args)
	kv.mu.Unlock()
	if res!=nil{
		*reply=*res
	}else if msg!=nil{
		select{
		case <-time.After(_AGREE_TIME_OUT):
			reply.Meta, reply.Redirect, reply.LeaderID=args.Meta, true, kv.rf.GetLeader()
		case res:=<-*msg:
			*reply=*res
		}		
	}else{
		reply.Meta, reply.Redirect, reply.LeaderID=args.Meta, true, kv.rf.GetLeader()
		// dump("S(%d) send %v", kv.me, *reply)
	}
}

// call with lock
func (kv *KVServer) check(args *Args) *Reply{
	if cache, ok:=kv.ch[args.ClientID]; ok && args.RequestID<=cache.RequestID{ 
		dump("S(%d) cache %s", kv.me, cache.String())
		return &cache 
	}
	return nil
}
// call with lock
func (kv *KVServer) checkAndExecute(args *Args) *Reply{
	if cache:=kv.check(args);cache!=nil{ return cache }
	var reply=Reply{
		Meta:		args.Meta,
		Redirect:	false,
		LeaderID:	-1,
	}
	switch args.Type{
	case _GET:		reply.Value=kv.db[args.Key]
	case _PUT:		kv.db[args.Key]=args.Value
	case _APPEND:	kv.db[args.Key]=kv.db[args.Key]+args.Value
	}
	kv.ch[args.ClientID]=reply
	dump("S(%d) execute %s", kv.me, args.String())
	return &reply
}
// call with lock
func (kv *KVServer) checkAndStart(args *Args) (*Reply, *chan *Reply){
	if cache:=kv.check(args);cache!=nil{ return cache, nil }
	var msg=make(chan *Reply, 1)
	var op=Op{
		Args:		args,
		ReplyMsg:	&msg,
		LeaderID:	kv.me,
	}
	// dump("S(%d) try start %v", kv.me, *args)
	if _, _, ok:=kv.rf.Start(op); ok{ return nil, &msg }
	// dump("S(%d) is not leader", kv.me)
	return nil, nil
}

