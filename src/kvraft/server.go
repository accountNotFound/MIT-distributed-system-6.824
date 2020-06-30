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
var dumpLock sync.Mutex

func dump(format string, a ...interface{}){
	if useDebug>0{
		dumpLock.Lock()
		timeStamp++
		fmt.Printf("%d: ", timeStamp)
		fmt.Printf(format+"\n", a...)
		dumpLock.Unlock()
	}
	return
}


type _Cache struct{
	SEQ		int
	Value	string
}
func (c *_Cache) String() string{
	var value=c.Value
	if len(value)>5{ value=value[0: 5] }
	return fmt.Sprintf("{%d \"%s\"}", c.SEQ, value)
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
	db			map[string] string
	cache		map[int] _Cache			// client UID -> Cache, for request out-of-date check
	notif		map[int] chan string	// raft log index -> string, for notifying corresponding routine(for leader)
} 
func (kv *KVServer) String() string{
	// var _, isleader=kv.rf.GetState()
	// var leaderID=kv.rf.GetLeader()
	return fmt.Sprintf("S(%d)", kv.me)
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
	dump("%s be killed", kv.String())
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
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
	labgob.Register(Args{})

	var kv=KVServer{
		me:				me,
		maxraftstate:	maxraftstate,
		applyCh:		make(chan raft.ApplyMsg, 256),
		db:				make(map[string] string),
		cache:			make(map[int] _Cache),
		notif:			make(map[int] chan string),
	}
	
	// You may need initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func(){
		for !kv.killed(){
			var msg=<-kv.applyCh
			kv.checkAndApply(msg.CommandIndex, msg.Command.(Args))
		}
	}()
	dump("%s start", kv.String())

	return &kv
}

func (kv *KVServer) handle(args *Args, reply *Reply){
	dump("%s recieve %s", kv.String(), args.String())
	if msg:=kv.checkAndStart(*args); msg==nil{
		dump("%s is not leader", kv.String())
	}else{ 
		select{
		case <-time.After(_AGREE_TIME_OUT):
			dump("%s agree %s time out", kv.String(), args.String())
		case value:=<-msg:
			*reply=Reply{
				Valid:	true,
				Value:	value,
			}
			dump("%s send %s to C(%d)", kv.String(), reply.String(), args.UID)
		}
	}
}

func (kv *KVServer) checkAndStart(args Args) chan string{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.dead==1{ return nil }
	var res=make(chan string, 1)
	if cache, ok:=kv.cache[args.UID]; ok && args.SEQ<=cache.SEQ{
		dump("%s hit cache %s", kv.String(), args.String())
		res<-cache.Value
		return res
	}else if index, _, ok:=kv.rf.Start(args); ok{
		dump("%s start %s, waiting %d", kv.String(), args.String(), index)
		kv.notif[index]=res
		return res
	}
	return nil
}

func (kv *KVServer) checkAndApply(index int, args Args){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.dead==1{ return }
	var value string
	if cache, ok:=kv.cache[args.UID]; ok && args.SEQ<=cache.SEQ{
		value=cache.Value
		dump("%s hit cache %s, len(notif)=%d", kv.String(), args.String(), len(kv.notif))
	}else{
		switch args.Type{
		case _GET:		value=kv.db[args.Key]
		case _PUT:		kv.db[args.Key]=args.Value
		case _APPEND:	kv.db[args.Key]+=args.Value
		}
		kv.cache[args.UID]=_Cache{
			SEQ:	args.SEQ,
			Value:	value,
		}
		dump("%s apply %s, len(notif)=%d", kv.String(), args.String(), len(kv.notif))
	}
	if ch, ok:=kv.notif[index]; ok{
		ch<-value
		delete(kv.notif, index)		
		dump("%s notify %d", kv.String(), index)
	}
}



