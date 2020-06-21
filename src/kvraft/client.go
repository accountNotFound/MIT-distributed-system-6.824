package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync"

const _RETRY_TIME_OUT=time.Millisecond*3000

var _GLOBAL_CLIENT_ID int64=0

// Clerk is public 
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me			int64
	seq			int
	leaderID	int

	// attention !!!!!
	//
	// it is very disappoint that servers[i] doesn't mean server.me==i
	// so you have to map serverID to corresponding clientEnd for leader redirection
	id2end		[]*labrpc.ClientEnd
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk and return immediately
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	_GLOBAL_CLIENT_ID++
	var ck=Clerk{
		servers:	servers,
		// me:			nrand(),
		me:			_GLOBAL_CLIENT_ID,
		seq:		0,
		leaderID:	int(nrand())%len(servers),
		id2end:		make([]*labrpc.ClientEnd, len(servers)),
	}
	dump("C(%d) start", ck.me)
	return &ck
}


// Get by given key, returns "" if the key does not exist.
func (ck *Clerk) Get(key string) string {
	return ck.sendRequest(_GET, key, "")
}
// Put value at given key
func (ck *Clerk) Put(key string, value string) {
	ck.sendRequest(_PUT, key, value)
}
// Append value at given key
func (ck *Clerk) Append(key string, value string) {
	ck.sendRequest(_APPEND, key, value)
}

func (ck *Clerk) sendRequest(opType _OpType, key, value string) string{
	ck.seq++
	for handlerName:="KVServer."+opType.String();;{
		var args=Args{
			Meta:		Meta{ck.me, ck.seq},
			Type:		opType,
			Key:		key,
			Value:		value,
		}
		var reply Reply
		var msg=make(chan bool, 1)
		go func(){ 
			dump("C(%d) send %v to S(%d)", ck.me, args.String(), ck.leaderID)
			msg<-ck.getServer(ck.leaderID).Call(handlerName, &args, &reply) 
		}()
		select{
		case <-time.After(_RETRY_TIME_OUT):
			dump("C(%d) retry", ck.me)
		case ok:=<-msg:
			if ok{ 
				dump("C(%d): %v exchange %v S(%d)", ck.me, args.String(), reply.String(), ck.leaderID)
				if reply.Redirect{
					if reply.LeaderID==-1{
						time.Sleep(time.Millisecond*500) // wait until election end
					}else{
						// time.Sleep(time.Millisecond*500)
						ck.leaderID=reply.LeaderID						
					}
				}else{
					// time.Sleep(time.Millisecond*500)
					dump("C(%d) return \"%s\"", ck.me, reply.Value)
					return reply.Value
				}
			}
		}
	}
}

func (ck *Clerk) getServer(i int) *labrpc.ClientEnd{
	if ck.id2end[i]==nil{
		var wg=sync.WaitGroup{}
		for i:=range ck.servers{
			wg.Add(1)
			go func(i int){
				defer wg.Done()
				var noArgs bool
				var reply=-1
				for reply==-1{ ck.servers[i].Call("KVServer.ID", &noArgs, &reply) }
				ck.id2end[reply]=ck.servers[i]
				// dump("C(%d) hello to %d: S(%d)", ck.me, i, reply)
			}(i)
		}
		wg.Wait()
	}
	return ck.id2end[i]
}