package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"

const _RETRY_TIME_OUT=time.Millisecond*500

var _GLOBAL_CLIENT_ID =0

// Clerk is public 
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me			int
	seq			int
	leaderIndex	int
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
		servers:		servers,
		me:				_GLOBAL_CLIENT_ID,
		seq:			0,
		leaderIndex:	int(nrand())%len(servers),
	}
	dump("C(%d) start", ck.me)
	return &ck
}

func (ck *Clerk) String() string{
	return fmt.Sprintf("C(%d,%d)", ck.me, ck.leaderIndex)
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
	var args=Args{
		UID:	ck.me,
		SEQ:	ck.seq,
		Type:	opType,
		Key:	key,
		Value:	value,
	}
	for rpcname:="KVServer."+opType.String();;{
		var reply Reply
		if ck.servers[ck.leaderIndex].Call(rpcname, &args, &reply) && reply.Valid{
			dump("C(%d) %s exchange %s index(%d)", ck.me, args.String(), reply.String(), ck.leaderIndex)
			return reply.Value
		}
		ck.leaderIndex=(ck.leaderIndex+1)%len(ck.servers)
		time.Sleep(_RETRY_TIME_OUT)
	}
}
