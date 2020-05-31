package raft


import (
	"time"
	"fmt"
	"sync"
	"sync/atomic"
	"../labrpc"
	"os"
)

const (
	_HeartBeatInterval	=time.Duration(120)*time.Millisecond
	_ElectLower			=time.Duration(350)*time.Millisecond
	_ElectUpper			=time.Duration(500)*time.Millisecond
	_Infinite			=time.Duration(10)*time.Second	
)

// ApplyMsg is a pre-defined struct by framework, don't change it
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type tState uint8
const (
	_Follower	tState=iota
	_Candidate
	_Leader
)
func (s tState) String() string{
	switch s{
	case _Follower:		return "follower"
	case _Candidate:	return "candidate"
	default:			return "leader"
	}
}



// Raft is a pre-defined struct by framework
type Raft struct {
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electTimer		*time.Timer
	heartbeatTimer	*time.Timer

	state		tState
	term		int
	voteFor		int

	lastCommit	int
	lastApply	int
	nextIndex	[]int
	matchIndex	[]int

	log			*tLog
	applyCh		chan ApplyMsg	
}
// read only, no need to lock
func (rf *Raft) String() string{
	return fmt.Sprintf("(%d:%s:%d) nexts: %v, matches: %v, lastCommit: %d", rf.me, rf.state, rf.term, rf.nextIndex, rf.matchIndex, rf.lastCommit)
}
// Kill this raft
func (rf *Raft) Kill(){
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}
func (rf *Raft) killed() bool {
	var z=atomic.LoadInt32(&rf.dead)
	return z==1
}
// GetState return this raft's term and whether it is leader
func (rf *Raft) GetState() (int, bool){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term=rf.term
	var isleader=rf.state==_Leader
	return term, isleader
}
// Start try to send applyMsg to this raft 
func (rf *Raft) Start(command interface{}) (int, int, bool){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.save()
	var term=rf.term
	var isleader=rf.state==_Leader
	var index=rf.log.size()
	if isleader{
		rf.log.put(tEntry{
			Command:	command,
			Term:		rf.term,
		})
		rf.matchIndex[rf.me]=rf.log.size()-1
		rf.nextIndex[rf.me]=rf.log.size()
		dout("--------user append one(%v)---------", rf.log.back().Command)
	}
	return index, term, isleader
}
// Make a raft and return immediately
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	var rf=Raft{
		peers:			peers,
		persister:		persister,
		me:				me,
		state:			_Follower,
		term:			0,
		voteFor:		-1,
		lastCommit:		0,
		lastApply:		0,
		nextIndex:		make([]int, len(peers)),
		matchIndex:		make([]int, len(peers)),
		log:			makeLog(),
		applyCh:		applyCh,
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.load()
	for i:=range peers{ 
		rf.nextIndex[i]=rf.log.size() 
		rf.matchIndex[i]=0
	}
	rf.electTimer=time.NewTimer(randDuration(_ElectLower, _ElectUpper))
	rf.heartbeatTimer=time.NewTimer(_Infinite)	
	dout("%s start", rf.String())
	go func(){
		for{
			select{
			case <-rf.electTimer.C:
				rf.mu.Lock()
				if rf.state==_Leader{ 
					dout("%s try to elect", rf.String())
					os.Exit(1)
				}
				rf.convertTo(_Candidate)
				rf.save()
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state!=_Leader{ 
					dout("%s try to heartbead", rf.String())
					os.Exit(1)					
				 }
				rf.convertTo(_Leader)			
				rf.mu.Unlock()			
			}			
		}
	}()
	return &rf
}

// should be called with lock
func (rf *Raft) convertTo(s tState){
	if s==_Candidate || rf.state!=s{
		dout("%s convert %s -> %s", rf.String(), rf.state, s)
		rf.state=s		
	}	
	switch s{
	case _Follower:
		rf.electTimer.Reset(randDuration(_ElectLower, _ElectUpper))
		rf.heartbeatTimer.Reset(_Infinite)
	case _Candidate:
		rf.term++
		rf.voteFor=rf.me
		rf.sendRequestVotes()
		rf.electTimer.Reset(randDuration(_ElectLower, _ElectUpper))
		rf.heartbeatTimer.Reset(_Infinite)
	default:
		if rf.state!=_Leader{
			for i:=range rf.nextIndex { rf.nextIndex[i]=rf.log.size() }			
		}
		rf.sendAppendEntries()
		rf.electTimer.Reset(_Infinite)
		rf.heartbeatTimer.Reset(_HeartBeatInterval)
	}

}
// should be called with lock
func (rf *Raft) save(){

}
// should be called with lock
func (rf *Raft) load(){

}
// should be called with lock
func (rf *Raft) sendRequestVotes(){
	var voteCnt int32=1
	for i:=range rf.peers{
		if i==rf.me{ continue }
		var args=RequestVoteArgs{
			Term:			rf.term,
			CandidateID:	rf.me,
			LastLogTerm:	rf.log.back().Term,
			LastLogIndex:	rf.log.size()-1,
		}
		var reply RequestVoteReply
		dout("%s -{request vote}-> %d", rf.String(), i)	
		go func(i int, args *RequestVoteArgs, reply *RequestVoteReply){			
			var ok=rf.peers[i].Call("Raft.RequestVote", args, reply)
			if !ok{
				dout("%s -{request vote}-> %d fail", rf.String(), i)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term>rf.term{
				rf.term=reply.Term				
				rf.voteFor=-1				
				rf.convertTo(_Follower)
				rf.save()
			}else if rf.state==_Candidate && reply.Success{
				atomic.AddInt32(&voteCnt, 1)
				if atomic.LoadInt32(&voteCnt)>int32(len(rf.peers)/2) {
					rf.convertTo(_Leader)
					rf.save()
				}				
			}
		}(i, &args, &reply)
	}
}
// should be called with lock
func (rf *Raft) sendAppendEntries(){
	for i:=range rf.peers{
		if i==rf.me{ continue }
		var args=AppendEntriesArgs{
			Term:			rf.term,			
			LeaderID:		rf.me,		
			PrevLogIndex:	rf.nextIndex[i]-1,		
			PrevLogTerm:	rf.log.get(rf.nextIndex[i]-1).Term,			
			Entries:		rf.log.copyRange(rf.nextIndex[i], rf.log.size()),
			LeaderCommit:	rf.lastCommit,
		}
		var reply AppendEntriesReply
		dout("%s -{entries [%d, %d)}-> %d", rf.String(), rf.nextIndex[i], rf.log.size(), i)	
		go func(i int, args *AppendEntriesArgs, reply *AppendEntriesReply){			
			var ok=rf.peers[i].Call("Raft.AppendEntries", args, reply)
			if !ok{
				dout("%s -{entries [%d, %d)}-> %d fail", rf.String(), rf.nextIndex[i], rf.log.size(), i)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term>rf.term{
				rf.term=reply.Term				
				rf.voteFor=-1				
				rf.convertTo(_Follower)
				rf.save()
			}else if rf.state==_Leader && reply.Success{
				if reply.ExpectNext==-1{
					rf.matchIndex[i]=args.PrevLogIndex+len(args.Entries)
					rf.nextIndex[i]=rf.matchIndex[i]+1
					var min, mid=minAndMediate(rf.matchIndex)
					rf.lastCommit=max(rf.lastCommit, mid)
					if rf.lastApply<rf.lastCommit{
						go rf.applyEntries()
						rf.log.compressUntil(min+1)
						rf.save()					
					}						
				}else{
					rf.nextIndex[i]=reply.ExpectNext
				}
				
			}
		}(i, &args, &reply)
	}
}

// RequestVote for certain candidate
// rpc by candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term>rf.term ||
	args.Term==rf.term && args.LastLogIndex>rf.log.size()-1 ||
	args.Term==rf.term && args.LastLogIndex==rf.log.size()-1 && args.LastLogTerm>rf.log.back().Term ||
	args.Term==rf.term && args.LastLogIndex==rf.log.size()-1 && args.LastLogTerm==rf.log.back().Term && (rf.voteFor==-1 || rf.voteFor==args.CandidateID){
		defer rf.save()
		rf.term=args.Term				
		rf.voteFor=args.CandidateID		
		rf.convertTo(_Follower)
		*reply=RequestVoteReply{ 
			Term:		rf.term, 
			Success:	true,
		}
		dout("%s -{vote}-> %d", rf.String(), args.CandidateID)
	}else{
		*reply=RequestVoteReply{ 
			Term:		rf.term, 
			Success:	false, 
		}
		dout("%s -{vote}-> %d refuse", rf.String(), args.CandidateID)
	}
}
// AppendEntries to this raft
// rpc by leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term>=rf.term{
		defer rf.save()
		rf.term=args.Term				
		rf.convertTo(_Follower)			
		if args.PrevLogIndex<=rf.log.size()-1 && args.PrevLogTerm==rf.log.get(args.PrevLogIndex).Term{
			if args.PrevLogIndex+len(args.Entries)<=rf.lastCommit{
				dout("%s desperate entries [%d, %d)", rf.String(), args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries)+1)
			}else{
				var replaceBegin=max(rf.lastCommit+1, args.PrevLogIndex+1)
				var appendBegin=max(0, rf.lastCommit-args.PrevLogIndex)
				rf.log.replaceFrom(replaceBegin, args.Entries[appendBegin:])
				dout("%s append entries [%d, %d)", rf.String(), replaceBegin, args.PrevLogIndex+1+len(args.Entries))
				rf.matchIndex[args.LeaderID]=args.LeaderCommit
				rf.lastCommit=max(rf.lastCommit, args.LeaderCommit)
				rf.lastCommit=min(rf.lastCommit, rf.log.size())
				go rf.applyEntries()
				var m, _=minAndMediate(rf.matchIndex)
				rf.log.compressUntil(m+1)
			}
			*reply=AppendEntriesReply{
				Term:		rf.term,
				Success:	true,
				ExpectNext:	-1,
			}
		}else if args.PrevLogIndex<=rf.log.size()-1{
			// try to get correct expect next index
			*reply=AppendEntriesReply{
				Term:		rf.term,
				Success:	true,
				ExpectNext:	args.PrevLogIndex-1,
			}			
		}else{ // args.PrevLogIndex>rf.log.size()-1
			*reply=AppendEntriesReply{
				Term:		rf.term,
				Success:	true,
				ExpectNext: rf.log.size(),
			}
			dout("%s expect from %d", rf.String(), reply.ExpectNext)			
		}
	}else{
		*reply=AppendEntriesReply{
			Term:		rf.term,
			Success:	false,
			ExpectNext: -1,
		}
		dout("%s refuse %d", rf.String(), args.LeaderID)
	}
}

// should be called with a go runtine, since command application may time cost
// no need to lock, since rf.lastCommit only increase
func (rf *Raft) applyEntries(){
	var tmp=rf.lastApply
	for {
		rf.mu.Lock()
		if rf.lastApply>rf.lastCommit{
			dout("%s lastApply(%d)>lastCommit(%d)", rf.String(), rf.lastApply, rf.lastCommit)
			os.Exit(1)
		}		
		if rf.lastApply==rf.lastCommit { break }
		rf.lastApply++	
		rf.applyCh<-ApplyMsg {
			CommandValid:	true,
			Command:		rf.log.get(rf.lastApply).Command,
			CommandIndex:	rf.lastApply,
		}
		rf.mu.Unlock()	
	}
	if tmp<rf.lastCommit{
		dout("%s apply entries [%d, %d)", rf.String(), tmp+1, rf.lastCommit+1)		
	}
}