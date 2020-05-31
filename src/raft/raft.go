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
	return fmt.Sprintf("(%d:%s:%d)", rf.me, rf.state, rf.term)
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
	return -1, false
}
// Start try to send applyMsg to this raft 
func (rf *Raft) Start(command interface{}) (int, int, bool){
	var term, isLeader=rf.GetState()
	return -1, term, isLeader
}
// Make a raft and return immediately
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	var rf=Raft{
		peers:			peers,
		persister:		persister,
		me:				me,
		electTimer:		time.NewTimer(randDuration(_ElectLower, _ElectUpper)),
		heartbeatTimer:	time.NewTimer(_HeartBeatInterval),
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
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state!=_Leader{ 
					dout("%s try to broadcast heartbeat", rf.String())
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
	switch s{
	case _Follower:
		rf.electTimer.Reset(randDuration(_ElectLower, _ElectUpper))
		rf.heartbeatTimer.Reset(_Infinite)
	case _Candidate:
		rf.term++
		rf.voteFor=rf.me
		rf.save()
		rf.sendRequestVotes()
		rf.electTimer.Reset(randDuration(_ElectLower, _ElectUpper))
		rf.heartbeatTimer.Reset(_Infinite)
	default:
		for i:=range rf.nextIndex { rf.nextIndex[i]=rf.log.size() }
		rf.sendAppendEntries()
		rf.electTimer.Reset(_Infinite)
		rf.heartbeatTimer.Reset(_HeartBeatInterval)
	}
	if s==_Candidate || rf.state!=s{
		dout("%s convert %s -> %s", rf.String(), rf.state, s)
		rf.state=s
	}
}
// should be called with lock
func (rf *Raft) save(){

}
// should be called with lock
func (rf *Raft) load(){

}
//should be called with lock
func (rf *Raft) applyEntries(){
	var tmp=rf.lastApply
	if rf.lastApply>rf.lastCommit{
		dout("%s lastApply(%d)>lastCommit(%d)", rf.String(), rf.lastApply, rf.lastCommit)
		os.Exit(1)
	}
	if rf.lastApply==rf.lastCommit { return }
	for rf.lastApply<rf.lastCommit {
		rf.lastApply++	
		rf.applyCh<-ApplyMsg {
			CommandValid:	true,
			Command:		rf.log.get(rf.lastApply).Command,
			CommandIndex:	rf.lastApply,
		}	
	}
	dout("%s apply entries [%d, %d)", rf.String(), tmp+1, rf.lastCommit+1)
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
		go func(i int, args *RequestVoteArgs, reply *RequestVoteReply){
			for !rf.peers[i].Call("Raft.RequestVote", args, reply){
				dout("%s -{request vote}-> %d fail", rf.String(), i)
			}
			dout("%s -{request vote}-> %d", rf.String(), i)
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
		go func(i int, args *AppendEntriesArgs, reply *AppendEntriesReply){
			for !rf.peers[i].Call("Raft.AppendEntries", args, reply){
				dout("%s -{entries [%d, %d)}-> %d fail", rf.String(), rf.nextIndex[i], rf.log.size(), i)
			}
			dout("%s -{entries [%d, %d)}-> %d", rf.String(), rf.nextIndex[i], rf.log.size(), i)
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
						rf.applyEntries()
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
				dout("%s desperate entries [%d, %d)", rf.String(), args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
			}else{
				var begin=max(0, rf.lastCommit-args.PrevLogIndex)
				rf.log.replaceFrom(rf.lastCommit+1, args.Entries[begin:])
				dout("%s append entries [%d, %d)", rf.String(), begin, rf.log.size())
				rf.matchIndex[args.LeaderID]=args.LeaderCommit
				rf.lastCommit=max(rf.lastCommit, args.LeaderCommit)
				rf.lastCommit=min(rf.lastCommit, rf.log.size())
				rf.applyEntries()
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
		dout("%s refuse %s", rf.String(), args.LeaderID)
	}
}