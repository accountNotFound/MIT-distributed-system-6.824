package raft

import (
	"time"
	"fmt"
	"sync"
	"sync/atomic"
	"../labrpc"
	"bytes"
	"../labgob"
)

const _UseDebug=0 // 0 means no std output
const _Delay=1

const (
	_HeartBeatInterval	=time.Duration(120)*time.Millisecond*_Delay
	_ElectLower			=time.Duration(350)*time.Millisecond*_Delay
	_ElectUpper			=time.Duration(500)*time.Millisecond*_Delay
	_Infinite			=time.Duration(10)*time.Second*_Delay
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

	log			tLog
	applyCh		chan ApplyMsg	

	useSave		bool
}
// read only, no need to lock
func (rf *Raft) String() string{
	// return fmt.Sprintf("(%d:%s:%d) nexts: %v, matches: %v, lastCommit: %d", rf.me, rf.state, rf.term, rf.nextIndex, rf.matchIndex, rf.lastCommit)
	return fmt.Sprintf("(%d,%s,%d,%d,%d)[%d:%v]", rf.me, rf.state, rf.term, rf.lastApply, rf.lastCommit, rf.log.size()-1, rf.log.last())
}
// Kill this raft
func (rf *Raft) Kill(){
	rf.sync()
	atomic.StoreInt32(&rf.dead, 1)
	rf.useSave=true
	dout("%s be killed", rf.String())
	rf.unsync()
	// Your code here, if desired.
}
func (rf *Raft) killed() bool {
	var z=atomic.LoadInt32(&rf.dead)
	return z==1
}
// GetState return this raft's term and whether it is leader
func (rf *Raft) GetState() (int, bool){
	rf.sync()
	defer rf.unsync()
	var term=rf.term
	var isleader=rf.state==_Leader
	return term, isleader
}
// Start try to send applyMsg to this raft 
func (rf *Raft) Start(command interface{}) (int, int, bool){
	rf.sync()
	defer rf.unsync()
	var term=rf.term
	var isleader=rf.state==_Leader
	var index=rf.log.size()
	if isleader{
		rf.log.append(tEntry{
			Command:	command,
			Term:		rf.term,
		})
		rf.matchIndex[rf.me]=rf.log.size()-1
		rf.nextIndex[rf.me]=rf.log.size()
		rf.useSave=true
		dout("--------user append one(%v)---------", rf.log.last().Command)
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
		log:			tLog{ 0, []tEntry{{0, nil}} },
		applyCh:		applyCh,
		useSave:		false,
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
				rf.sync()
				// if rf.state==_Leader{ 
				// 	dout("%s try to elect", rf.String())
				// 	os.Exit(1)
				//  }
				rf.convertTo(_Candidate)
				rf.unsync()
			case <-rf.heartbeatTimer.C:
				rf.sync()
				// if rf.state!=_Leader{ 
				// 	dout("%s try to heartbeat", rf.String())
				// 	os.Exit(1)
				//  }			
				 rf.convertTo(_Leader)
				rf.unsync()			
			}			
		}
	}()
	return &rf
}

// self define functions below
func (rf *Raft) sync(){
	rf.mu.Lock()
}
func (rf *Raft) unsync(){
	if rf.useSave{ 
		rf.save()
		rf.useSave=false 
	}
	rf.mu.Unlock()
}
// should be called with lock
func (rf *Raft) save(){
	// save raft state
	var w=new(bytes.Buffer)
	var e=labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.log.offset)
	e.Encode(rf.log.slice)
	// e.Encode(rf.lastApply)
	e.Encode(rf.lastCommit)
	rf.persister.SaveRaftState(w.Bytes())
	dout("%s save [%d:%d)", rf.String(), rf.lastCommit, rf.log.size())
}
// should be called with lock
func (rf *Raft) load(){
	// load raft state
	var r=bytes.NewBuffer(rf.persister.ReadRaftState())
	var d=labgob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log.offset)
	d.Decode(&rf.log.slice)
	// d.Decode(&rf.lastApply)
	d.Decode(&rf.lastCommit)
	dout("%s load [%d:%d)", rf.String(), rf.lastCommit, rf.log.size())
}
// should be called with lock
func (rf *Raft) convertTo(s tState){
	if s==_Candidate || rf.state!=s{
		dout("%s --> %s", rf.String(), s)	
	}	
	switch s{
	case _Follower:
		rf.electTimer.Reset(randDuration(_ElectLower, _ElectUpper))
		rf.heartbeatTimer.Reset(_Infinite)
	case _Candidate:
		rf.term++
		rf.voteFor=rf.me
		rf.useSave=true
		rf.startElection()
		rf.electTimer.Reset(randDuration(_ElectLower, _ElectUpper))
		rf.heartbeatTimer.Reset(_Infinite)
	default:
		if rf.state!=_Leader{
			for i:=range rf.nextIndex{
				rf.nextIndex[i]=rf.log.size() 
				rf.matchIndex[i]=rf.lastCommit
			}			
		}
		rf.broadcast()
		rf.electTimer.Reset(_Infinite)
		rf.heartbeatTimer.Reset(_HeartBeatInterval)
	}
	rf.state=s
}
// should be called with lock
func (rf *Raft) apply(commit int){
	// rf.matchIndex[rf.me]=rf.lastCommit
	rf.lastCommit=min(rf.log.size()-1, max(rf.lastCommit, commit))
	if rf.lastApply==rf.lastCommit{ return }
	rf.useSave=true
	var first=rf.lastApply+1
	for rf.lastApply<rf.lastCommit{
		rf.lastApply++
		rf.applyCh<-ApplyMsg {
			CommandValid:	true,
			Command:		rf.log.get(rf.lastApply).Command,
			CommandIndex:	rf.lastApply,
		}	
	}
	dout("%s apply [%d:%d)", rf.String(), first, rf.lastCommit+1)	
}
// should be called with lock
func (rf *Raft) compress(persist int){
	// compress log
	// for i:=range rf.matchIndex{ rf.matchIndex[i]=max(rf.matchIndex[i], persist) }
	// if persist==rf.log.offset{ return }
	// rf.useSave=true
	// rf.log.keep(persist, rf.log.size())
	// rf.log.offset=persist	
	// dout("%s compress [0, %d)", rf.String(), persist+1)
}

// if you want to save the raft state, just set rf.useSave=true with rf.sync()
// when run rf.unsync(), it will check rf.useSave and finally run rf.save() (if rf.useSave==true)
// should be called with lock
func (rf *Raft) startElection(){
	var raft=rf.String()
	var voteCnt int32=1
	for i:=range rf.peers{
		if i==rf.me{ continue }
		var args=RequestVoteArgs{
			Term:			rf.term,
			CandidateID:	rf.me,
			LastLogTerm:	rf.log.last().Term,
			LastLogIndex:	rf.log.size()-1,
		}
		var reply RequestVoteReply
		dout("%s request vote from (%d,,)", raft, i)	
		go func(i int, args *RequestVoteArgs, reply *RequestVoteReply){			
			if !rf.peers[i].Call("Raft.RequestVote", args, reply){
				dout("%s request vote from (%d,,) fail", raft, i)
				return
			}
			rf.sync()
			defer rf.unsync()
			// dealing with out-of-date reply in lossy network, so does in broadcast
			if args.Term!=rf.term{ return }
			if rf.state!=_Candidate{ return }
			if reply.Term>rf.term{
				rf.convertTo(_Follower)
				rf.term=reply.Term
				rf.useSave=true
				return			
			}
			if reply.Success{
				atomic.AddInt32(&voteCnt, 1)
				if atomic.LoadInt32(&voteCnt)>int32(len(rf.peers)/2){
					rf.convertTo(_Leader)
				}				
			}
		}(i, &args, &reply)
	}
}
// should be called with lock
func (rf *Raft) broadcast(){
	var raft=rf.String()
	for i:=range rf.peers{
		rf.nextIndex[i]=max(rf.nextIndex[i], rf.log.offset+1)		
		if i==rf.me{ continue }
		var args=AppendEntriesArgs{
			Term:			rf.term,				
			PrevLogIndex:	rf.nextIndex[i]-1,		
			PrevLogTerm:	rf.log.get(rf.nextIndex[i]-1).Term,			
			Entries:		rf.log.copy(rf.nextIndex[i], rf.log.size()),
			LeaderID:		rf.me,	
			LeaderCommit:	rf.lastCommit,
			LeaderPersist:	minimum(rf.matchIndex),
		}
		var reply AppendEntriesReply
		dout("%s send [%d:%d) to (%d,,)", raft, rf.nextIndex[i], rf.log.size(), i)	
		go func(i int, args *AppendEntriesArgs, reply *AppendEntriesReply){			
			if !rf.peers[i].Call("Raft.AppendEntries", args, reply){
				dout("%s send [%d:%d) to (%d,,) fail", raft, rf.nextIndex[i], rf.log.size(), i)
				return
			}
			rf.sync()
			defer rf.unsync()
			//dealing with out-of-date reply in lossy network, so does in startElection
			if args.Term!=rf.term{ return }			
			if rf.state!=_Leader{ return }
			if reply.Term>rf.term{
				rf.convertTo(_Follower)
				rf.term=reply.Term	
				rf.useSave=true
				return			
			}
			if reply.Success{
				rf.matchIndex[i]=args.PrevLogIndex+len(args.Entries)
				rf.nextIndex[i]=rf.matchIndex[i]+1
				rf.apply(mediate(rf.matchIndex)) // try update and useSave lastCommit in apply()
				rf.compress(minimum(rf.matchIndex)) // try compress and useSave log
			}else{
				// if reply.ExpectNext==-1{
				// 	dout("%s send [%d,%d) refused by %d", raft, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), i)
				// 	os.Exit(1)
				// }
				rf.nextIndex[i]=reply.ExpectNext
			}
		}(i, &args, &reply)
	}
}

// RequestVote for certain candidate
// rpc by candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	rf.sync()
	defer rf.unsync()
	if args.Term<rf.term{
		// refuse to vote
		*reply=RequestVoteReply{ 
			Term:		rf.term, 
			Success:	false, 
		}
		dout("%s vote to (%d,,%d) refuse", rf.String(), args.CandidateID, args.Term)
		return			
	}
	if args.Term>rf.term{
		rf.convertTo(_Follower)
		rf.term=args.Term
		rf.voteFor=-1
		rf.useSave=true
	}
	if args.LastLogTerm>rf.log.last().Term ||
	args.LastLogTerm==rf.log.last().Term && args.LastLogIndex>rf.log.size()-1 ||
	args.LastLogTerm==rf.log.last().Term && args.LastLogIndex==rf.log.size()-1 && (rf.voteFor==-1 || rf.voteFor==args.CandidateID){
		// vote garuantee
		rf.convertTo(_Follower)
		if rf.voteFor==-1{ rf.useSave=true }
		rf.voteFor=args.CandidateID
		*reply=RequestVoteReply{ 
			Term:		rf.term, 
			Success:	true,
		}
		dout("%s vote to (%d,,%d)", rf.String(), args.CandidateID, args.Term)	
	}else{
		// refuse to vote
		*reply=RequestVoteReply{ 
			Term:		rf.term, 
			Success:	false, 
		}
		dout("%s vote to (%d,,%d) refuse", rf.String(), args.CandidateID, args.Term)			
	}
}
// AppendEntries to this raft
// rpc by leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.sync()
	defer rf.unsync()
	if args.Term<rf.term{
		*reply=AppendEntriesReply{
			Term:		rf.term,
			Success:	false,
			ExpectNext:	-1,
		}
		dout("%s [%d:%d) refuse [%d:...) from (%d,,%d)", 
					rf.String(), rf.lastCommit, rf.log.size(), args.PrevLogIndex+1, args.LeaderID, args.Term)	
		return 		
	}
	if args.Term>rf.term{
		rf.convertTo(_Follower)
		rf.term=args.Term
		rf.useSave=true
	}
	if args.PrevLogIndex<=rf.log.size()-1 && rf.log.get(args.PrevLogIndex).Term==args.PrevLogTerm{
		rf.convertTo(_Follower)
		var prev=max(args.PrevLogIndex, rf.lastCommit)
		var entries=args.Entries.copy(prev-args.PrevLogIndex, len(args.Entries))
		rf.log.keep(0, prev+1)
		dout("%s [%d:%d) append [%d:%d) from %d, expect [%d:...)", 
					rf.String(), rf.lastCommit, prev+1, prev+1, prev+1+len(entries), args.LeaderID, rf.log.size()+len(entries))
		rf.log.append(entries...)	
		rf.matchIndex[args.LeaderID]=args.LeaderCommit	// not necessary, but for effeciency
		rf.nextIndex[args.LeaderID]=args.LeaderCommit+1 // not necessary, but for effeciency
		rf.apply(args.LeaderCommit) // try update and useSave lastCommit in apply()
		rf.compress(args.LeaderPersist) // not complete
		*reply=AppendEntriesReply{
			Term:		rf.term,
			Success:	true,
			ExpectNext:	rf.log.size(),
		}
	}else if args.PrevLogIndex<=rf.log.size()-1{
		// un match, backup expect next index multi times in one rpc handler
		rf.convertTo(_Follower)
		var prev=max(args.PrevLogIndex, rf.lastCommit)
		for prev>rf.lastCommit && rf.log.get(prev).Term==rf.log.get(args.PrevLogIndex).Term{ prev-- }
		*reply=AppendEntriesReply{
			Term:		rf.term,
			Success:	false,
			ExpectNext:	prev+1,
		}
		dout("%s [%d:%d) unmatch [%d:...) from (%d,,%d), expect [%d:...)", 
					rf.String(), rf.lastCommit, rf.log.size(), args.PrevLogIndex, args.LeaderID, args.Term, reply.ExpectNext)
	}else{
		// un match
		rf.convertTo(_Follower)
		*reply=AppendEntriesReply{
			Term:		rf.term,
			Success:	false,
			ExpectNext:	rf.log.size(),
		}
		dout("%s [%d:%d) unmatch [%d:...) from (%d,,%d), expect [%d:...)", 
					rf.String(), rf.lastCommit, rf.log.size(), args.PrevLogIndex, args.LeaderID, args.Term, reply.ExpectNext)				
	}
}