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

const useCompress=1 // 0 means not use log compress (for lab 3B)
const useDebug=0 	// 0 means no std detail output (mainly for debug), see util.dump()
const delay=1 		// adjust broadcast time and election time

const (
	_HEARTBEAT_INTERVAL		=time.Duration(120)*time.Millisecond*delay
	_ELECTION_LOWER			=time.Duration(300)*time.Millisecond*delay
	_ELECTION_UPPER			=time.Duration(450)*time.Millisecond*delay
)

// ApplyMsg is a pre-defined struct by framework, don't change it
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type _State uint8
const (
	_FOLLOWER	_State=iota
	_CANDIDATE
	_LEADER
)
func (s _State) String() string{
	switch s{
	case _FOLLOWER:		return "follower"
	case _CANDIDATE:	return "candidate"
	default:			return "leader"
	}
}

type _Entry struct{
	Term		int
	Command		interface{}
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

	//2A below
	electTimer		*time.Timer
	heartbeatTimer	*time.Timer
	state			_State
	currentTerm		int
	votedFor		int
	log				[]_Entry

	//2B below
	lastCommit	int
	lastApply	int
	nextIndex	[]int
	matchIndex	[]int		
	applyCh		chan ApplyMsg	

	//2C below
	useSave		bool // for efficiency, don't call save() directly, just set useSave = true

	// 3A below
	currentLeader	int

	// 3B below, in lab2 this will be simply set as zero and no change
	// absolute log size = len(log)+lastSnapshot
	lastSnapshot	int
}

// GetLeader of current term
// for lab3A leader redirection
func (rf *Raft) GetLeader() int{
	rf.sync()
	defer rf.unsync()
	var leader=rf.currentLeader
	return leader
}

// implementation of some pre-define API below

// Kill this raft
func (rf *Raft) Kill(){
	rf.sync()
	rf.dead=1
	rf.useSave=true
	dump("%s be killed", rf.String())
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
	var term=rf.currentTerm
	var isleader=rf.state==_LEADER
	return term, isleader
}
// Start try to send applyMsg to this raft 
func (rf *Raft) Start(command interface{}) (int, int, bool){
	rf.sync()
	defer rf.unsync()
	var term=rf.currentTerm
	var isleader=rf.state==_LEADER
	var index=rf.logSize()
	if isleader{
		rf.appendLog(_Entry{
			Command:	command,
			Term:		rf.currentTerm,
		})
		rf.matchIndex[rf.me]=rf.logSize()-1
		rf.nextIndex[rf.me]=rf.logSize()
		dump("--------user start one(%v) to %s---------", rf.logLast().Command, rf.String())
		rf.checkLeaderAndbroadcast()
		// rf.heartbeatTimer.Reset(0)
		// don't broadcast immediately if the network environment is too bad, or there may be some bug
	}
	return index, term, isleader
}
// Make a raft and return immediately
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	var rf=Raft{
		peers:			peers,
		persister:		persister,
		me:				me,
		state:			_FOLLOWER,
		currentTerm:	0,
		votedFor:		-1,
		lastCommit:		0,
		lastApply:		0,
		nextIndex:		make([]int, len(peers)),
		matchIndex:		make([]int, len(peers)),
		log:			[]_Entry{{0, nil}},
		applyCh:		applyCh,
		useSave:		false,
		lastSnapshot:	0,
		currentLeader:	-1,
	}
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.load()
	for i:=range peers{ 
		rf.nextIndex[i]=rf.logSize() 
		rf.matchIndex[i]=0
	}
	rf.electTimer=time.NewTimer(randDuration(_ELECTION_LOWER, _ELECTION_UPPER))
	rf.heartbeatTimer=time.NewTimer(_HEARTBEAT_INTERVAL)	
	dump("%s start", rf.String())
	go func(){
		for{
			select{
			case <-rf.electTimer.C:
				rf.sync()
				rf.toCandidateAndstartElection()
				rf.unsync()
			case <-rf.heartbeatTimer.C:
				rf.sync()
				rf.checkLeaderAndbroadcast()
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


// all below functions should be called with lock

// saver and loader for persistence
func (rf *Raft) save(){
	// save raft state
	var w=new(bytes.Buffer)
	var e=labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastCommit)
	e.Encode(rf.lastSnapshot)
	rf.persister.SaveRaftState(w.Bytes())
}
func (rf *Raft) load(){
	// load raft state
	var r=bytes.NewBuffer(rf.persister.ReadRaftState())
	var d=labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastCommit)
	d.Decode(&rf.lastSnapshot)
}

// some getter
func (rf *Raft) String() string{
	return fmt.Sprintf("%s(%d,%d,%d,%d,%d,%v)", 
					rf.state.String(), rf.me, rf.currentTerm, rf.lastSnapshot, rf.lastApply, rf.lastCommit, rf.matchIndex)
}
func (rf *Raft) logAt(index int) _Entry{
	return rf.log[index-rf.lastSnapshot]
}
func (rf *Raft) logLast() _Entry{
	return rf.log[len(rf.log)-1]
}
func (rf *Raft) logSize() int{
	return rf.lastSnapshot+len(rf.log)
}
func (rf *Raft) logCopy(begin, end int) []_Entry{
	var res=make([]_Entry, end-begin)
	copy(res, rf.log[begin-rf.lastSnapshot: end-rf.lastSnapshot])
	return res
}

// some setter
func (rf *Raft) appendLog(entries ..._Entry){
	rf.log=append(rf.log, entries...)
	rf.useSave=len(entries)>0
}
func (rf *Raft) truncateLog(last int){
	if last<rf.logSize()-1{
		rf.log=rf.log[0: last+1-rf.lastSnapshot]
		rf.useSave=true
	}
}
func (rf *Raft) setTermAndConvert(term int, state _State){
	var prevState=rf.String()
	rf.currentTerm=term
	rf.state=state	
	dump("%s convert %s", prevState, rf.String())	
	switch state{
	case _FOLLOWER:
		rf.votedFor=-1
		rf.useSave=true
		rf.electTimer.Reset(randDuration(_ELECTION_LOWER, _ELECTION_UPPER)) // prepare for next election
	case _CANDIDATE:
		rf.votedFor=rf.me
		rf.useSave=true
		rf.electTimer.Reset(randDuration(_ELECTION_LOWER, _ELECTION_UPPER))	// prepare for next election
	default:
		rf.currentLeader=rf.me
		for i:=range rf.peers{ 
			rf.matchIndex[i]=rf.lastSnapshot
			rf.nextIndex[i]=rf.logSize() 
		}
		rf.checkLeaderAndbroadcast()
		// rf.heartbeatTimer.Reset(0)	// start heartbeat broadcast immediately
		// it is the only case that raft convert to leader when election timer time out
	}
}
func (rf *Raft) setVote(vote int){
	rf.useSave=vote!=rf.votedFor
	rf.votedFor=vote
}
func (rf *Raft) setCommitAndApply(commit int){
	rf.lastCommit=min(rf.logSize()-1, max(rf.lastCommit, commit))
	if rf.lastApply==rf.lastCommit{ return }
	rf.useSave=true
	rf.lastApply=max(rf.lastApply, rf.lastSnapshot)	// no necessary to apply log before snapshot
	var first=rf.lastApply+1
	for rf.lastApply<rf.lastCommit{
		rf.lastApply++
		rf.applyCh<-ApplyMsg {
			CommandValid:	true,
			Command:		rf.logAt(rf.lastApply).Command,
			CommandIndex:	rf.lastApply,
		}	
	}
	dump("%s apply [%d:%d)", rf.String(), first, rf.lastCommit+1)	
}
func (rf *Raft) setSnapshotAndCompress(snapshot int){
	if useCompress==0{ return }
	var prevLastSnapshot=rf.lastSnapshot
	rf.lastSnapshot=min(rf.logSize()-1, max(rf.lastSnapshot, snapshot))
	if rf.lastSnapshot==prevLastSnapshot{ return }
	rf.log=rf.log[rf.lastSnapshot-prevLastSnapshot: ]
	dump("%s snapshot [%d:%d)", rf.String(), prevLastSnapshot+1, rf.lastSnapshot+1)
}



// main entry of state machine below, need to aquire lock previously

func (rf *Raft) toCandidateAndstartElection(){
	if rf.state==_LEADER{ return }
	rf.setTermAndConvert(rf.currentTerm+1, _CANDIDATE)

	var voteCnt=1

	for i:=range rf.peers{
		if i==rf.me{ continue }
		var args=RequestVoteArgs{
			Term:			rf.currentTerm,
			CandidateID:	rf.me,
			LastLogTerm:	rf.logLast().Term,
			LastLogIndex:	rf.logSize()-1,
		}
		var reply RequestVoteReply

		go func(i int, args *RequestVoteArgs, reply *RequestVoteReply){
			// dealing with out-of-date request
			rf.sync()
			var currentTerm=rf.currentTerm
			// var currentRaftInfo=rf.String()
			rf.unsync()
			if currentTerm!=args.Term{ return }

			// send request
			// dump("%s -%v-> node(%d)...", currentRaftInfo, *args, i)
			if !rf.peers[i].Call("Raft.RequestVote", args, reply){
				// dump("%s -%v-> node(%d) fail", currentRaftInfo, *args, i)
				return 
			}

			rf.sync()
			defer rf.unsync()


			// dealing with out-of-date reply, simply return
			if rf.currentTerm>reply.Term{ return }
			// if rf.state!=_CANDIDATE || rf.currentTerm>reply.Term || rf.currentTerm>args.Term{ return }
			
			
			// should always dealing with term first
			if rf.currentTerm<reply.Term{ rf.setTermAndConvert(reply.Term, _FOLLOWER) }
			if rf.state!=_CANDIDATE{ return }
			// dealing with RequestVote affair

			dump("%s %v exchange %v node(%d)", rf.String(), *args, *reply, i)
			if reply.VoteGaruantee{
				voteCnt++
				if voteCnt>len(rf.peers)/2{ 
					rf.setTermAndConvert(rf.currentTerm, _LEADER) 
				}
			}
		}(i, &args, &reply)
	}
}

func (rf *Raft) checkLeaderAndbroadcast(){
	if rf.state!=_LEADER{ return }
	rf.heartbeatTimer.Reset(_HEARTBEAT_INTERVAL)
	for i:=range rf.peers{
		if i==rf.me{ continue }
		var args=AppendEntriesArgs{
			Term:			rf.currentTerm,			
			LeaderID:		rf.me,
			PrevLogIndex:	rf.nextIndex[i]-1,		
			PrevLogTerm:	rf.logAt(rf.nextIndex[i]-1).Term,
			Entries:		rf.logCopy(rf.nextIndex[i], rf.logSize()),
			LeaderCommit:	rf.lastCommit,
			LeaderSnapshot: rf.lastSnapshot,
		}
		var reply AppendEntriesReply

		go func(i int, args *AppendEntriesArgs, reply *AppendEntriesReply){
			// dealing with out-of-date request
			rf.sync()
			var currentTerm=rf.currentTerm
			// var currentRaftInfo=rf.String()
			rf.unsync()
			if currentTerm!=args.Term{ return }

			// send request
			// dump("%s -%v-> node(%d)...", currentRaftInfo, args.String(), i)
			if !rf.peers[i].Call("Raft.AppendEntries", args, reply){
				// dump("%s -%v-> node(%d) fail", currentRaftInfo, args.String(), i)
				return 
			}

			rf.sync()
			defer rf.unsync()

		
			// dealing with out-of-date reply, simply return
			if rf.currentTerm>reply.Term{ return }
			// if rf.state!=_LEADER || rf.currentTerm>reply.Term || rf.currentTerm>args.Term{ return }

			
			// should always dealing with term first
			if rf.currentTerm<reply.Term{ rf.setTermAndConvert(reply.Term, _FOLLOWER) }
			if rf.state!=_LEADER{ return }
			// dealing with AppendEntries affair 
			if reply.Success{
				// another out-of-date check
				if rf.matchIndex[i]>args.PrevLogIndex+len(args.Entries){ return }

				rf.matchIndex[i]=args.PrevLogIndex+len(args.Entries)
				rf.nextIndex[i]=rf.matchIndex[i]+1
				rf.setCommitAndApply(mediate(rf.matchIndex))
				rf.setSnapshotAndCompress(minimum(rf.matchIndex))
			}else if reply.ExpectNext!=-1{	
				rf.nextIndex[i]=max(reply.ExpectNext, rf.matchIndex[i]+1)
			}
			dump("%s %v exchange %v node(%d)", rf.String(), args.String(), *reply, i)
		}(i, &args, &reply)
	}
}


// all below functions are RPC handler

// RequestVote for certain candidate
// rpc by candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	rf.sync()
	defer rf.unsync()
	if rf.currentTerm>args.Term{
		goto REFUSE
	}
	if rf.currentTerm<args.Term{ rf.setTermAndConvert(args.Term, _FOLLOWER) }
	if rf.state==_FOLLOWER && (
	args.LastLogTerm>rf.logLast().Term ||
	args.LastLogTerm==rf.logLast().Term && args.LastLogIndex>=rf.logSize() ||
	args.LastLogTerm==rf.logLast().Term && args.LastLogIndex==rf.logSize()-1 && (rf.votedFor==-1||rf.votedFor==args.CandidateID)){
		rf.electTimer.Reset(randDuration(_ELECTION_LOWER, _ELECTION_UPPER))	// prepare for next election		
		rf.setVote(args.CandidateID)
		reply.Term, reply.VoteGaruantee=rf.currentTerm, true
		// dump("candidate(%d,%d) <-%v- %s (vote)", args.CandidateID, args.Term, *reply, rf.String())
		return
	}

REFUSE:
	reply.Term, reply.VoteGaruantee=rf.currentTerm, false
	// dump("candidate(%d,%d) <-%v- %s (refuse)", args.CandidateID, args.Term, *reply, rf.String())
}
// AppendEntries to this raft
// rpc by leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.sync()
	defer rf.unsync()
	if rf.currentTerm>args.Term{
		goto REFUSE
	}
	if rf.currentTerm<args.Term || rf.state!=_FOLLOWER{ rf.setTermAndConvert(args.Term, _FOLLOWER) }
	if rf.state!=_FOLLOWER{
		goto REFUSE
	}
	rf.electTimer.Reset(randDuration(_ELECTION_LOWER, _ELECTION_UPPER))	// prepare for next election
	rf.currentLeader=args.LeaderID

	if args.PrevLogIndex>=rf.logSize(){
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, false, rf.logSize()
		return
	}else if args.PrevLogIndex<=rf.lastCommit || rf.logAt(args.PrevLogIndex).Term==args.PrevLogTerm{
		// don't simply truncate log at args.PrevLogIndex, 
		// otherwise it may fail when args arrive without order
		var raftIndex=max(rf.lastCommit, args.PrevLogIndex)+1
		var argsIndex=max(rf.lastCommit, args.PrevLogIndex)-args.PrevLogIndex

		for argsIndex<len(args.Entries) && 
		raftIndex<rf.logSize() && 
		rf.logAt(raftIndex).Term==args.Entries[argsIndex].Term{ 
			raftIndex++
			argsIndex++
		}
		if argsIndex<len(args.Entries){
			// only truncate log when there is excatly conflict
			rf.truncateLog(raftIndex-1)
			rf.appendLog(args.Entries[argsIndex: ]...)		
		}
		rf.setCommitAndApply(args.LeaderCommit)
		rf.setSnapshotAndCompress(args.LeaderSnapshot)			
		// leader won't use reply.ExpectNext if reply.Success is true
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, true, args.PrevLogIndex+len(args.Entries)+1
		return
	}else{
		// this is not the log backup optimization mention in raft paper, but it does work
		// actually you can set any value to reply.ExpectNext since the leader will truncate it if necessary
		var prev=args.PrevLogIndex-1
		var term=rf.logAt(args.PrevLogIndex).Term
		for prev>rf.lastCommit && rf.logAt(prev).Term==term{ prev-- }
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, false, prev+1		
		return
	}
REFUSE:
	reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, false, -1
}