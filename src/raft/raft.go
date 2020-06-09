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

const useDebug=0 // 0 means no std output, see util.dump()
const delay=1

const (
	heartbeatInterC		=time.Duration(120)*time.Millisecond*delay
	electLowerC			=time.Duration(350)*time.Millisecond*delay
	electUpperC			=time.Duration(500)*time.Millisecond*delay
)

// ApplyMsg is a pre-defined struct by framework, don't change it
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type stateT uint8
const (
	followerC	stateT=iota
	candidateC
	leaderC
)
func (s stateT) String() string{
	switch s{
	case followerC:		return "follower"
	case candidateC:	return "candidate"
	default:			return "leader"
	}
}

type entryT struct{
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
	state			stateT
	currentTerm		int
	votedFor		int
	log				[]entryT

	//2B below
	lastCommit	int
	lastApply	int
	nextIndex	[]int
	matchIndex	[]int		
	applyCh		chan ApplyMsg	

	//2C below
	useSave		bool

	//3B below, in lab2 this will be simply set as zero and no change
	// absolute log size = len(log)+lastSnapshot
	lastSnapshot	int
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
	var isleader=rf.state==leaderC
	return term, isleader
}
// Start try to send applyMsg to this raft 
func (rf *Raft) Start(command interface{}) (int, int, bool){
	rf.sync()
	defer rf.unsync()
	var term=rf.currentTerm
	var isleader=rf.state==leaderC
	var index=rf.logSize()
	if isleader{
		rf.appendLog(entryT{
			Command:	command,
			Term:		rf.currentTerm,
		})
		rf.matchIndex[rf.me]=rf.logSize()-1
		rf.nextIndex[rf.me]=rf.logSize()
		dump("--------user start one(%v) to %s---------", rf.logLast().Command, rf.String())
	}
	return index, term, isleader
}
// Make a raft and return immediately
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	var rf=Raft{
		peers:			peers,
		persister:		persister,
		me:				me,
		state:			followerC,
		currentTerm:	0,
		votedFor:		-1,
		lastCommit:		0,
		lastApply:		0,
		nextIndex:		make([]int, len(peers)),
		matchIndex:		make([]int, len(peers)),
		log:			[]entryT{{0, nil}},
		applyCh:		applyCh,
		useSave:		false,
		lastSnapshot:	0,
	}
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.load()
	for i:=range peers{ 
		rf.nextIndex[i]=rf.logSize() 
		rf.matchIndex[i]=0
	}
	rf.electTimer=time.NewTimer(randDuration(electLowerC, electUpperC))
	rf.heartbeatTimer=time.NewTimer(heartbeatInterC)	
	dump("%s start", rf.String())
	go func(){
		for{
			select{
			case <-rf.electTimer.C:
				rf.toCandidateAndstartElection()
			case <-rf.heartbeatTimer.C:
				rf.checkLeaderAndbroadcast()
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
	return fmt.Sprintf("%s(%d,%d,%d,%d,%d)", 
					rf.state.String(), rf.me, rf.currentTerm, rf.lastSnapshot, rf.lastApply, rf.lastCommit)
}
func (rf *Raft) logAt(absoluteIndex int) entryT{
	if absoluteIndex<rf.lastSnapshot{
		fmt.Printf("%s try get log[%d], but lastCommit=%d, lastSnapshot=%d, len(log)=%d", 
					rf.String(), absoluteIndex, rf.lastCommit, rf.lastSnapshot, len(rf.log))
	}
	return rf.log[absoluteIndex-rf.lastSnapshot]
}
func (rf *Raft) logLast() entryT{
	return rf.log[len(rf.log)-1]
}
func (rf *Raft) logSize() int{
	return rf.lastSnapshot+len(rf.log)
}
func (rf *Raft) logCopy(begin, end int) []entryT{
	var res=make([]entryT, end-begin)
	copy(res, rf.log[begin-rf.lastSnapshot: end-rf.lastSnapshot])
	return res
}

// some setter
func (rf *Raft) appendLog(entries ...entryT){
	rf.log=append(rf.log, entries...)
	rf.useSave=len(entries)>0
}
func (rf *Raft) truncateLog(last int){
	if last<rf.logSize()-1{
		rf.log=rf.log[0: last+1-rf.lastSnapshot]
		rf.useSave=true
	}
}
func (rf *Raft) setTermAndConvert(term int, state stateT){
	var prev=rf.String()
	switch state{
	case followerC:
		rf.votedFor=-1
		rf.useSave=true
		rf.electTimer.Reset(randDuration(electLowerC, electUpperC)) // prepare for next election
	case candidateC:
		rf.votedFor=rf.me
		rf.useSave=true
		rf.electTimer.Reset(randDuration(electLowerC, electUpperC))	// prepare for next election
	default:
		for i:=range rf.peers{ 
			rf.matchIndex[i]=rf.lastSnapshot
			rf.nextIndex[i]=rf.logSize() 
		}
		rf.heartbeatTimer.Reset(0)	// start heartbeat broadcast immediately
		// it is the only case that raft convert to leader when election timer time out
	}
	rf.currentTerm=term
	rf.state=state	
	dump("%s -> %s", prev, rf.String())
}
func (rf *Raft) setVote(vote int){
	rf.useSave=vote!=rf.votedFor
	rf.votedFor=vote
}
func (rf *Raft) setCommitAndApply(commit int){
	rf.lastCommit=min(rf.logSize()-1, max(rf.lastCommit, commit))
	if rf.lastApply==rf.lastCommit{ return }
	rf.useSave=true
	rf.lastApply=max(rf.lastApply, rf.lastSnapshot)
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
	var prevLastSnapshot=rf.lastSnapshot
	rf.lastSnapshot=min(rf.logSize()-1, max(rf.lastSnapshot, snapshot))
	if rf.lastSnapshot==prevLastSnapshot{ return }
	rf.log=rf.log[rf.lastSnapshot-prevLastSnapshot: ]
	dump("%s snapshot [%d:%d)", rf.String(), prevLastSnapshot+1, rf.lastSnapshot+1)
}



// main entry of state machine below, no need to aquire lock previously

func (rf *Raft) toCandidateAndstartElection(){
	rf.sync()
	defer rf.unsync()
	if rf.state==leaderC{ return }
	rf.setTermAndConvert(rf.currentTerm+1, candidateC)
	var voteCnt int32=1
	var prevRaftInfo=rf.String()
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
			var currentRaftInfo=rf.String()
			rf.unsync()
			if currentRaftInfo!=prevRaftInfo{ return }

			// send request
			dump("%s -%v-> node(%d)...", currentRaftInfo, *args, i)
			if !rf.peers[i].Call("Raft.RequestVote", args, reply){
				dump("%s -%v-> node(%d) fail", currentRaftInfo, *args, i)
				return 
			}

			rf.sync()
			defer rf.unsync()
			// should always dealing with term first
			if rf.currentTerm<reply.Term{ rf.setTermAndConvert(reply.Term, followerC) }

			// dealing with out-of-date reply, simply return
			if rf.String()!=currentRaftInfo{ return }
			if rf.state!=candidateC || rf.currentTerm>reply.Term || rf.currentTerm>args.Term{ return }

			// dealing with RequestVote affair
			if reply.VoteGaruantee{
				atomic.AddInt32(&voteCnt, 1)
				if atomic.LoadInt32(&voteCnt)>int32(len(rf.peers)/2){ 
					rf.setTermAndConvert(rf.currentTerm, leaderC) 
				}
			}
		}(i, &args, &reply)
	}
}

func (rf *Raft) checkLeaderAndbroadcast(){
	rf.sync()
	defer rf.unsync()
	if rf.state!=leaderC{ return }
	rf.heartbeatTimer.Reset(heartbeatInterC)
	var prevRaftInfo=rf.String()
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
			var currentRaftInfo=rf.String()
			rf.unsync()
			if currentRaftInfo!=prevRaftInfo{ return }

			// send request
			dump("%s -%v-> node(%d)...", currentRaftInfo, args.String(), i)
			if !rf.peers[i].Call("Raft.AppendEntries", args, reply){
				dump("%s -%v-> node(%d) fail", currentRaftInfo, args.String(), i)
				return 
			}

			rf.sync()
			defer rf.unsync()
			// should always dealing with term first
			if rf.currentTerm<reply.Term{ rf.setTermAndConvert(reply.Term, followerC) }

			// dealing with out-of-date reply, simply return
			if rf.String()!=currentRaftInfo{ return }
			if rf.state!=leaderC || rf.currentTerm>reply.Term || rf.currentTerm>args.Term{ return }

			// dealing with AppendEntries affair 
			if reply.Success{
				rf.matchIndex[i]=args.PrevLogIndex+len(args.Entries)
				rf.nextIndex[i]=rf.matchIndex[i]+1
				rf.setCommitAndApply(mediate(rf.matchIndex))
				rf.setSnapshotAndCompress(minimum(rf.matchIndex))
			}else if reply.ExpectNext!=-1{
				rf.nextIndex[i]=min(reply.ExpectNext, rf.logSize())				
			}
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
	if rf.currentTerm<args.Term{ rf.setTermAndConvert(args.Term, followerC) }
	if rf.state==followerC && (
	args.LastLogTerm>rf.logLast().Term ||
	args.LastLogTerm==rf.logLast().Term && args.LastLogIndex>=rf.logSize() ||
	args.LastLogTerm==rf.logLast().Term && args.LastLogIndex==rf.logSize()-1 && (rf.votedFor==-1||rf.votedFor==args.CandidateID)){
		rf.electTimer.Reset(randDuration(electLowerC, electUpperC))	// prepare for next election		
		rf.setVote(args.CandidateID)
		reply.Term, reply.VoteGaruantee=rf.currentTerm, true
		dump("candidate(%d,%d) <-%v- %s (vote)", args.CandidateID, args.Term, *reply, rf.String())
		return
	}
	REFUSE:
		reply.Term, reply.VoteGaruantee=rf.currentTerm, false
		dump("candidate(%d,%d) <-%v- %s (refuse)", args.CandidateID, args.Term, *reply, rf.String())
}
// AppendEntries to this raft
// rpc by leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.sync()
	defer rf.unsync()
	if rf.currentTerm>args.Term{
		goto REFUSE
	}
	if rf.currentTerm<args.Term || rf.state!=followerC{ rf.setTermAndConvert(args.Term, followerC) }
	if rf.state!=followerC{
		goto REFUSE
	}
	rf.electTimer.Reset(randDuration(electLowerC, electUpperC))	// prepare for next election

	if args.PrevLogIndex<rf.logSize() && args.PrevLogTerm==rf.logAt(args.PrevLogIndex).Term{
		if args.PrevLogIndex+len(args.Entries)<=rf.lastCommit{
			dump("leader(%d,%d) <-%v- %s (all repeat)", args.LeaderID, args.Term, *reply, rf.String())			
		}else if args.PrevLogIndex<rf.lastCommit{
			rf.truncateLog(rf.lastCommit)
			rf.appendLog(args.Entries[rf.lastCommit-args.PrevLogIndex: ]...)		
			dump("leader(%d,%d) <-%v- %s (partial repeat)", args.LeaderID, args.Term, *reply, rf.String())	
		}else{
			rf.truncateLog(args.PrevLogIndex)
			rf.appendLog(args.Entries...)
			dump("leader(%d,%d) <-%v- %s (no repeat)", args.LeaderID, args.Term, *reply, rf.String())	
		}
		rf.setCommitAndApply(args.LeaderCommit)
		rf.setSnapshotAndCompress(args.LeaderSnapshot)
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, true, rf.logSize()
		return
	}else if args.PrevLogIndex<rf.logSize(){
		var prev=args.PrevLogIndex
		var confictTerm=rf.logAt(prev).Term
		for prev>rf.lastCommit && rf.logAt(prev).Term==confictTerm{ prev-- }
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, false, prev+1
		dump("leader(%d,%d) <-%v- %s (conflict backup)", args.LeaderID, args.Term, *reply, rf.String())
		return
	}else{
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, false, rf.logSize()
		dump("leader(%d,%d) <-%v- %s (no fetch backup)", args.LeaderID, args.Term, *reply, rf.String())	
		return
	}
	REFUSE:
		reply.Term, reply.Success, reply.ExpectNext=rf.currentTerm, false, -1
		dump("leader(%d,%d) <-%v- %s (refuse)", args.LeaderID, args.Term, *reply, rf.String())
}