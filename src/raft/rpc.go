package raft

import "fmt"

// RequestVoteArgs rpc
type RequestVoteArgs struct{
	Term			int
	CandidateID		int
	LastLogTerm		int
	LastLogIndex	int
}
// RequestVoteReply rpc
type RequestVoteReply struct{
	Term			int
	VoteGaruantee	bool
}
// AppendEntriesArgs rpc
type AppendEntriesArgs struct{
	Term			int				
	LeaderID		int				
	PrevLogIndex	int			
	PrevLogTerm		int			
	Entries			[]entryT
	LeaderCommit	int			
	LeaderSnapshot	int		// for follower log compress, in lab2 this will be ignore
}
func (args *AppendEntriesArgs) String() string{
	return fmt.Sprintf("{%d, %d, %d, %d, %d}", 
					args.Term, args.PrevLogIndex, len(args.Entries), args.LeaderCommit, args.LeaderSnapshot)
}

// AppendEntriesReply rpc
type AppendEntriesReply struct{
	Term		int
	Success		bool // to check if the follower append the log
	ExpectNext	int	// for log backup efficency, if it is -1, means follower dones't accept leaders entries
}