package raft



// RequestVoteArgs rpc
type RequestVoteArgs struct{
	Term			int
	CandidateID		int
	LastLogTerm		int
	LastLogIndex	int
}
// RequestVoteReply rpc
type RequestVoteReply struct{
	Term	int
	Success	bool
}
// AppendEntriesArgs rpc
type AppendEntriesArgs struct{
	Term			int				
	LeaderID		int				
	PrevLogIndex	int			
	PrevLogTerm		int			
	Entries			tEntrySlice
	LeaderCommit	int			
}
// AppendEntriesReply rpc
type AppendEntriesReply struct{
	Term		int
	ExpectNext	int	// for log backup efficency
	Success		bool	
}