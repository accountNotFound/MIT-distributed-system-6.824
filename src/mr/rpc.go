package mr

// Job public struct
type Job struct{
	Phase		tPhase
	ID			int	
	Name		string
	MapNum		int
	ReduceNum	int
}