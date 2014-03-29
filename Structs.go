package Raft

type appendEntriesTuple struct {
	Request  appendEntries
	Response chan appendEntriesResponse
}

type requestVoteTuple struct {
	Request  voteRequest
	Response chan voteResponse
}

type appendEntries struct {
	Term         uint64
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogItem
	CommitIndex  uint64
}

// appendEntriesResponse represents the response to an appendEntries RPC.
type appendEntriesResponse struct {
	Term    uint64
	Success bool
	reason  string
}

type voteRequest struct {
	Term uint64
	//id of the candidate	
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type voteResponse struct {
	Term uint64
	// vote granted or not
	VoteGranted bool
	reason      string
}

type CommandTuple struct {
	Command         []byte
	CommandResponse chan bool
	Err             chan error
}

type LogItem struct {
	Index   uint64
	Term    uint64 // when received by leader
	Command []byte

	committed chan bool
}
