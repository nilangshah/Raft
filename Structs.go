package Raft

type appendEntriesTuple struct {
	Request  appendEntries
	Response chan appendEntriesResponse
}

type requestVoteTuple struct {
	Request  voteRequest
	Response chan voteResponse
}

// appendEntries represents an appendEntries RPC.
type appendEntries struct {
	Term         uint64     `json:"term"`
	LeaderId     uint64     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogItem `json:"entries"`
	CommitIndex  uint64     `json:"commit_index"`
}

// appendEntriesResponse represents the response to an appendEntries RPC.
type appendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	reason  string
}

// requestVote represents a requestVote RPC.
type voteRequest struct {
	Term         uint64 `json:"term"`
	//id of the candidate	
	CandidateID  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// requestVoteResponse represents the response to a requestVote RPC.
type voteResponse struct {	
	Term        uint64 `json:"term"`
	// vote granted or not
	VoteGranted bool   `json:"vote_granted"`
	reason      string
}


type CommandTuple struct {
Command []byte
CommandResponse chan<- []byte
Err chan error
}

// logEntry is the atomic unit being managed by the distributed log. A log entry
// always has an index (monotonically increasing), a term in which the Raft
// network leader first sees the entry, and a command. The command is what gets
// executed against the node state machine when the log entry is successfully
// replicated.
type LogItem struct {
	Index uint64 `json:"index"`
	Term uint64 `json:"term"` // when received by leader
	Command []byte `json:"command,omitempty"`
	committed chan bool `json:"-"`
	commandResponse chan<- []byte `json:"-"` // only non-nil on receiver's log
	//isConfiguration bool `json:"-"` // for configuration change entries
}

