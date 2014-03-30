package Raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/nilangshah/Raft/cluster"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

//total no of servers
var no_of_servers int

//no of votes needed to win election
var quorum int

//possible states of server at any point of time
const (
	follower  = "Follower"
	candidate = "Candidate"
	leader    = "Leader"
	debug     = true
)

//constants
const (
	//Leader is unknwon
	unknownLeader = 0
	// Have not vote anyone in perticular term
	noVote = 0
)

var (
	errTimeout               = errors.New("Time out while log replication")
	errDeposed               = errors.New("deposed during replication")
	errOutOfSync             = errors.New("out of sync")
	errappendEntriesRejected = errors.New("appendEntries RPC rejected")
)

var (
	//min election time out - T milliseconds
	// If follower do not hear from leader during [T-2T] time election timer will hit
	// follower will become candidate and start new election
	//If candidate do not hear from followers during [T-2T] time election timer will hit
	// candidate will update term and start a fresh election
	MinimumElectionTimeoutMs int32 = 250
	//max election time out -  2T milliseconds
	maximumElectionTimeoutMs = 2 * MinimumElectionTimeoutMs
)

// string with mutex
type protectedString struct {
	sync.RWMutex
	value string
}

// bool with mutex
type protectedVotes struct {
	sync.RWMutex
	votes map[uint64]bool
}

// getter method for protectedstring
func (s *protectedString) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

// setter method for protectedstring
func (s *protectedString) Set(value string) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

// mutex boolean
type protectedBool struct {
	sync.RWMutex
	value bool
}

// getter method of bool
func (s *protectedBool) Get() bool {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

// setter method of bool
func (s *protectedBool) Set(value bool) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

// Replicator interface
// Replicator is responsible for Raft leader election and Raft log replication
// Replicator uses cluster package for commnication to other replicator instances.
type Replicator interface {
	// Term of this replicator
	Term() uint64
	// Is this replicator leader
	IsLeader() bool
	//
	GetLeader() uint64
	// replicator is started or stopped
	IsRunning() bool
	// method to set replicator leader
	IsLeaderSet(bool)
	// method to start replicator
	Start()
	// method to stop replicator
	Stop()
	// Mailbox for state machine layer above to send commands of any
	// kind, and to have them replicated by raft.  If the server is not
	// the leader, the message will be silently dropped.
	Outbox() chan<- interface{}

	//Mailbox for state machine layer above to receive commands. These
	//are guaranteed to have been replicated on a majority
	Inbox() <-chan *LogItem
}

func (r replicator) Outbox() chan<- interface{} {
	return r.command_outchan
}

func (r replicator) Inbox() <-chan *LogItem {
	return r.command_inchan
}

//return leader as per this replicator
func (r replicator) GetLeader() uint64 {
	return r.leader
}

//return term of server
func (r replicator) Term() uint64 {
	return r.term
}

//return whether server is leader or not
func (r replicator) IsLeader() bool {
	r.isLeader.RLock()
	defer r.isLeader.RUnlock()
	return r.isLeader.Get()
}

func (r replicator) IsRunning() bool {
	r.running.RLock()
	defer r.running.RUnlock()
	return r.running.Get()
}

//set server leader status
func (r replicator) IsLeaderSet(val bool) {
	r.isLeader.Lock()
	defer r.isLeader.Unlock()
	r.isLeader.value = val
}

//start the server
func (r *replicator) Start() {
	if debug {
		log.Println("server started ", r.s.Pid())
	}
	go r.loop()
}

// stop the server
func (r *replicator) Stop() {
	q := make(chan struct{})
	r.quit <- q
	<-q
}

type nextIndex struct {
	sync.RWMutex
	m map[uint64]uint64 // followerId: nextIndex
}

// replicator object
type replicator struct {
	s               cluster.Server     // server interface
	term            uint64             // "current term number, which increases monotonically"
	vote            uint64             // vote given to which sevrer
	electionTick    <-chan time.Time   // election timer
	state           *protectedString   //state can be follower, candidate, leader
	running         *protectedBool     //servrer is start or stop
	leader          uint64             //id of leader
	isLeader        *protectedBool     //I am leader or not
	quit            chan chan struct{} // stopping chan
	log             *raftLog           //Raft log
	logger          *log.Logger
	command_inchan  chan *LogItem
	command_outchan chan interface{}
	//requestVoteChan chan requestVoteTuple
}

func init() {
	gob.Register(voteResponse{}) // give it a dummy VoteRequestObject.
	gob.Register(voteRequest{})
	gob.Register(appendEntries{})
	gob.Register(appendEntriesResponse{})
	gob.Register(LogItem{})

}

// Create replicator object and return replicator interface for it
func New(server cluster.Server, dirName string) Replicator { // returns replicator interface
	//latestTerm := uint64(1)
	//var logger *log.Logger
	///////////////////////////////
	//if debug {
	logfile := os.Getenv("GOPATH") + "/src/github.com/nilangshah/Raft/log" + strconv.Itoa(server.Pid())
	f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		//t.Fatalf("error opening file: %v", err)
	} else {
		//defer f.Close()

		log.SetOutput(f)
	}

	//}
	//////////////////////////////
	raftlog := newRaftLog(dirName)
	latestTerm := raftlog.lastTerm()
	no_of_servers = server.No_Of_Peers()
	quorum = ((no_of_servers - 1) / 2) + 1

	r := &replicator{
		s:               server,
		term:            latestTerm,
		leader:          unknownLeader,
		vote:            noVote,
		electionTick:    nil,
		log:             raftlog,
		state:           &protectedString{value: follower},
		running:         &protectedBool{value: false},
		isLeader:        &protectedBool{value: false},
		quit:            make(chan chan struct{}),
		logger:          log.New(f, log.Prefix(), log.Flags()),
		command_inchan:  make(chan *LogItem),
		command_outchan: make(chan interface{}),
	}
	for i := range cluster.Jsontype.Object.Servers {
		if cluster.Jsontype.Object.Servers[i].Id == r.s.Pid() {
			r.term = cluster.Jsontype.Object.Servers[i].Term
			break
		}
	}
	r.term = latestTerm
	//r.logger.Println(r.term)
	r.resetElectionTimeout()
	return r
}

func (r *replicator) loop() {
	r.running.Set(true)

	for r.running.Get() {
		if debug {
			r.logger.Println(">>> server", r.s.Pid(), "in term", r.term, "in state ", r.state.Get())
		}
		switch state := r.state.Get(); state {
		case follower:
			r.followerSelect()
		case candidate:
			r.candidateSelect()
		case leader:
			r.leaderSelect()
		default:
			r.logger.Println(fmt.Sprintf("unknown Server State '%s'", state))
		}
	}
}

//send heartbeat to peers after each broadcast interval
func broadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMs / 5
	return time.Duration(d) * time.Millisecond
}

func (r replicator) newNextIndex(defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for _, id := range r.s.Peers() {
		ni.m[uint64(id)] = defaultNextIndex
	}

	return ni
}

func (ni *nextIndex) prevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))

	}
	return ni.m[id]
}

func (ni *nextIndex) decrement(id uint64, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()

	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}

	if i != prev {
		return i, errOutOfSync
	}

	if i > 0 {
		ni.m[id]--
	}
	return ni.m[id], nil
}

func (r replicator) flush(id int, ni *nextIndex, timeout time.Duration) error {
	currentTerm := r.term
	prevLogIndex := ni.prevLogIndex(uint64(id))
	entries, prevLogTerm := r.log.entriesAfter(prevLogIndex)
	commitIndex := r.log.getCommitIndex()

	msg := &appendEntries{Term: currentTerm, LeaderId: uint64(r.s.Pid()), PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries:     entries,
		CommitIndex: commitIndex}
	r.s.Outbox() <- &cluster.Envelope{Pid: id, Msg: msg}

	select {

	case t := <-r.s.Inbox():

		switch t.Msg.(type) {

		case appendEntriesResponse:
			resp := t.Msg.(appendEntriesResponse)
			if resp.Term > currentTerm {
				return errDeposed
			}

			if !resp.Success {
				newPrevLogIndex, err := ni.decrement(uint64(id), prevLogIndex)
				if err != nil {
					if debug {
						log.Printf("flush to %v: while decrementing prevLogIndex: %s", id, err)
					}
					return err
				}
				if debug {
					log.Printf("flush to %v: rejected; prevLogIndex(%v) becomes %d", id, id, newPrevLogIndex)
				}
				return errappendEntriesRejected
			}

			if len(entries) > 0 {
				newPrevLogIndex, err := ni.set(uint64(id), entries[len(entries)-1].Index, prevLogIndex)
				if err != nil {
					if debug {
						log.Printf("flush to %v: while moving prevLogIndex forward: %s", id, err)
					}
					return err
				}
				if debug {
					log.Printf("flush to %v: accepted; prevLogIndex(%v) becomes %d", id, id, newPrevLogIndex)
				}
				return nil
			}
			return nil

		}

	case <-time.After(2 * timeout):
		return errTimeout
	}
	return nil

}

func (ni *nextIndex) set(id, index, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()

	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("server %d not found", id))
	}
	if i != prev {
		return i, errOutOfSync
	}

	ni.m[id] = index
	return index, nil
}

func (r *replicator) concurrentReplicate(ni *nextIndex, timeout time.Duration) (int, bool) {
	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(r.s.Peers()))
	for _, id1 := range r.s.Peers() {

		go func(id int) {
			errChan := make(chan error, 1)
			go func() { errChan <- r.flush(id, ni, timeout) }()
			responses <- tuple{uint64(id), <-errChan} // first responder wins

		}(id1)

	}

	successes, stepDown := 0, false
	for i := 0; i < cap(responses); i++ {
		switch t := <-responses; t.err {
		case nil:
			successes++
		case errDeposed:
			stepDown = true
		default:
		}
	}
	return successes, stepDown
}

func (ni *nextIndex) bestIndex() uint64 {
	ni.RLock()
	defer ni.RUnlock()

	if len(ni.m) <= 0 {
		return 0
	}

	var i uint64 = math.MaxUint64
	for _, nextIndex := range ni.m {
		if nextIndex < i {
			i = nextIndex
		}
	}
	return i
}

// for leaderstate
func (r *replicator) leaderSelect() {
	if r.leader != uint64(r.s.Pid()) {
		r.logger.Println(fmt.Sprintf("leader (%d) not me (%d) when entering leaderSelect", r.leader, r.s.Pid()))
	}
	if r.vote != 0 {
		r.logger.Println(fmt.Sprintf("vote (%d) not zero when entering leaderSelect", r.leader, r.s.Pid()))
	}
	nIndex := r.newNextIndex(r.log.lastIndex()) // +1)
	replicate := make(chan struct{})
	//go r.listenFlush(replicate,nIndex)
	hbeat := time.NewTicker(broadcastInterval())
	defer hbeat.Stop()
	go func() {
		for _ = range hbeat.C {
			replicate <- struct{}{}
		}
	}()

	for {
		select {
		case q := <-r.quit:
			r.handleQuit(q)
			return
		case t := <-r.command_outchan:
			cmd := t.(CommandTuple)
			// Append the command to our (leader) log
			r.logger.Println("got command, appending", r.term)
			currentTerm := r.term
			entry := &LogItem{
				Index:     r.log.lastIndex() + 1,
				Term:      currentTerm,
				Command:   cmd.Command,
				committed: cmd.CommandResponse,
			}
			r.logger.Println(entry)
			if err := r.log.appendEntry(entry); err != nil {
				panic(err)
				continue
			}

			r.logger.Printf(
				"after append, commitIndex=%d lastIndex=%d lastTerm=%d",
				r.log.getCommitIndex(),
				r.log.lastIndex(),
				r.log.lastTerm(),
			)

			go func() { replicate <- struct{}{} }()
			//cmd.Err <- nil

		case <-replicate:
			if debug {
				r.logger.Println("I am leader", r.s.Pid(), "Term", r.term)
			}
			successes, stepDown := r.concurrentReplicate(nIndex, 2*broadcastInterval())
			if stepDown {
				r.logger.Println("deposed during replicate")
				r.state.Set(follower)
				r.leader = unknownLeader
				return
			}
			r.logger.Println(successes)
			if successes >= quorum-1 {

				var indices []uint64
				indices = append(indices, r.log.currentIndex())
				for _, i := range nIndex.m {
					indices = append(indices, i)
				}

				sort.Sort(uint64Slice(indices))
				commitIndex := indices[quorum-1]
				committedIndex := r.log.commitIndex

				peersBestIndex := commitIndex
				ourLastIndex := r.log.lastIndex()
				ourCommitIndex := r.log.getCommitIndex()
				if peersBestIndex > ourLastIndex {
					r.leader = unknownLeader
					r.vote = noVote
					r.state.Set(follower)
					return
				}
				if commitIndex > committedIndex {
					// leader needs to do a fsync before committing log entries
					if err := r.log.commitTo(peersBestIndex); err != nil {
						r.logger.Printf("commitTo(%d): %s", peersBestIndex, err)
						continue // oh well, next time?
					}
					if r.log.getCommitIndex() > ourCommitIndex {
						r.logger.Printf("after commitTo(%d), commitIndex=%d -- queueing another flush", peersBestIndex, r.log.getCommitIndex())
						go func() { replicate <- struct{}{} }()
					}
				}

			}
		case t := <-r.s.Inbox():

			switch t.Msg.(type) {
			case voteRequest:
				req := t.Msg.(voteRequest)
				resp, stepDown := r.handleRequestVote(req)

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: resp}
				if stepDown {
					if r.leader != unknownLeader {
						if debug {
							r.logger.Printf("abandoning old leader=%d\n", r.leader)
						}
					}
					if debug {
						r.logger.Println("new leader unknown", r.s.Pid())
					}
					r.leader = unknownLeader
					r.state.Set(follower)
					r.isLeader.Set(false)
					return
				}
			case voteResponse:
				//res :=t.Msg.(*voteResponse)

			case appendEntries:
				res := t.Msg.(appendEntries)
				if debug {
					r.logger.Println("Error:two server in", "server ids", r.s.Pid(), res.LeaderId, "terms", r.term, ":", res.Term)
				}
				resp, stepDown := r.handleAppendEntries(res)
				//r.logappendEntriesResponse(res, *resp, stepDown)
				r.s.Outbox() <- &cluster.Envelope{Pid: int(res.LeaderId), Msg: resp}
				if stepDown {
					r.leader = res.LeaderId
					r.state.Set(follower)
					return
				}

			}

		}
	}

}

// heartbeat from sevrer recieved
func (r *replicator) handleAppendEntries(res appendEntries) (*appendEntriesResponse, bool) {

	if res.Term < r.term {
		return &appendEntriesResponse{
			Term:    r.term,
			Success: false,
			reason:  fmt.Sprintf("Term %d < %d", res.Term, r.term),
		}, false
	}
	stepDown := false
	if res.Term > r.term {

		r.term = res.Term
		r.vote = noVote
		stepDown = true
		if debug {
			r.logger.Println("leader updated to ", res.LeaderId)

			if r.state.Get() == leader {
				r.logger.Println("server shud stepdown", r.s.Pid(), ":", r.term)
			}
			if r.state.Get() != leader {
				r.logger.Println("update term to ", res.Term)
			}
		}
		//return true
	}
	if r.state.Get() == candidate && res.LeaderId != r.leader && res.Term >= r.term {
		r.term = res.Term
		r.vote = noVote
		stepDown = true
	}

	r.resetElectionTimeout()

	// Reject if log doesn't contain a matching previous entry
	if err := r.log.discard(res.PrevLogIndex, res.PrevLogTerm); err != nil {
		//r.logger.Println("all notgood")
		return &appendEntriesResponse{
			Term:    r.term,
			Success: false,
			reason:  fmt.Sprintf("while ensuring last log entry had index=%d term=%d: error: %s", res.PrevLogIndex, res.PrevLogTerm, err)}, stepDown
	}
	// Process the entries
	for i, entry := range res.Entries {

		// Append entry to the log
		if err := r.log.appendEntry(entry); err != nil {
			return &appendEntriesResponse{
				Term:    r.term,
				Success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(res.Entries),
					err,
				),
			}, stepDown
		}

	}

	if res.CommitIndex > 0 && res.CommitIndex > r.log.getCommitIndex() {
		r.logger.Println("commit to", res.CommitIndex)
		if err := r.log.commitTo(res.CommitIndex); err != nil {
			return &appendEntriesResponse{
				Term:    r.term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", res.CommitIndex, err),
			}, stepDown
		}
	}

	return &appendEntriesResponse{
		Term:    r.term,
		Success: true,
	}, stepDown

}

//server state is cadidate
func (r *replicator) candidateSelect() {

	if r.leader != unknownLeader {
		r.logger.Println("known leader when entering candidateSelect")
	}
	if r.vote != 0 {
		if debug {
			r.logger.Println(r.s.Pid(), ":", r.state)
		}
		r.logger.Println("existing vote when entering candidateSelect")
	}
	r.generateVoteRequest()

	voting := protectedVotes{votes: map[uint64]bool{uint64(r.s.Pid()): true}}
	r.vote = uint64(r.s.Pid())
	if debug {
		r.logger.Printf("term=%d election started\n", r.term)
	}
	for {
		select {
		case q := <-r.quit:
			if debug {
				r.logger.Println("quit")
			}
			r.handleQuit(q)
			return
		case <-r.electionTick:
			if debug {
				r.logger.Println("election ended with no winner; incrementing term and trying again ", r.s.Pid())
			}
			r.resetElectionTimeout()
			r.term++
			r.vote = noVote
			return // draw
		case t := <-r.s.Inbox():
			switch t.Msg.(type) {
			case voteRequest:

				req := t.Msg.(voteRequest)
				resp, stepDown := r.handleRequestVote(req)
				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: resp}
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					if r.leader != unknownLeader {
						if debug {
							r.logger.Printf("abandoning old leader=%d\n", r.leader)
						}
					}
					if debug {
						r.logger.Println("new leader unknown")
					}
					r.leader = unknownLeader
					r.state.Set(follower)
					return
				}
			case appendEntries:
				res := t.Msg.(appendEntries)
				if debug {
					r.logger.Println("leader is up and running ", res.LeaderId)
				}
				resp, stepDown := r.handleAppendEntries(res)
				//r.logger.Println("leader msg came", res.LeaderId)
				r.s.Outbox() <- &cluster.Envelope{Pid: int(res.LeaderId), Msg: resp}
				if stepDown {
					r.leader = res.LeaderId
					r.state.Set(follower)
					return
				}

			case voteResponse:
				res := t.Msg.(voteResponse)

				//r.responseVoteChan <- t.msg.Response
				if res.Term > r.term {
					if debug {
						r.logger.Printf("got vote from future term (%d>%d); abandoning election\n", res.Term, r.term)
					}
					r.leader = unknownLeader
					r.state.Set(follower)
					r.vote = noVote
					return // lose
				}
				if res.Term < r.term {
					if debug {
						r.logger.Printf("got vote from past term (%d<%d); ignoring\n", res.Term, r.term)
					}
					break
				}
				if res.VoteGranted {
					if debug {
						r.logger.Printf("%d voted for me\n", t.Pid)
					}
					voting.Lock()
					voting.votes[uint64(t.Pid)] = true
					voting.Unlock()
				}
				// "Once a candidate wins an election, it becomes leader."
				if r.pass(voting) {
					if debug {
						r.logger.Println("I won the election", r.s.Pid())
					}
					r.isLeader.Set(true)
					r.leader = uint64(r.s.Pid())
					r.state.Set(leader)
					r.isLeader.Set(true)
					r.vote = noVote
					return // win
				}
			}

		}
	}

}

//check wether quorum has been made or not
func (r replicator) pass(voting protectedVotes) bool {
	voting.Lock()
	defer voting.Unlock()
	no_of_votes := 1

	for id := range r.s.Peers() {
		if voting.votes[uint64(r.s.Peers()[id])] {
			no_of_votes++
			//log.Println("vote from",uint64(id) )
		}
		if no_of_votes >= quorum {
			break
		}
	}
	//log.Println(voting.votes)
	//log.Println(no_of_votes,":",quorum)
	return no_of_votes >= quorum

}

//ask vote to peers
func (r replicator) generateVoteRequest() {

	msg := &voteRequest{
		Term:         r.term,
		CandidateID:  uint64(r.s.Pid()),
		LastLogIndex: r.log.lastIndex(),
		LastLogTerm:  r.log.lastTerm(),
	}
	if debug {
		r.logger.Println("generate vote request ", r.s.Pid())
	}
	r.s.Outbox() <- &cluster.Envelope{Pid: -1, Msg: msg}
}

//server state is follower
func (r *replicator) followerSelect() {
	for {
		select {
		case q := <-r.quit:
			r.handleQuit(q)
			return
		case <-r.electionTick:
			if debug {
				r.logger.Println("election timeout, becoming candidate", r.s.Pid())
			}
			r.term++
			r.vote = noVote
			r.leader = unknownLeader
			r.resetElectionTimeout()
			r.state.Set(candidate)
			r.isLeader.Set(false)

			return

		case t := <-r.s.Inbox():
			switch t.Msg.(type) {
			case voteRequest:

				req := t.Msg.(voteRequest)

				resp, stepDown := r.handleRequestVote(req)

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: resp}
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					if r.leader != unknownLeader {
						if debug {
							r.logger.Printf("abandoning old leader=%d", r.leader)
						}
					}
					//log.Println("new leader unknown")
					r.leader = unknownLeader
				}
			case voteResponse:
				//res := t.Msg.(*voteResponse)

			case appendEntries:
				res := t.Msg.(appendEntries)
				if r.leader == unknownLeader {
					r.leader = res.LeaderId
					if debug {
						r.logger.Printf("discovered Leader %d", res.LeaderId)
					}
				}
				if len(res.Entries) > 0 {
					r.logger.Println("append ", len(res.Entries), "commit index", r.log.getCommitIndex())
				}
				resp, stepDown := r.handleAppendEntries(res)
				r.s.Outbox() <- &cluster.Envelope{Pid: int(res.LeaderId), Msg: resp}
				if debug {
					r.logger.Println("for ", r.s.Pid(), "leader is up and running ", res.LeaderId, res.Term, r.term, r.leader)
				}
				if stepDown {
					if r.leader != unknownLeader {
						if debug {
							r.logger.Printf("abandoning old leader=%d", r.leader)
						}
					}
					if debug {
						r.logger.Printf("following new leader=%d", res.LeaderId)
					}

					r.leader = res.LeaderId

					return
				}

			}

		}
	}
}

// give vote
func (r *replicator) handleRequestVote(req voteRequest) (*voteResponse, bool) {
	if req.Term < r.term {
		return &voteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, false
	}

	stepDown := false
	if req.Term > r.term {
		if debug {
			r.logger.Printf("requestVote from newer term (%d): we defer %d", req.Term, r.s.Pid())
		}
		r.term = req.Term
		r.vote = noVote
		r.leader = unknownLeader
		stepDown = true
	}

	if r.state.Get() == leader && !stepDown {
		return &voteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, stepDown
	}

	if r.vote != 0 && r.vote != req.CandidateID {
		if stepDown {
			r.logger.Println("impossible state in handleRequestVote")
		}
		return &voteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, stepDown
	}

	if r.log.lastIndex() > req.LastLogIndex || r.log.lastTerm() > req.LastLogTerm {
		return &voteResponse{
			Term:        r.term,
			VoteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				r.log.lastIndex(),
				r.log.lastTerm(),
				req.LastLogIndex,
				req.LastLogTerm,
			),
		}, stepDown
	}

	r.vote = req.CandidateID
	r.resetElectionTimeout()
	return &voteResponse{
		Term:        r.term,
		VoteGranted: true,
	}, stepDown

}

func (r *replicator) resetElectionTimeout() {
	r.electionTick = time.NewTimer(electionTimeout()).C

}

func minimumElectionTimeout() time.Duration {
	return time.Duration(MinimumElectionTimeoutMs) * time.Millisecond
}

func maximumElectionTimeout() time.Duration {
	return time.Duration(maximumElectionTimeoutMs) * time.Millisecond
}

func electionTimeout() time.Duration {
	n := rand.Intn(int(maximumElectionTimeoutMs - MinimumElectionTimeoutMs))
	d := int(MinimumElectionTimeoutMs) + n
	return time.Duration(d) * time.Millisecond
}

//to stop the sevrer
func (r *replicator) handleQuit(q chan struct{}) {
	if debug {
		r.logger.Println("server stopped ", r.s.Pid())
	}
	r.running.Set(false)
	close(q)
}
