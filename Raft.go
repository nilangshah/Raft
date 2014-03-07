package Raft

import (
	"encoding/gob"
	"fmt"
	"github.com/nilangshah/Raft/cluster"
	"log"
	"math/rand"
	"os"
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
	//Message type is request of vote
	RequestType = 1
	//Message type is response of vote request
	ResponseType = 2
	// Message type is Heartbeat of leader
	Heartbeat = 3
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

// response of vote
type voteResponse struct {
	Term uint64
	// vote granted or not
	VoteGranted bool
}

//vote request
type voteRequest struct {
	Term uint64
	//id of the candidate
	CandidateID uint64
}

// heartbeat msg of server
type heartbeat struct {
	// server id
	Id      uint64
	Term    uint64
	Success bool
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

// replicator object
type replicator struct {
	s            cluster.Server     // server interface
	term         uint64             // "current term number, which increases monotonically"
	vote         uint64             // vote given to which sevrer
	electionTick <-chan time.Time   // election timer
	state        *protectedString   //state can be follower, candidate, leader
	running      *protectedBool     //servrer is start or stop 
	leader       uint64             //id of leader
	isLeader     *protectedBool     //I am leader or not
	quit         chan chan struct{} // stopping chan

	//requestVoteChan chan requestVoteTuple
}

func init() {
	gob.Register(voteResponse{}) // give it a dummy VoteRequestObject.
	gob.Register(voteRequest{})
	gob.Register(heartbeat{})

}

// Create replicator object and return replicator interface for it
func New(server cluster.Server, fileName string) Replicator { // returns replicator interface
	latestTerm := uint64(1)

	///////////////////////////////
	if debug {
		logfile := os.Getenv("GOPATH") + "/src/github.com/nilangshah/Raft/log" + strconv.Itoa(server.Pid())
		f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			//t.Fatalf("error opening file: %v", err)
		} else {
			//defer f.Close()
			log.SetOutput(f)
		}
	}
	//////////////////////////////	
	no_of_servers = server.No_Of_Peers()
	quorum = ((no_of_servers - 1) / 2) + 1
	if debug {
		log.Println(no_of_servers, "quouram is ::::::::::::::::::", quorum)
	}
	r := &replicator{
		s:            server,
		term:         latestTerm,
		leader:       unknownLeader,
		vote:         noVote,
		electionTick: nil,
		state:        &protectedString{value: follower},
		running:      &protectedBool{value: false},
		isLeader:     &protectedBool{value: false},
		quit:         make(chan chan struct{}),
	}
	for i := range cluster.Jsontype.Object.Servers {
		if cluster.Jsontype.Object.Servers[i].Id == r.s.Pid() {
			r.term = cluster.Jsontype.Object.Servers[i].Term
			break
		}
	}

	r.resetElectionTimeout()
	return r
}

func (r *replicator) loop() {
	r.running.Set(true)

	for r.running.Get() {
		if debug {
			log.Println(">>> server", r.s.Pid(), "in term", r.term, "in state ", r.state.Get())
		}
		switch state := r.state.Get(); state {
		case follower:
			r.followerSelect()
		case candidate:
			r.candidateSelect()
		case leader:
			r.leaderSelect()
		default:
			panic(fmt.Sprintf("unknown Server State '%s'", state))
		}
	}
}

//send heartbeat to peers after each broadcast interval
func broadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMs / 10
	return time.Duration(d) * time.Millisecond
}

func (r replicator) sendHeartBeat() {
	msg := &heartbeat{Id: uint64(r.s.Pid()), Term: r.term}
	r.s.Outbox() <- &cluster.Envelope{Pid: -1, Msg: msg}
}

// for leaderstate
func (r *replicator) leaderSelect() {
	if r.leader != uint64(r.s.Pid()) {
		panic(fmt.Sprintf("leader (%d) not me (%d) when entering leaderSelect", r.leader, r.s.Pid()))
	}
	if r.vote != 0 {
		panic(fmt.Sprintf("vote (%d) not zero when entering leaderSelect", r.leader, r.s.Pid()))
	}

	hbeat := time.NewTicker(broadcastInterval())
	defer hbeat.Stop()
	go func() {
		for _ = range hbeat.C {
			r.sendHeartBeat()
		}
	}()

	for {
		select {
		case q := <-r.quit:
			r.handleQuit(q)
			return

		case t := <-r.s.Inbox():
			//mType := msg["MsgType"].(float64)

			switch t.Msg.(type) {
			case voteRequest:
				req := t.Msg.(voteRequest)
				resp, stepDown := r.handleRequestVote(req)

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: resp}
				if stepDown {
					if r.leader != unknownLeader {
						log.Printf("abandoning old leader=%d\n", r.leader)
					}
					if debug {
						log.Println("new leader unknown", r.s.Pid())
					}
					r.leader = unknownLeader
					r.state.Set(follower)
					return
				}
			case voteResponse:
				//res :=t.Msg.(*voteResponse)

			case heartbeat:
				res := t.Msg.(heartbeat)
				if debug {
					log.Println("Error:two server in", "server ids", r.s.Pid(), res.Id, "terms", r.term, ":", res.Term)
				}
				stepDown := r.handleHeartbeat(res)
				if stepDown {
					r.leader = res.Id
					r.state.Set(follower)
					return
				}

			}

		}
	}

}

// heartbeat from sevrer recieved
func (r *replicator) handleHeartbeat(res heartbeat) bool {

	if res.Term < r.term {
		return false
	}
	stepDown := false
	if res.Term > r.term {

		r.term = res.Term
		r.vote = noVote
		log.Println("leader updated to ", res.Id)
		if debug {
			if r.state.Get() == leader {
				log.Println("server shud stepdown", r.s.Pid(), ":", r.term)
			}
			if r.state.Get() != leader {
				log.Println("update term to ", res.Term)
			}
		}
		return true
	}

	if r.state.Get() == candidate && res.Id != r.leader && res.Term >= r.term {
		r.term = res.Term
		r.vote = noVote
		return true
	}
	if r.leader == unknownLeader {
		r.leader = res.Id
	}
	r.resetElectionTimeout()
	return stepDown

}

//server state is cadidate
func (r *replicator) candidateSelect() {

	if r.leader != unknownLeader {
		panic("known leader when entering candidateSelect")
	}
	if r.vote != 0 {
		if debug {
			log.Println(r.s.Pid(), ":", r.state)
		}
		panic("existing vote when entering candidateSelect")
	}
	r.generateVoteRequest()

	voting := protectedVotes{votes: map[uint64]bool{uint64(r.s.Pid()): true}}
	r.vote = uint64(r.s.Pid())
	if debug {
		log.Printf("term=%d election started\n", r.term)
	}
	for {
		select {
		case q := <-r.quit:
			if debug {
				log.Println("quit")
			}
			r.handleQuit(q)
			return
		case <-r.electionTick:
			if debug {
				log.Println("election ended with no winner; incrementing term and trying again ", r.s.Pid())
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
							log.Printf("abandoning old leader=%d\n", r.leader)
						}
					}
					if debug {
						log.Println("new leader unknown")
					}
					r.leader = unknownLeader
					r.state.Set(follower)
					return
				}
			case heartbeat:
				res := t.Msg.(heartbeat)
				log.Println("leader is up and running ", res.Id)
				stepDown := r.handleHeartbeat(res)
				if stepDown {
					r.leader = res.Id
					r.state.Set(follower)
					return
				}

			case voteResponse:
				res := t.Msg.(voteResponse)

				//r.responseVoteChan <- t.msg.Response	
				if res.Term > r.term {
					if debug {
						log.Printf("got vote from future term (%d>%d); abandoning election\n", res.Term, r.term)
					}
					r.leader = unknownLeader
					r.state.Set(follower)
					r.vote = noVote
					return // lose
				}
				if res.Term < r.term {
					if debug {
						log.Printf("got vote from past term (%d<%d); ignoring\n", res.Term, r.term)
					}
					break
				}
				if res.VoteGranted {
					if debug {
						log.Printf("%d voted for me\n", t.Pid)
					}
					voting.Lock()
					voting.votes[uint64(t.Pid)] = true
					voting.Unlock()
				}
				// "Once a candidate wins an election, it becomes leader."
				if r.pass(voting) {
					if debug {
						log.Println("I won the election", r.s.Pid())
					}
					r.isLeader.Set(true)
					r.leader = uint64(r.s.Pid())
					r.state.Set(leader)
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
		Term:        r.term,
		CandidateID: uint64(r.s.Pid()),
	}
	if debug {
		log.Println("generate vote request ", r.s.Pid())
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
				log.Println("election timeout, becoming candidate", r.s.Pid())
			}
			r.term++
			r.vote = noVote
			r.leader = unknownLeader
			r.resetElectionTimeout()
			r.state.Set(candidate)

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
						log.Printf("abandoning old leader=%d", r.leader)
					}
					//log.Println("new leader unknown")
					r.leader = unknownLeader
				}
			case voteResponse:
				//res := t.Msg.(*voteResponse)

			case heartbeat:
				res := t.Msg.(heartbeat)
				stepDown := r.handleHeartbeat(res)
				log.Println("for ", r.s.Pid(), "leader is up and running ", res.Id, res.Term, r.term, r.leader)
				if stepDown {
					r.leader = res.Id
					r.state.Set(follower)
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
		log.Printf("requestVote from newer term (%d): we defer %d", req.Term, r.s.Pid())
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
			panic("impossible state in handleRequestVote")
		}
		return &voteResponse{
			Term:        r.term,
			VoteGranted: false,
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
		log.Println("server stopped ", r.s.Pid())
	}
	r.running.Set(false)
	close(q)
}
