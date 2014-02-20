package Raft

import (
	"fmt"
	"github.com/nilangshah/Raft/cluster"
	"log"
	"math/rand"
	"reflect"
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
)

const (
	
	unknownLeader = 0
	noVote        = 0
	RequestType   = 1 //constants 
	ResponseType  = 2
	Heartbeat     = 3
)

var (
	//min election time out - T
	MinimumElectionTimeoutMs int32 = 250 
	//max election time out -  2T
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
	Term        uint64
	// vote granted or not
	VoteGranted bool 
}

//vote request
type voteRequest struct {
	Term        uint64 
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

type msgPass struct {
	// type of msg
	MsgType  int           
	Request  *voteRequest  //msgtype = 1
	Response *voteResponse // msgtype= 2
	Hb       heartbeat     //msgtype=3

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

// replicator interface	
type Replicator interface {
 	Term() uint64
	IsLeader() bool
	IsRunning() bool
	IsLeaderSet(bool)
	Start()
	Stop()
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

func (r replicator) IsRunning() bool{
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
	log.Println("server started ", r.s.Pid())
	go r.loop()
}

// stop the server
func (r *replicator) Stop() {
	q := make(chan struct{})
	r.quit <- q
	<-q
	log.Println("server stopped ", r.s.Pid())
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

//create and return replicator object
func New(server cluster.Server, fileName string) Replicator { // returns replicator interface
	latestTerm := uint64(1)
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
		log.Println(">>> server", r.s.Pid(), "in term", r.term, "in state ", r.state.Get())
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
	msg := &msgPass{
		MsgType: Heartbeat,
		Hb:      heartbeat{Id: uint64(r.s.Pid()), Term: r.term},
	}
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
			//log.Println("send heart beat")
			r.sendHeartBeat()
		}
	}()

	for {
		select {
		case q := <-r.quit:
			r.handleQuit(q)
			return

		case t := <-r.s.Inbox():
			msg, ok := (t.Msg).(map[string]interface{})
			if !ok {
				log.Println("not proper msg", r.s.Pid())
				panic("msg")
				return
			}
			mType := msg["MsgType"].(float64)

			switch int(mType) {
			case 1:

				req := voteRequest{}
				v := reflect.ValueOf(&req).Elem()

				for key, value := range msg["Request"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						// or return an error on private fields
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}

				resp, stepDown := r.handleRequestVote(req)
				msg := &msgPass{
					MsgType:  ResponseType,
					Response: resp,
				}

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: msg}
				if stepDown {
					if r.leader != unknownLeader {
						log.Printf("abandoning old leader=%d\n", r.leader)
					}
					//log.Println("new leader unknown")
					r.leader = unknownLeader
					r.state.Set(follower)
					return
				}
			case 2:
				res := voteResponse{}
				v := reflect.ValueOf(&res).Elem()

				for key, value := range msg["Response"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}
			case Heartbeat:
				res := heartbeat{}
				v := reflect.ValueOf(&res).Elem()

				for key, value := range msg["Hb"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}
				log.Println("Error:two server in", "server ids", r.s.Pid(), res.Id, "terms", r.term, ":", res.Term)
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
		log.Println("server shud stepdown", r.s.Pid(), ":", r.term)
		r.term = res.Term
		r.vote = noVote
		return true
	}

	if r.state.Get() == candidate && res.Id != r.leader && res.Term >= r.term {
		r.term = res.Term
		r.vote = noVote
		return true
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
		log.Println(r.s.Pid(),":",r.state)
		panic("existing vote when entering candidateSelect")
	}
	r.generateVoteRequest()

	voting := protectedVotes{votes: map[uint64]bool{uint64(r.s.Pid()): true}}
	r.vote = uint64(r.s.Pid())
	log.Printf("term=%d election started\n", r.term)

	for {
		select {
		case q := <-r.quit:
			log.Println("quit")
			r.handleQuit(q)
			return
		case <-r.electionTick:
			log.Println("election ended with no winner; incrementing term and trying again")
			r.resetElectionTimeout()
			r.term++
			r.vote = noVote
			return // draw
		case t := <-r.s.Inbox():
			msg, ok := (t.Msg).(map[string]interface{})
			if !ok {
				log.Println("not proper msg", r.s.Pid())
				panic("msg")
				return
			}
			mType := msg["MsgType"].(float64)
			switch int(mType) {
			case 1:

				req := voteRequest{}
				v := reflect.ValueOf(&req).Elem()

				for key, value := range msg["Request"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}

				resp, stepDown := r.handleRequestVote(req)
				msg := &msgPass{
					MsgType:  ResponseType,
					Response: resp,
				}

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: msg}
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					if r.leader != unknownLeader {
						log.Printf("abandoning old leader=%d\n", r.leader)
					}
					//log.Println("new leader unknown")
					r.leader = unknownLeader
					r.state.Set(follower)
					return
				}
			case Heartbeat:
				res := heartbeat{}
				v := reflect.ValueOf(&res).Elem()

				for key, value := range msg["Hb"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}
				//log.Println("leader is up and running ", res.Id)
				stepDown := r.handleHeartbeat(res)
				if stepDown {
					r.leader = res.Id
					r.state.Set(follower)
					return
				}

			case 2:
				res := voteResponse{}
				v := reflect.ValueOf(&res).Elem()

				for key, value := range msg["Response"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}
				//r.responseVoteChan <- t.msg.Response	
				if res.Term > r.term {
					log.Printf("got vote from future term (%d>%d); abandoning election\n", res.Term, r.term)
					r.leader = unknownLeader
					r.state.Set(follower)
					r.vote = noVote
					return // lose
				}
				if res.Term < r.term {
					log.Printf("got vote from past term (%d<%d); ignoring\n", res.Term, r.term)
					break
				}
				if res.VoteGranted {
					//log.Printf("%d voted for me\n", t.Pid)
					voting.Lock()
					voting.votes[uint64(t.Pid)] = true
					voting.Unlock()
				}
				// "Once a candidate wins an election, it becomes leader."
				if r.pass(voting) {
					log.Println("I won the election")
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

	msg := &msgPass{
		MsgType: RequestType,
		Request: &voteRequest{
			Term:        r.term,
			CandidateID: uint64(r.s.Pid()),
		},
		//Response: nil,		
	}
	log.Println("generate vote request ", r.s.Pid())
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
			log.Println("election timeout, becoming candidate", r.s.Pid())
			r.term++
			r.vote = noVote
			r.leader = unknownLeader
			r.resetElectionTimeout()
			r.state.Set(candidate)

			return

		case t := <-r.s.Inbox():
			msg, ok := (t.Msg).(map[string]interface{})
			if !ok {
				log.Println("not proper msg", r.s.Pid())
				panic("msg")
				return
			}
			mType := msg["MsgType"].(float64)

			switch int(mType) {
			case 1:

				req := voteRequest{}
				v := reflect.ValueOf(&req).Elem()

				for key, value := range msg["Request"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}

				resp, stepDown := r.handleRequestVote(req)
				msg := &msgPass{
					MsgType:  ResponseType,
					Response: resp,
				}

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: msg}
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					if r.leader != unknownLeader {
						log.Printf("abandoning old leader=%d", r.leader)
					}
					//log.Println("new leader unknown")
					r.leader = unknownLeader
				}
			case 2:
				res := voteResponse{}
				v := reflect.ValueOf(&res).Elem()

				for key, value := range msg["Response"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						// or return an error on private fields
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
				}
			case Heartbeat:
				res := heartbeat{}
				v := reflect.ValueOf(&res).Elem()

				for key, value := range msg["Hb"].(map[string]interface{}) {
					field := v.FieldByName(key)
					if !field.IsValid() {
						println("invalid")
						break
					}
					if !field.CanSet() {
						continue
					}
					val, ok := value.(float64)
					if ok {
						field.Set(reflect.ValueOf(uint64(val)))
					} else {
						field.Set(reflect.ValueOf(value))
					}
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
		//log.Printf("requestVote from newer term (%d): we defer %d", req.Term, r.s.Pid())
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
	r.running.Set(false)
	close(q)
}
