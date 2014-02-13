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

var no_of_servers int
var quorum int

const (
	follower  = "Follower"
	candidate = "Candidate"
	leader    = "Leader"
)

const (
	unknownLeader = 0
	noVote        = 0
	RequestType   = 1
	ResponseType  = 2
	Heartbeat     = 3
	
)

var (
	// MinimumElectionTimeoutMs can be set at package initialization. It may be
	// raised to achieve more reliable replication in slow networks, or lowered
	// to achieve faster replication in fast networks. Lowering is not
	// recommended.
	MinimumElectionTimeoutMs int32 = 250

	maximumElectionTimeoutMs = 2 * MinimumElectionTimeoutMs
)

type protectedString struct {
	sync.RWMutex
	value string
}

type protectedVotes struct{
	sync.RWMutex
	votes map[uint64]bool
}



func (s *protectedString) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *protectedString) Set(value string) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

type voteResponse struct {
	Term        uint64
	VoteGranted bool
}

type voteRequest struct {
	Term        uint64
	CandidateID uint64
}

type heartbeat struct{
	Id uint64
	
}




type msgPass struct {
	MsgType  int
	Request  voteRequest  //msgtype = 1
	Response voteResponse // msgtype= 2
	Hb heartbeat
	
}

// protectedBool is just a bool protected by a mutex.
type protectedBool struct {
	sync.RWMutex
	value bool
}

func (s *protectedBool) Get() bool {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *protectedBool) Set(value bool) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

type Replicator interface {
	Term() uint64
	IsLeader() bool
	IsLeaderSet(bool)
	Start()
	Stop()
	loop()
	followerSelect()
	candidateSelect()
	leaderSelect()
}

func (r replicator) Term() uint64 {
	return r.term
}

func (r replicator) IsLeader() bool {
	return r.isLeader.Get()
}
func (r replicator) IsLeaderSet(val bool) {
	r.isLeader.Lock()
	defer r.isLeader.Unlock()
	r.isLeader.value = val
}

type replicator struct {
	s            cluster.Server
	term         uint64 // "current term number, which increases monotonically"
	vote         uint64
	electionTick <-chan time.Time
	state        *protectedString
	running      *protectedBool
	leader       uint64
	isLeader     *protectedBool
	quit         chan chan struct{}
	
	//requestVoteChan chan requestVoteTuple
}

func New(server cluster.Server, fileName string) Replicator {
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

func (r *replicator) Start() {
	log.Println("server started ",r.s.Pid())
	go r.loop()
}

func (r *replicator) loop() {
	r.running.Set(true)
	for r.running.Get() {
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

func broadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMs / 10
	return time.Duration(d) * time.Millisecond
}


func (r *replicator) sendHeartBeat(){
	msg := &msgPass{
		MsgType: Heartbeat,
		Hb: heartbeat{Id:uint64(r.s.Pid())},
	}

	r.s.Outbox() <- &cluster.Envelope{Pid: -1, Msg: msg}
	
}


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
			log.Println("stopping 2")
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

				//req:= msg["Request"].(map[string]interface{})
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
					log.Println("new leader unknown")
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
				log.Println("Error:bad state another leader is up ",res.Id)
				//r.resHeartbeat<-res
				//fmt.Println(msg)
				
			}

		}
	}

}
func (r *replicator) candidateSelect() {

	if r.leader != unknownLeader {
		panic("known leader when entering candidateSelect")
	}
	if r.vote != 0 {
		panic("existing vote when entering candidateSelect")
	}
	r.generateVoteRequest()

	voting := protectedVotes{votes: map[uint64]bool{uint64(r.s.Pid()): true},}
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
					MsgType:  RequestType,
					Response: resp,
				}

				r.s.Outbox() <- &cluster.Envelope{Pid: int(req.CandidateID), Msg: msg}
				if stepDown {
					// stepDown as a Follower means just to reset the leader
					if r.leader != unknownLeader {
						log.Printf("abandoning old leader=%d\n", r.leader)
					}
					log.Println("new leader unknown")
					r.leader = unknownLeader
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
				log.Println("leader is up and running ",res.Id)
				
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
					log.Printf("%d voted for me\n", t.Pid)
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

func (r *replicator) pass(voting protectedVotes) bool {
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
	log.Println(voting.votes)
	log.Println(no_of_votes,":",quorum)
	return no_of_votes >= quorum

}

func (r *replicator) generateVoteRequest() {

	msg := &msgPass{
		MsgType: RequestType,
		Request: voteRequest{
			Term:        r.term,
			CandidateID: uint64(r.s.Pid()),
		},
		//Response: nil,		
	}
	log.Println("generate vote request ",r.s.Pid())
	r.s.Outbox() <- &cluster.Envelope{Pid: -1, Msg: msg}
}

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
					log.Println("new leader unknown")
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
				//log.Println("leader is up and running ",res.Id)
				//if res.Id==r.leader{
					r.resetElectionTimeout()
				//}else{
				//	log.Println("bad state: heartbeat from ",res.Id," but leader is",r.leader)				
			//	}
			}

		}
	}
}

func (r *replicator) handleRequestVote(req voteRequest) (voteResponse, bool) {
	if req.Term < r.term {
		return voteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if req.Term > r.term {
		log.Printf("requestVote from newer term (%d): we defer", r.term)
		r.term = req.Term
		r.vote = noVote
		r.leader = unknownLeader
		stepDown = true
	}

	// Special case: if we're the leader, and we haven't been deposed by a more
	// recent term, then we should always deny the vote
	if r.state.Get() == leader && !stepDown {
		return voteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, stepDown
	}

	// If we've already voted for someone else this term, reject
	if r.vote != 0 && r.vote != req.CandidateID {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
		return voteResponse{
			Term:        r.term,
			VoteGranted: false,
		}, stepDown
	}

	// We passed all the tests: cast vote in favor
	r.vote = req.CandidateID
	r.resetElectionTimeout()
	return voteResponse{
		Term:        r.term,
		VoteGranted: true,
	}, stepDown

}

func (r *replicator) Stop() {
	log.Println("stopping 1")
	q := make(chan struct{})
	r.quit <- q
	<-q
	log.Println("server stopped ",r.s.Pid())
}

func (r *replicator) resetElectionTimeout() {
		r.electionTick = time.NewTimer(electionTimeout()).C
	
}

// minimumElectionTimeout returns the current minimum election timeout.
func minimumElectionTimeout() time.Duration {
	return time.Duration(MinimumElectionTimeoutMs) * time.Millisecond
}

// maximumElectionTimeout returns the current maximum election time.
func maximumElectionTimeout() time.Duration {
	return time.Duration(maximumElectionTimeoutMs) * time.Millisecond
}

// electionTimeout returns a variable time.Duration, between the minimum and
// maximum election timeouts.
func electionTimeout() time.Duration {
	//rand.Seed(time.Now().Unix())
	n := rand.Intn(int(maximumElectionTimeoutMs - MinimumElectionTimeoutMs))
	d := int(MinimumElectionTimeoutMs) + n
	return time.Duration(d) * time.Millisecond
}

func (r *replicator) handleQuit(q chan struct{}) {
	log.Println("got quit signal")
	r.running.Set(false)
	close(q)
}
