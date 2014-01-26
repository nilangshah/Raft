package cluster

import (
	"encoding/json"
	"fmt"
	"time"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
)

const (
	BROADCAST = -1
)

var jsontype jsonobject

type jsonobject struct {
	Object ObjectType
}

type ObjectType struct {
	Servers []ServerInfo
}

type ServerInfo struct {
	Id   int
	Host string
}

type Envelope struct {
	// On the sender side, Pid identifies the receiving peer. If instead, Pid is
	// set to cluster.BROADCAST, the message is sent to all peers. On the receiver side, the
	// Id is always set to the original sender. If the Id is not found, the message is silently dropped
	Pid int

	// An id that globally and uniquely identifies the message, meant for duplicate detection at
	// higher levels. It is opaque to this package.
	MsgId int64

	// the actual message.
	Msg interface{}
}

type Server interface {
	// Id of this server
	Pid() int

	// array of other servers' ids in the same cluster
	Peers() []int

	// the channel to use to send messages to other peers
	// Note that there are no guarantees of message delivery, and messages
	// are silently dropped
	Outbox() chan *Envelope

	// the channel to receive messages from other peers.
	Inbox() chan *Envelope
}

func (s SerVer) Outbox() chan *Envelope {

	return s.out
}
func (s SerVer) Inbox() chan *Envelope {

	return s.in
}

func (s SerVer) Pid() int {
	return s.pid
}

func (s SerVer) Peers() []int {
	return s.peers
}

type SerVer struct {
	pid     int
	peers   []int
	in      chan *Envelope
	out     chan *Envelope
	Addr    map[int]string
	sockets map[int]*zmq.Socket
}

func New(myid int, fileName string) *SerVer {

	file, e := ioutil.ReadFile(fileName)
	if e != nil {
		panic("File error: " + e.Error())
	}

	json.Unmarshal(file, &jsontype)
	s := &SerVer{
		pid:   123,
		peers: make([]int, len(jsontype.Object.Servers)-1),
		in:    make(chan *Envelope),
		out:   make(chan *Envelope),
		Addr:  map[int]string{},
		sockets : map[int]*zmq.Socket{},
	}
	count := 0
	for i := range jsontype.Object.Servers {
		if jsontype.Object.Servers[i].Id == myid {
			s.pid = myid
		} else {
			s.peers[count] = jsontype.Object.Servers[i].Id
			count++
		}
		s.Addr[jsontype.Object.Servers[i].Id] = jsontype.Object.Servers[i].Host

	}
	go ListenIn(s)
	go SendOut(s)
	return s
}

func SendOut(s *SerVer) {

	for {
		select {
		case envelope := <-(s.Outbox()):

			if envelope.Pid == BROADCAST {
				envelope.Pid = s.Pid()
				for i := range s.Peers() {
					sock, ok := s.sockets[s.peers[i]]
					if ok {
						//fmt.Println("socket reused")
						go Connect_Send(sock, envelope)
					} else {
						//fmt.Println("socket needed")
						output, err := zmq.NewSocket(zmq.DEALER)
						if err != nil {
							panic("Socket Error " + err.Error())
						}else{
						s.sockets[s.peers[i]]=output						
						}
						err = output.Connect("tcp://" + s.Addr[s.peers[i]])
						if err != nil {
							panic("Connect error " + err.Error())
						} 
						
						go Connect_Send(output, envelope)
					}
				}
			} else {
				envelope.Pid = s.Pid()
				sock, ok := s.sockets[s.Pid()]
					if ok {
						go Connect_Send(sock, envelope)
					} else {
						output, err := zmq.NewSocket(zmq.DEALER)
						if err != nil {
							panic("Socket Error " + err.Error())
						}else{
						s.sockets[s.Pid()]=output
						}
						err = output.Connect("tcp://" + s.Addr[envelope.Pid])
						if err != nil {
							panic("Connect error " + err.Error())
						} 
						go Connect_Send(output, envelope)
					}
				//go Connect_Send(s.Addr[envelope.Pid], envelope)

			}
		case <-time.After(10 * time.Second):
			fmt.Println("shud close all socket")

		}
	}

}

func Connect_Send(output *zmq.Socket, env *Envelope) {

	b, err := json.Marshal(env)
	if err != nil {
		panic("Json error: " + err.Error())
	}
	output.SendMessage(b)

}

func ListenIn(s *SerVer) {
	//fmt.Println("recieveing on ", s.Addr[s.Pid()])
	input, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		panic("Socket: " + err.Error())
	}
	err = input.Bind("tcp://" + s.Addr[s.Pid()])
	if err != nil {
		panic("Socket: " + err.Error())

	}
	for {
		msg, err := input.RecvMessage(0)
		if err != nil {
		}
		env := new(Envelope)
		err = json.Unmarshal([]byte(msg[0]), &env)
		if err != nil {
			panic("Json error:" + err.Error())
		}
		//fmt.Printf("%+v", animals)

		s.Inbox() <- env
	}

}
