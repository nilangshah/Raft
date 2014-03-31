package cluster

import (
	"encoding/json"
	//"fmt"
	"bytes"
	"encoding/gob"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"log"
	"time"
)

const (
	BROADCAST = -1
)

var Jsontype Jsonobject

type Jsonobject struct {
	Object ObjectType
}

type ObjectType struct {
	Servers       []ServerInfo
	No_of_servers int
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

	//get the no of peers
	No_Of_Peers() int
}

func (s serVer) No_Of_Peers() int {
	return s.nop
}

func (s serVer) Outbox() chan *Envelope {

	return s.out
}
func (s serVer) Inbox() chan *Envelope {

	return s.in
}

func (s serVer) Pid() int {
	return s.pid
}

func (s serVer) Peers() []int {
	return s.peers
}

type serVer struct {
	pid     int
	peers   []int
	in      chan *Envelope
	out     chan *Envelope
	addr    map[int]string
	sockets map[int]*zmq.Socket
	nop     int
}

func New(myid int, fileName string) Server {
	//fmt.Println(myid,"")
	file, e := ioutil.ReadFile(fileName)
	if e != nil {
		panic("File error: " + e.Error())
	}

	json.Unmarshal(file, &Jsontype)
	s := &serVer{
		pid:     123,
		peers:   make([]int, len(Jsontype.Object.Servers)-1),
		in:      make(chan *Envelope),
		out:     make(chan *Envelope),
		addr:    map[int]string{},
		sockets: make(map[int]*zmq.Socket),
		nop:     Jsontype.Object.No_of_servers,
	}

	count := 0
	for i := range Jsontype.Object.Servers {
		if Jsontype.Object.Servers[i].Id == myid {
			s.pid = myid
		} else {
			s.peers[count] = Jsontype.Object.Servers[i].Id
			count++
		}
		s.addr[Jsontype.Object.Servers[i].Id] = Jsontype.Object.Servers[i].Host

	}
	for i := range s.Peers() {
		s.sockets[s.peers[i]], _ = zmq.NewSocket(zmq.PUSH)
		s.sockets[s.peers[i]].SetSndtimeo(time.Millisecond * 30)
		err := s.sockets[s.peers[i]].Connect("tcp://" + s.addr[s.peers[i]])
		if err != nil {
			panic("Connect error " + err.Error())
		}

	}
	go ListenIn(s)
	go SendOut(s)
	return s
}

func SendOut(s *serVer) {
	// Stand-in for a network connection
	var network bytes.Buffer
	for {
		envelope := <-(s.Outbox())
		if envelope.Pid == BROADCAST {
			envelope.Pid = s.Pid()

			for i := range s.Peers() {

				network.Reset()
				enc := gob.NewEncoder(&network)

				err := enc.Encode(envelope)
				if err != nil {
					panic("gob error: " + err.Error())
				}
				s.sockets[s.peers[i]].Send(network.String(), 0)

			}
		} else {
			network.Reset()
			a := envelope.Pid
			envelope.Pid = s.Pid()
			enc := gob.NewEncoder(&network)
			err := enc.Encode(envelope)
			if err != nil {
				panic("gob error: " + err.Error())
			}
			s.sockets[a].Send(network.String(), 0)

		}

	}

}

func ListenIn(s *serVer) {
	input, err := zmq.NewSocket(zmq.PULL)
	if err != nil {
		panic("Socket: " + err.Error())
	}
	err = input.Bind("tcp://" + s.addr[s.Pid()])
	if err != nil {
		panic("Socket: " + err.Error())

	}
	for {
		msg, err := input.Recv(0)
		if err != nil {
		}
		//env := new(Envelope)
		b := bytes.NewBufferString(msg)
		dec := gob.NewDecoder(b)
		//log.Println(msg)
		env := new(Envelope)
		err = dec.Decode(env)
		if err != nil {
			log.Fatal("decode:", err)
		}
		//err = json.Unmarshal([]byte(msg), &env)
		s.Inbox() <- env
	}

}
