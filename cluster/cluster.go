package cluster

import (
	"encoding/json"
	//"fmt"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
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
	Servers []ServerInfo
}

type ServerInfo struct {
	Id   int
	Host string
	Term uint64
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
	}

	count := 0
	for i := range Jsontype.Object.Servers {
		if Jsontype.Object.Servers[i].Id == myid {
			s.pid = myid
			//s.term=Jsontype.Object.Servers[i].Term
		} else {
			s.peers[count] = Jsontype.Object.Servers[i].Id
			count++
		}
		s.addr[Jsontype.Object.Servers[i].Id] = Jsontype.Object.Servers[i].Host

	}
	for i := range s.Peers() {
	//fmt.Println("peers of server",s.Pid(),"is  ")
	s.sockets[s.peers[i]],_ =  zmq.NewSocket(zmq.PUSH)
	s.sockets[s.peers[i]].SetSndtimeo(time.Millisecond*30)
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
	//count1:=0
	//sock := make(*zmq.Socket)
	for {
		//	if count1%1000==0{fmt.Println(count1)}
		//select {
		 envelope := <-(s.Outbox())
		//	count1++
			if envelope.Pid == BROADCAST {
				envelope.Pid = s.Pid()
				
				for i := range s.Peers() {
						
						b, err := json.Marshal(envelope)
						if err != nil {
							panic("Json error: " + err.Error())
						}
						s.sockets[s.peers[i]].Send(string(b), 0)

				}
			} else {
					a:=envelope.Pid
					envelope.Pid = s.Pid()
					//go Connect_Send(sock, envelope)
					b, err := json.Marshal(envelope)
					if err != nil {
						panic("Json error: " + err.Error())
					}
					s.sockets[a].Send(string(b), 0)
				//go Connect_Send(s.addr[envelope.Pid], envelope)

			}
	//	case <-time.After(100 * time.Second):
			//			fmt.Println("shud close all socket")

		//}
	}

}

/*
func Connect_Send(output *zmq.Socket, env *Envelope) {

	b, err := json.Marshal(env)
	if err != nil {
		panic("Json error: " + err.Error())
	}
	output.Send(string(b),0)

}*/

func ListenIn(s *serVer) {
	//fmt.Println("recieveing on ", s.addr[s.Pid()])
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
		env := new(Envelope)
		err = json.Unmarshal([]byte(msg), &env)
		if err != nil {
			panic("Json error:" + err.Error())
		}
		//fmt.Printf("%+v", animals)

		s.Inbox() <- env
	}

}
