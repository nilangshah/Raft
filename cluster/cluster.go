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

	file, e := ioutil.ReadFile(fileName)
	if e != nil {
		panic("File error: " + e.Error())
	}

	json.Unmarshal(file, &jsontype)
	s := &serVer{
		pid:     123,
		peers:   make([]int, len(jsontype.Object.Servers)-1),
		in:      make(chan *Envelope),
		out:     make(chan *Envelope),
		addr:    map[int]string{},
		sockets: map[int]*zmq.Socket{},
	}

	count := 0
	for i := range jsontype.Object.Servers {
		if jsontype.Object.Servers[i].Id == myid {
			s.pid = myid
		} else {
			s.peers[count] = jsontype.Object.Servers[i].Id
			count++
		}
		s.addr[jsontype.Object.Servers[i].Id] = jsontype.Object.Servers[i].Host

	}
	//fmt.Println("server start:",s.Pid())
	go ListenIn(s)
	go SendOut(s)
	return s
}

func SendOut(s *serVer) {
	//count1:=0
	//sock := make(*zmq.Socket)
	for {
		//	if count1%1000==0{fmt.Println(count1)}
		select {
		case envelope := <-(s.Outbox()):
			//count1++
			if envelope.Pid == BROADCAST {
				envelope.Pid = s.Pid()
				for i := range s.Peers() {
					sock, ok := s.sockets[s.peers[i]]
					if ok {
						b, err := json.Marshal(envelope)
						if err != nil {
							panic("Json error: " + err.Error())
						}
						sock.Send(string(b), 0)

					} else {
						sock, err := zmq.NewSocket(zmq.DEALER)
						if err != nil {
							panic("Socket Error " + err.Error())
						} else {
							s.sockets[s.peers[i]] = sock
						}
						err = sock.Connect("tcp://" + s.addr[s.peers[i]])
						if err != nil {
							panic("Connect error " + err.Error())
						}

						b, err := json.Marshal(envelope)
						if err != nil {
							panic("Json error: " + err.Error())
						}
						sock.Send(string(b), 0)

					}

				}
			} else {
				envelope.Pid = s.Pid()
				sock, ok := s.sockets[s.Pid()]
				if ok {
					//go Connect_Send(sock, envelope)
					b, err := json.Marshal(envelope)
					if err != nil {
						panic("Json error: " + err.Error())
					}
					sock.Send(string(b), 0)
				} else {
					sock, err := zmq.NewSocket(zmq.DEALER)
					if err != nil {
						panic("Socket Error " + err.Error())
					} else {
						s.sockets[s.Pid()] = sock
					}
					err = sock.Connect("tcp://" + s.addr[envelope.Pid])
					if err != nil {
						panic("Connect error " + err.Error())
					}
					//go Connect_Send(output, envelope)
					b, err := json.Marshal(envelope)
					if err != nil {
						panic("Json error: " + err.Error())
					}
					sock.Send(string(b), 0)

				}

				//go Connect_Send(s.addr[envelope.Pid], envelope)

			}
		case <-time.After(10 * time.Second):
			//			fmt.Println("shud close all socket")

		}
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
	input, err := zmq.NewSocket(zmq.DEALER)
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
