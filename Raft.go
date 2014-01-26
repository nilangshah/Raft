package main

import (
	"fmt"
	"github.com/nilangshah/Raft/cluster"
	"flag"
	"time"
)
func GenerateMsg(s *cluster.SerVer){
	for  {
	s.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: "hello there"}
	time.Sleep(3 * time.Second)
	
	}
}
func main() {
	myid := flag.Int("id", 1, "a int")
	flag.Parse()
	
	// parse argument flags and get this server's id into myid
	var input string
	server := cluster.New(*myid, "config.json")
	// the returned server object obeys the Server interface above.
	fmt.Scanln(&input)
	//wait for keystroke to start.
	// Let each server broadcast a message
	go GenerateMsg(server)
	
	if *myid == 1 {
		server.Outbox() <- &cluster.Envelope{Pid: 2, Msg: "hello bye"}
	}
	for {

		envelope := <-server.Inbox()
		fmt.Printf("Received msg from %d: '%s'\n", envelope.Pid, envelope.Msg)
	}

}
