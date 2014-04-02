package main

import (
	"fmt"
	"github.com/nilangshah/Raft"
	"github.com/nilangshah/Raft/cluster"
	"os"
	"strconv"
	"strings"
	"time"
)

func GetPath() string {
	data := os.Environ()
	for _, item := range data {
		key, val := getkeyval(item)
		if key == "GOPATH" {
			return val
		}
	}
	return ""
}

func getkeyval(item string) (key, val string) {
	splits := strings.Split(item, "=")
	key = splits[0]
	newval := strings.Join(splits[1:], "=")
	vals := strings.Split(newval, ":")
	val = vals[0]
	return
}

func main() {
	fmt.Println("hello")
	out := make(chan int)
	path := GetPath() + "/src/github.com/nilangshah/Raft/cluster/config.json"
	replicator := make([]Raft.Replicator, 5)
	server := make([]cluster.Server, 5)

	for i := 0; i < 5; i++ {
		fmt.Println("server", i, "started")
		logfile := os.Getenv("GOPATH") + "/src/github.com/nilangshah/Raft/Raftlog" + strconv.Itoa(i+1)
		
		server[i] = cluster.New(i+1, path)
		replicator[i] = Raft.New(server[i],logfile)
		replicator[i].Start()
	}
	leader_id := 0
	select {
	case <-time.After(5 * time.Second):
		for i := 0; i < 5; i++ {
			leader_id = int(replicator[i].GetLeader())
			fmt.Println("leader found")
		}

	}
	for i := 0; i < 10; i++ {
		response := make(chan bool)
		command := Raft.CommandTuple{Command: []byte("This is "+strconv.Itoa(i)+ "th message"), CommandResponse: response}
		replicator[leader_id-1].Outbox() <- command
		fmt.Println("command send")
		select {
		case t := <-response:
			fmt.Println(t, "comited received")

		}
	}
	<-out
}
