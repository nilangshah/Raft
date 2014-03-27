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
		logfile := os.Getenv("GOPATH") + "/src/github.com/nilangshah/Raft/Raftlog/log" + strconv.Itoa(i+1)
		f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(fmt.Sprintf("error opening file: %v", err))
		} else {
			//defer f.Close()

		}
		server[i] = cluster.New(i+1, path)
		replicator[i] = Raft.New(server[i], f, path)
		replicator[i].Start()
	}
	leader_id := 0
	select {
	case <-time.After(5 * time.Second):
		for i := 0; i < 5; i++ {
			leader_id = int(replicator[i].GetLeader())
		}

	}
	response := make(chan<- []byte)
	command := Raft.CommandTuple{Command: []byte("hello how r you?"), CommandResponse: response}
	replicator[leader_id-1].Outbox() <- command
	select {
	case <-time.After(5 * time.Second):
		fmt.Println("raft run done")
	}
	<-out

}