package Raft

import (
	"github.com/nilangshah/Raft/cluster"
	"log"
	"os"
	"testing"
	"time"
	"strings"
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
	val = strings.Join(splits[1:], "=")
	return
}


func TestElection(t *testing.T) {
	logfilePath := GetPath() + "/src/github.com/nilangshah/Raft/log"
	f, err := os.OpenFile(logfilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)
	no_of_servers = 5
	quorum = ((no_of_servers - 1) / 2) + 1
	out := make(chan int)
	path := GetPath() + "/src/github.com/nilangshah/Raft/cluster/config.json"
	server := make([]cluster.Server, no_of_servers)
	replicator := make([]Replicator, no_of_servers)
	for i := 1; i <= no_of_servers; i++ {
		server[i-1] = cluster.New(i, path)
		replicator[i-1] = New(server[i-1], path)
		replicator[i-1].Start()
	}
	i1, i2, i3 := -1, -1, -1
	for j := 0; j < 3; j++ {
		select {
		case _ = <-out:

		case <-time.After(5 * time.Second):
			for i := range replicator {
				if replicator[i].IsLeader() {
					i1 = i2
					i2 = i3
					i3 = i
					log.Println("server ", server[i].Pid(), " is killed")
					replicator[i].IsLeaderSet(false)
					replicator[i].Stop()
				}
			}

		}
	}
	for j := 0; j < 2; j++ {
		select {
		case _ = <-out:

		case <-time.After(5 * time.Second):
			replicator[i1].Start()
			i1 = i2
		}
	}
	select{
	case <-out:
	case <-time.After(10*time.Second):
		return
	}
}
