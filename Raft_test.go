package Raft

import (
	"github.com/nilangshah/Raft/cluster"
	"log"
	"os"
	"testing"
	"time"
	"strings"
	"math/rand"
	"fmt"
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
	vals := strings.Split(newval,":")
	val = vals[0]
	return
}
var repliCator []Replicator
//var f *File
// Stop leader 3 times at interval of 10 seconds... only 2 server will left and no leader present... start again 2 server and new leader will be elected. 
func TestElection_1(t *testing.T) {
	fmt.Println("test 1 started")
	logfilePath := GetPath() + "/src/github.com/nilangshah/Raft/log"
	f, err := os.OpenFile(logfilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		//t.Fatalf("error opening file: %v", err)
	} else{
	defer f.Close()
	log.SetOutput(f)
	}
	no_of_servers = 5
	quorum = ((no_of_servers - 1) / 2) + 1
	out := make(chan int)
	path := GetPath() + "/src/github.com/nilangshah/Raft/cluster/config.json"
	server := make([]cluster.Server, no_of_servers)
	repliCator = make([]Replicator, no_of_servers)
	for i := 1; i <= no_of_servers; i++ {
		server[i-1] = cluster.New(i, path)
		repliCator[i-1] = New(server[i-1], path)
		repliCator[i-1].Start()
	}
	i1, i2, i3 := -1, -1, -1
	for j := 0; j < 3; j++ {
		select {
		case _ = <-out:

		case <-time.After(5 * time.Second):
			for i := range repliCator {
				if repliCator[i].IsLeader() {
					i1 = i2
					i2 = i3
					i3 = i
					//log.Println("server ", server[i].Pid(), " is killed")
					repliCator[i].IsLeaderSet(false)
					repliCator[i].Stop()
				}
			}

		}
	}
	for j := 0; j < 3; j++ {
		select {
		case _ = <-out:

		case <-time.After(5 * time.Second):
			repliCator[i1].Start()
			i1 = i2
			i2=i3
		}
	}
	select{
	case <-out:
	case <-time.After(10*time.Second):
		return
	}
}


func killOneServer(replicator *[]Replicator){

	n:=rand.Intn(no_of_servers)
	if (*replicator)[n].IsRunning(){
		(*replicator)[n].Stop()
		select{
			case <- time.After(1*time.Second):
				(*replicator)[n].Start()	
		}
	}
}


// stop any server randomly after each 1 second and start it after 1 second.
func TestElection_2(t *testing.T){
	fmt.Println("test 2 started")
	logfilePath := GetPath() + "/src/github.com/nilangshah/Raft/log"
	f, err := os.OpenFile(logfilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		//t.Fatalf("error opening file: %v", err)
	} else{
	defer f.Close()
	log.SetOutput(f)
	}
	for count:=0;count<20;count++{
		select{
			case <-time.After(1*time.Second):
				go killOneServer(&repliCator)
				
		}	
	}

}



