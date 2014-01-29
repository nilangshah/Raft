package Raft

import (
	"flag"
	"fmt"
	"github.com/nilangshah/Raft/cluster"
	"os"
	"strings"
)

var GoPath string

func Init() {
	getenvironment := func(data []string, getkeyval func(item string) (key, val string)) {
		for _, item := range data {
			key, val := getkeyval(item)
			if key == "GOPATH" {
				GoPath = val
				return
			}
		}
	}
	getenvironment(os.Environ(), func(item string) (key, val string) {
		splits := strings.Split(item, "=")
		key = splits[0]
		val = strings.Join(splits[1:], "=")
		return
	})

}

func main() {
	myid := flag.Int("id", 1, "a int")
	flag.Parse()

	// parse argument flags and get this server's id into myid
	var input string
	path := GoPath + "/src/github.com/nilangshah/Raft/cluster/config.json"
	server := cluster.New(*myid, path)
	// the returned server object obeys the Server interface above.
	fmt.Scanln(&input)
	//wait for keystroke to start.
	// Let each server broadcast a message
	server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, Msg: "hello there"}

	if *myid == 1 {
		server.Outbox() <- &cluster.Envelope{Pid: 2, Msg: "hello bye"}
	}
	for {

		envelope := <-server.Inbox()
		fmt.Printf("Received msg from %d: '%s'\n", envelope.Pid, envelope.Msg)
	}

}
