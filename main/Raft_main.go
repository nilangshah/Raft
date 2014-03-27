package main

import (
	"flag"
	"fmt"
	"github.com/nilangshah/Raft"
	"github.com/nilangshah/Raft/cluster"
	"log"
	"net"
	//"net/http"
	"strconv"
	"net/rpc"
	"os"
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

var server1 cluster.Server

type Args struct {
	X, Y int
}

type RaftTest struct{}

type LeaderInfo struct {
	Term uint64
	//id of the candidate
	Pid uint64
} 

//var linfo *LeaderInfo
var replicator Raft.Replicator

func (t *RaftTest) LeaderInfo(args *Args, reply *LeaderInfo) error {
	//log.Println(replicator.Term(),replicator.GetLeader(),replicator.IsLeader())
	reply.Term=replicator.Term()
	reply.Pid=replicator.GetLeader()
	return nil
}
var port int
func main() {

	myid := flag.Int("id", 1, "a int")
	flag.Parse()
	port=12345
	//var input string
	
		port=port+*myid
		Cal := new(RaftTest)
		
		rpc.Register(Cal)
		listener, e := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if e != nil {
			log.Fatal("listen error:", e)
		}
		go func() {
			for {
				if conn, err := listener.Accept(); err != nil {
					log.Fatal("accept error: " + err.Error())
				} else {
					log.Printf("new connection established\n")
					go rpc.ServeConn(conn)
				}
			}
		}()
	
	logfile := os.Getenv("GOPATH") + "/src/github.com/nilangshah/Raft/Raftlog/log" + strconv.Itoa(*myid)
		f, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(fmt.Sprintf("error opening file: %v", err))
		}
	path := GetPath() + "/src/github.com/nilangshah/Raft/cluster/config.json"
	server1 = cluster.New(*myid, path)
	replicator = Raft.New(server1,f, path)
	replicator.Start()
	fmt.Println(*myid)
		
	//fmt.Scanln(&input)

	select {
	case <-time.After(100 * time.Second):
		fmt.Println(replicator.IsRunning(), replicator.Term())

	}
}
