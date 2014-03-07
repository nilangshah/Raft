package Raft

import (
	//"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"
	//"fmt"
	"net/rpc"
)

var wg *sync.WaitGroup
var nos int

type Args struct {
	X, Y int
}
type LeaderInfo struct {
	Term uint64
	//id of the candidate
	Pid uint64
}

//start 5 servers and kill randomly 3 servers and after 5 second start them..
func TestElection_1(t *testing.T) {
	nos = 5
	cmd := make([]*exec.Cmd, nos)
	wg = new(sync.WaitGroup)
	path := os.Getenv("GOPATH") + "/bin/main"

	for i := 1; i < 6; i++ {
		cmd[i-1] = exec.Command(path, "-id", strconv.Itoa(i))
		cmd[i-1].Start()
	}
	select {
	case <-time.After(2 * time.Second):
	}
	for j := 0; j < 5; j = j + 2 {
		wg.Add(1)
		go KillServer(wg, cmd, j)
		wg.Wait()

	}
	var reply []LeaderInfo
	select {
	case <-time.After(15 * time.Second):
		port := 12345
		reply = make([]LeaderInfo, 5)
		for i := 1; i < 6; i++ {

			client, err := rpc.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port+i))
			if err != nil {
			} else {
				arg := &Args{1, 2}

				err = client.Call("RaftTest.LeaderInfo", arg, &reply[i-1])
				if err != nil {
					log.Fatal("communication error:", err)
				}
			}
			client.Close()
		}

		if (reply[0].Term == reply[1].Term) && (reply[2].Term == reply[3].Term) && (reply[4].Term == reply[1].Term) && (reply[0].Pid == reply[1].Pid) && (reply[2].Pid == reply[3].Pid) && (reply[4].Pid == reply[1].Pid) {
		} else {
			panic("test failed")
		}
	}
	select {
	case <-time.After(5 * time.Second):
		for i := 1; i < 6; i++ {
			cmd[i-1].Process.Kill()
			cmd[i-1].Wait()
		}
	}

	return

}

func KillServer(wg *sync.WaitGroup, cmd []*exec.Cmd, j int) {
	path := os.Getenv("GOPATH") + "/bin/main"
	a := 0
	rand.Seed(time.Now().Unix())
	if j == 4 {
		a = 0
	} else {
		a = rand.Intn(2)
	}
	cmd[j+a].Process.Kill()
	cmd[j+a].Wait()
	wg.Done()
	select {
	case <-time.After(5 * time.Second):
		cmd[j+a] = exec.Command(path, "-id", strconv.Itoa(j+a+1))
		cmd[j+a].Start()
	}

}
