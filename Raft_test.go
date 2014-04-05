package Raft

import (
	"errors"
	"fmt"
	//"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	//"fmt"
	"net/rpc"
)

const (
	unknown        = 1000
	unknown_leader = 0
)

var wg *sync.WaitGroup
var nos int
var cmd []*exec.Cmd

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
	fmt.Println("test 1")
	nos = 5
	cmd = make([]*exec.Cmd, nos)
	wg = new(sync.WaitGroup)
	compeletepath := os.Getenv("GOPATH")
	path := strings.Split(compeletepath, ":")[0] + "/bin/main"

	for i := 1; i < nos+1; i++ {
		cmd[i-1] = exec.Command(path, "-id", strconv.Itoa(i))
		cmd[i-1].Start()
	}
	a := 0
	select {
	case <-time.After(1 * time.Second):

	}
	for j := 0; j < nos; j = j + 2 {
		wg.Add(1)

		//rand.Seed(time.Now().UnixNano())
		if j == nos-1 {
			a = 0
		} else {
			a = rand.Intn(2)
		}

		go KillServer(wg, cmd, j+a, true)
		wg.Wait()

	}
	select {
	case <-time.After(2 * time.Second):
		_, leader := GetLeader()
		if leader == unknown_leader {
			kill_all_server(cmd)
			panic("leader not elected")
		}

	}
	return

}

func GetLeader() (uint64, uint64) {

	select {
	case <-time.After(2 * time.Second):
		term, leader, err := rpc_call()
		if err != nil || leader == 0 {
			return GetLeader()

		} else {

			//log.Println("New leader is",leader)
			return term, leader
		}

	}

}

//rpc call to ask latest term and current leader to all available servers
func rpc_call() (uint64, uint64, error) {
	var term uint64
	term = unknown
	var leader uint64
	leader = unknown_leader
	var reply []LeaderInfo
	port := 12345
	reply = make([]LeaderInfo, nos)
	for i := 1; i < nos+1; i++ {

		client, err := rpc.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port+i))
		if err != nil {
		} else {
			arg := &Args{1, 2}

			err = client.Call("RaftTest.LeaderInfo", arg, &reply[i-1])
			if err != nil {
				return unknown, unknown_leader, err
			}
			if term == unknown || leader == unknown {
				term = reply[i-1].Term
				leader = reply[i-1].Pid

			} else {
				if leader != reply[i-1].Pid {
					fmt.Println(term, reply[i-1].Term, leader, reply[i-1].Pid)
					// kill_all_server(cmd)
					//fmt.Println("diffrent leader exist")
					return unknown, unknown_leader, errors.New("leader not exist")

				}

			}
			//log.Println(reply[i-1])
			client.Close()
		}

	}
	return term, leader, nil

}

func KillServer(wg *sync.WaitGroup, cmd []*exec.Cmd, j int, restart bool) {
	path := GetPath() + "/bin/main"
	fmt.Println("kill leader ",j+1)
	cmd[j].Process.Kill()
	cmd[j].Wait()
	wg.Done()
	if restart {
		select {
		case <-time.After(2 * time.Second):
			cmd[j] = exec.Command(path, "-id", strconv.Itoa(j+1))
			cmd[j].Start()
		}
	}
}

//start server and check whether leader has elected in next 2sec.
func TestElection_2(t *testing.T) {
	fmt.Println("test 2")

	wg = new(sync.WaitGroup)

	_, killed_Leader, err := rpc_call()
	if err != nil {
		kill_all_server(cmd)
		panic("test failed")
	}

	//fmt.Println("Kill Leader", killed_Leader)

	wg.Add(1)
	go KillServer(wg, cmd, int(killed_Leader-1), false)
	wg.Wait()
	var new_Leader uint64
	new_Leader = unknown
	_, new_Leader = GetLeader()
	if new_Leader == killed_Leader {
		fmt.Println(new_Leader, " ", killed_Leader)
		kill_all_server(cmd)
		panic("no leader not elected")

	}

	path := GetPath() + "/bin/main"
	cmd[int(killed_Leader)-1] = exec.Command(path, "-id", strconv.Itoa(int(killed_Leader)))
	cmd[int(killed_Leader)-1].Start()
	select {
	case <-time.After(2 * time.Second):

	}

	return

}

//5 servers running kill leader, so 4 server will elect new leader , now again kill leader , 3 server will elect new leader, kill 1 more time leader, now only 2 server exist and leader should not be elected
func TestElection_3(t *testing.T) {
	fmt.Println("test 3")
	nos = 5
	wg = new(sync.WaitGroup)
	for i := 0; i < 3; i++ {

		_, killed_Leader := GetLeader()

		//		fmt.Println("Kill Leader", killed_Leader)

		wg.Add(1)
		KillServer(wg, cmd, int(killed_Leader-1), false)
		wg.Wait()

	}

	select {
	case <-time.After(1 * time.Second):
		final_term, final_Leader, err := rpc_call()
		if err != nil {
			fmt.Println(final_term, final_Leader)
			kill_all_server(cmd)
			panic("leader still exist")
		}

	}

	kill_all_server(cmd)
	return

}

func kill_all_server(cmd []*exec.Cmd) {
	for i := 1; i < nos+1; i++ {
		cmd[i-1].Process.Kill()
		cmd[i-1].Wait()
	}
}
