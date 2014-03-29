package Raft

import (
	"fmt"
	"log"
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
	unknwon_leader = 0
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
	//	val = strings.Join(splits[1:], "=")
	newval := strings.Join(splits[1:], "=")
	vals := strings.Split(newval, ":")
	val = vals[0]

	return
}

//start 5 servers and kill randomly 3 servers and after 5 second start them..
func TestElection_1(t *testing.T) {
	nos = 5
	cmd = make([]*exec.Cmd, nos)
	wg = new(sync.WaitGroup)
	compeletepath := os.Getenv("GOPATH")
	path := strings.Split(compeletepath, ":")[0] + "/bin/main"

	for i := 1; i < 6; i++ {
		cmd[i-1] = exec.Command(path, "-id", strconv.Itoa(i))
		cmd[i-1].Start()
	}
	a := 0
	select {
	case <-time.After(3 * time.Second):

	}
	for j := 0; j < 5; j = j + 2 {
		wg.Add(1)

		//rand.Seed(time.Now().UnixNano())
		if j == 4 {
			a = 0
		} else {
			a = rand.Intn(2)
		}

		go KillServer(wg, cmd, j+a, true)
		wg.Wait()

	}

	select {
	case <-time.After(8 * time.Second):
		rpc_call()

	}

	return

}

//rpc call to ask latest term and current leader to all available servers
func rpc_call() (uint64, uint64) {
	var term_Leader uint64
	term_Leader = unknown
	var pid_Leader uint64
	pid_Leader = unknown
	var reply []LeaderInfo
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
			if term_Leader == unknown || pid_Leader == unknown {
				term_Leader = reply[i-1].Term
				pid_Leader = reply[i-1].Pid

			} else {
				if term_Leader != reply[i-1].Term || pid_Leader != reply[i-1].Pid {
					fmt.Println(term_Leader, reply[i-1].Term, pid_Leader, reply[i-1].Pid)
					// kill_all_server(cmd)
					fmt.Println("diffrent term or leader exist")

				}

			}
			//log.Println(reply[i-1])
			client.Close()
		}

	}
	return term_Leader, pid_Leader

}

func KillServer(wg *sync.WaitGroup, cmd []*exec.Cmd, j int, restart bool) {
	path := GetPath() + "/bin/main"
	fmt.Println("kill server ", j+1)
	cmd[j].Process.Kill()
	cmd[j].Wait()
	wg.Done()
	if restart {
		select {
		case <-time.After(5 * time.Second):
			cmd[j] = exec.Command(path, "-id", strconv.Itoa(j+1))
			cmd[j].Start()
		}
	}
}

//start server and check whether leader has elected in next 2sec.
func TestElection_2(t *testing.T) {
	fmt.Println("test 2")
	nos = 5
	wg = new(sync.WaitGroup)

	select {
	case <-time.After(2 * time.Second):
	}

	_, killed_Leader := rpc_call()

	//fmt.Println("Kill Leader", killed_Leader)

	wg.Add(1)
	go KillServer(wg, cmd, int(killed_Leader-1), false)
	wg.Wait()
	var new_Leader uint64
	new_Leader = unknown
	select {
	case <-time.After(2 * time.Second):

		_, new_Leader = rpc_call()

		if new_Leader == killed_Leader {
			fmt.Println(new_Leader, " ", killed_Leader)
			kill_all_server(cmd)
			panic("no leader not elected")

		}
	}
	path := GetPath() + "/bin/main"
	select {
	case <-time.After(1 * time.Second):
		cmd[int(killed_Leader)-1] = exec.Command(path, "-id", strconv.Itoa(int(killed_Leader)))
		cmd[int(killed_Leader)-1].Start()
	}

	return

}

//5 servers running kill leader, so 4 server will elect new leader , now again kill leader , 3 server will elect new leader, kill 1 more time leader, now only 2 server exist and leader should not be elected
func TestElection_3(t *testing.T) {
	fmt.Println("test 3")
	nos = 5
	wg = new(sync.WaitGroup)
	for i := 0; i < 3; i++ {
		select {
		case <-time.After(7 * time.Second):
		}

		_, killed_Leader := rpc_call()

		//		fmt.Println("Kill Leader", killed_Leader)

		wg.Add(1)
		KillServer(wg, cmd, int(killed_Leader-1), false)
		wg.Wait()
		var new_Leader uint64
		new_Leader = unknown
		select {
		case <-time.After(7 * time.Second):

			_, new_Leader = rpc_call()

			if new_Leader == killed_Leader {
				fmt.Println(new_Leader, " ", killed_Leader)
				kill_all_server(cmd)
				panic("no leader not elected")

			}
		}
	}
	select {
	case <-time.After(1 * time.Second):
		final_term, final_Leader := rpc_call()
		if final_Leader == unknwon_leader {
			fmt.Println("Term", final_term, "Leader", final_Leader)
		} else {
			kill_all_server(cmd)
			panic("leader still exist")
		}
	}
	/*path := GetPath() + "/bin/main"
	select {
	case <-time.After(1 * time.Second):
		cmd[int(killed_Leader)-1] = exec.Command(path, "-id", strconv.Itoa(int(killed_Leader)))
		cmd[int(killed_Leader)-1].Start()
	}
	*/
	select {
	case <-time.After(2 * time.Second):
		kill_all_server(cmd)
	}
	return

}

func kill_all_server(cmd []*exec.Cmd) {
	for i := 1; i < 6; i++ {
		cmd[i-1].Process.Kill()
		cmd[i-1].Wait()
	}

}
