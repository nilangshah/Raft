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
	unknown = 1000
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
	select {
	case <-time.After(2 * time.Second):
	}
	a := 0
	for j := 0; j < 5; j = j + 2 {
		wg.Add(1)

		rand.Seed(time.Now().Unix())
		if j == 4 {
			a = 0
		} else {
			a = rand.Intn(2)
		}

		go KillServer(wg, cmd, j+a, true)
		wg.Wait()

	}
	var term_Leader uint64
	term_Leader = unknown
	var pid_Leader uint64
	pid_Leader = unknown
	var reply []LeaderInfo
	select {
	case <-time.After(8 * time.Second):
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
						panic("diffrent term or leader exist")
						for i := 1; i < 6; i++ {
							cmd[i-1].Process.Kill()
							cmd[i-1].Wait()
						}
					}

				}

				client.Close()
			}

		}

	}

	return

}

func KillServer(wg *sync.WaitGroup, cmd []*exec.Cmd, j int, restart bool) {
	path := GetPath() + "/bin/main"
	//fmt.Println("kill server ",j)
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
	var reply []LeaderInfo
	port := 12345
	reply = make([]LeaderInfo, 5)
	for i := 1; i < 6; i++ {

		client, err := rpc.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port+i))
		if err != nil {
			log.Fatal("rpc error")
		} else {
			arg := &Args{1, 2}

			err = client.Call("RaftTest.LeaderInfo", arg, &reply[i-1])
			if err != nil {
				log.Fatal("communication error:", err)
			}
			fmt.Println("Server id ", i-1, " Term ", reply[i-1].Term, " Leaderid", reply[i-1].Pid)
			client.Close()
		}

	}

	fmt.Println("Kill Leader", reply[0].Pid)
	killed_Leader := reply[0].Pid
	wg.Add(1)
	go KillServer(wg, cmd, int(reply[0].Pid-1), false)
	wg.Wait()
	var new_Leader uint64
	new_Leader = unknown
	select {
	case <-time.After(2 * time.Second):
		for i := 1; i < 6; i++ {

			client, err := rpc.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port+i))
			if err != nil {
				//log.Fatal("rpc error")
			} else {
				arg := &Args{1, 2}

				err = client.Call("RaftTest.LeaderInfo", arg, &reply[i-1])
				if err != nil {
					log.Fatal("communication error:", err)
				}
				fmt.Println("Server id ", i-1, " Term ", reply[i-1].Term, "new Leaderid", reply[i-1].Pid)
				if new_Leader == unknown {
					new_Leader = reply[i-1].Pid
				} else {
					if new_Leader != reply[i-1].Pid {
						fmt.Println("diffrent leader exist", reply[i-1].Term, reply[i-1].Pid, new_Leader)
						panic("diff leaders")
						for i := 1; i < 6; i++ {
							cmd[i-1].Process.Kill()
							cmd[i-1].Wait()
						}
					}

				}
				client.Close()
			}

		}
		if new_Leader == killed_Leader {
			fmt.Println(new_Leader, " ", killed_Leader)
			panic("no leader not elected")
			for i := 1; i < 6; i++ {
				cmd[i-1].Process.Kill()
				cmd[i-1].Wait()
			}
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
