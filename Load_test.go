package Raft

import (
	"fmt"
	"github.com/nilangshah/Raft/cluster"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
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
	val = strings.Join(splits[1:], "=")
	return
}

func BombardMsg(s cluster.Server) {

	for x := 0; x < 100000; x++ {
		//fmt.Println("x:",x)
		rand.Seed(time.Now().Unix())
		y := (rand.Intn(3))
		msg := "hello there " + strconv.Itoa(s.Pid()) + "." + strconv.Itoa(x)
		if y > 0 {
		} //fmt.Println("length is :",len(s.Outbox()))
		s.Outbox() <- &cluster.Envelope{Pid: y, Msg: msg}
		//if x%10 == 0 {
		//		time.Sleep(50*time.Millisecond)
		//	}

	}
}
func TestLoad(t *testing.T) {

	//var input string
	path := GetPath() + "/src/github.com/nilangshah/Raft/cluster/config.json"
	server := make([]cluster.Server, 3)
	for i := 1; i < 4; i++ {
		server[i-1] = cluster.New(i, path)
	}

	for i := 1; i < 4; i++ {
		//fmt.Println(">>>>>>>>>>>>>>     ",server[i-1].Pid())
		go BombardMsg(server[i-1])

	}
	count := 0
	for {
		select {

		case _ = <-server[0].Inbox():
			count++
			//fmt.Printf( "%d Received msg from %d: '%s'\n",1 ,envelope.Pid, envelope.Msg)
		case _ = <-server[1].Inbox():
			count++
			//fmt.Printf( "%d Received msg from %d: '%s'\n",2 ,envelope.Pid, envelope.Msg)
		case _ = <-server[2].Inbox():
			count++
			//fmt.Printf( "%d Received msg from %d: '%s'\n",3 ,envelope.Pid, envelope.Msg)

		case <-time.After(2 * time.Second):
			fmt.Println(count, "        ")
			os.Exit(0)

		}

	}

}
