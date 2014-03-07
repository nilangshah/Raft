package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	errNotSame = errors.New("Msg do not match")
)

var wg *sync.WaitGroup

var buffer bytes.Buffer

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

func BombardMsg(s Server) {
	x := 0
	for x = 0; x < 100; x++ {
		time.After(10)
		msg := "hello there " + strconv.Itoa(s.Pid()) + "." + strconv.Itoa(x)
		s.Outbox() <- &Envelope{Pid: -1, Msg: msg}

	}
	wg.Done()
	return

}

func strcmp(a, b string) int {
	var min = len(b)
	if len(a) < len(b) {
		min = len(a)
	}
	var diff int
	for i := 0; i < min && diff == 0; i++ {
		diff = int(a[i]) - int(b[i])
	}
	if diff == 0 {
		diff = len(a) - len(b)
	}
	return diff
}

func LargeMsg(s Server) {

	for i := 0; i < 100000; i++ {
		buffer.WriteString(strconv.Itoa(i))
	}
	s.Outbox() <- &Envelope{Pid: 2, Msg: buffer.String()}
	wg.Done()
}

func MatchLargeMsg(s Server) {
	for {
		select {

		case envelope := <-s.Inbox():
			a := envelope.Msg.(string)
			b := strcmp(buffer.String(), a)
			if b != 0 {
				panic(errNotSame)
			}
		case <-time.After(5 * time.Second):
			wg.Done()

		}
	}
}

var server []Server

func TestLoad(t *testing.T) {
	fmt.Println("test1: Generate large no of msges and broadcast them")
	no_of_servers := 5
	wg = new(sync.WaitGroup)
	path := GetPath() + "/src/github.com/nilangshah/Raft/cluster/config.json"
	server = make([]Server, no_of_servers)
	for i := 1; i <= no_of_servers; i++ {
		server[i-1] = New(i, path)
	}
	for i := 1; i <= no_of_servers; i++ {
		wg.Add(1)
		wg.Add(1)
		go BombardMsg(server[i-1])
		go ListenMsg(server[i-1])

	}

	wg.Wait()

}

func TestSize(t *testing.T) {
	fmt.Println("test2: Generate msg of size 488890 bytes and send to peer")
	no_of_servers := 2
	wg = new(sync.WaitGroup)
	for i := 1; i <= no_of_servers; i++ {
		if i == 1 {
			wg.Add(1)
			go LargeMsg(server[i-1])
		}
		if i == 2 {
			wg.Add(1)
			go MatchLargeMsg(server[i-1])
		}
	}

	wg.Wait()

}

func ListenMsg(s Server) {
	count := 0
	for {
		select {

		case _ = <-s.Inbox():

			count++
		case <-time.After(10 * time.Second):
			wg.Done()
			return

		}

	}

}
