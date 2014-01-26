package Raft

import (
	//"fmt"
	"github.com/nilangshah/Raft/cluster"
	"time"
	"testing"
	"math/rand"
	"os"
	"strings"
	
	
)

func GetPath() string{
		data:=os.Environ()
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


func BombardMsg(s *cluster.SerVer){

	
	for x:=0;x<5000;x++ {
	rand.Seed(time.Now().Unix())
	y:=(rand.Intn(4)-1)
	s.Outbox() <- &cluster.Envelope{Pid:y , Msg: "hello there"}
	//time.Sleep(15 * time.Millisecond)
	
	}
}
func TestLoad(t *testing.T) {
	
	
       //var input string
	path :=GetPath() +"/src/github.com/nilangshah/Raft/cluster/config.json"
	server := make([]*cluster.SerVer,3)
	for i:=1;i<4;i++{
	server[i-1]=cluster.New(i,path )	
	}
	 
	for i:=1; i<4;i++{
	
	go BombardMsg(server[i-1])
	
	}
	
	for{
		select {
			
			case _= <-server[0].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",1 ,envelope.Pid, envelope.Msg)
			case  _=<-server[1].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",2 ,envelope.Pid, envelope.Msg)
			case  _=<-server[2].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",2 ,envelope.Pid, envelope.Msg)
			
			case <-time.After(2*time.Second):
				os.Exit(0)
					
			 }
		
	   }	
	

}
