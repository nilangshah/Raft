package Raft

import (
	//"fmt"
	"github.com/nilangshah/Raft/cluster"
	"time"
	"testing"
	"math/rand"
	"os"
	
	
)
func GenerateMsg(s *cluster.SerVer){

	
	for x:=0;x<10;x++ {
	rand.Seed(time.Now().Unix())
	y:=(rand.Intn(7)-1)
	s.Outbox() <- &cluster.Envelope{Pid:y , Msg: "hello there"}
	
	time.Sleep(500 * time.Millisecond)
	
	}
}
func TestConn(t *testing.T) {
	path1 :=GetPath() +"/src/github.com/nilangshah/Raft/cluster/config.json"
	
       //var input string
	//var path string
	//path = environment["GOPATH"]+"/src/github.com/nilangshah/Raft/cluster/config.json"
	server := make([]*cluster.SerVer,5)
	for i:=1;i<6;i++{
	server[i-1]=cluster.New(i,path1 )	
	}
	 
	for i:=1; i<6;i++{
	
	go GenerateMsg(server[i-1])
	
	}
	
	for{
		select {
			
			case _= <-server[0].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",1 ,envelope.Pid, envelope.Msg)
			case  _=<-server[1].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",2 ,envelope.Pid, envelope.Msg)
			case  _=<-server[2].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",3 ,envelope.Pid, envelope.Msg)
			case  _= <-server[3].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",4 ,envelope.Pid, envelope.Msg)

			case  _= <-server[4].Inbox():
				//fmt.Printf( "%d Received msg from %d: '%s'\n",5 ,envelope.Pid, envelope.Msg)
			case <-time.After(2*time.Second):
				os.Exit(0)
			
					
			 }
		
	   }	
	

}
