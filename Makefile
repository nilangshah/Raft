all :
	rm -rf /home/nilang/Documents/go-space/go/src/github.com/nilangshah/Raft/log*
	go install github.com/nilangshah/Raft/cluster	
	go install github.com/nilangshah/Raft
	go install github.com/nilangshah/Raft/main
	go test
