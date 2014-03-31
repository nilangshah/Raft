all :
	go install github.com/nilangshah/Raft/cluster	
	go install github.com/nilangshah/Raft
	go install github.com/nilangshah/Raft/main
	go test
	rm -rf Raftlog1 Raftlog2 Raftlog3 Raftlog4 Raftlog5 log1 log2 log3 log4 log5
