Raft
====

This is a Go implementation of the Raft distributed consensus protocol. Raft is a protocol by which a cluster of nodes can maintain in sync through the use of a replicated log.

For more details on Raft, you can read In Search of an Understandable Consensus Algorithm by Diego Ongaro and John Ousterhout.

2 main events of Raft:


    1.Leader Election
    2.Replicated Log

With these two constructs, you can build a system that can maintain state across multiple servers -- even in the event of multiple failures.

[![GoDoc](https://godoc.org/github.com/nilangshah/Raft?status.png)](https://godoc.org/github.com/nilangshah/Raft)


Zeromq and Go channels are used for server communication. 

Zermq dealer Socket is used at both the end. Currently Socket is not closed after the message is sent, it will keepalive. Zeromq will handel the socket timeout and keepalive.

config.json file contains the server configuration and it should be kept in the cluster directory. 

Raft_test.go.. It will start 5 servers and wills send messsages randomly to other peers.

Load_test.go.. It will start 3 servers and will send 100000 messages to other peers.

GOPATH must have been set to run this package.

Usage:
    
    Checkout the project using : go get github.com/nilangshah/Raft
    1.To test the package
        
        go test

   2. To run the package
        
        go install github.com/nilangshah/Raft
      
        Now change config.json file to configure servers. Start all the servers using ...

        Raft -id=server-id
        
        after starting all servers are waititng for pressing enter to start communication.
        
