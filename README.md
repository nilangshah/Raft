Raft
====

Raft Consensus in go


Zeromq and Go channels are used for server communication. 

Zermq dealer Socket is used at both the end. Currently Socket is not closed after the message is sent, it will keepalive. But in future it can be closed if connection is idle for some time say 10-15sec.

config.json file contains the server configuration and it should be kept in the cluster directory. 

Raft_test.go.. It will start 5 servers and wills send messsages randomly to other peers.

Load_test.go.. It will start 3 servers and will send 5000 messages to other peers.

GOPATH must have been set to run this package.

Usage:
    
    Checkout the project using : go get github.com/nilangshah/Raft
    1.To test the package
        
        go test

   2. To run the package
        
        go install github.com/nilangshah/Raft
      
        Now change config.json file to configure servers. Start all the servers using ...

        Raft -id=server-id
        
        after starting all servers press enter to start the communication
        
