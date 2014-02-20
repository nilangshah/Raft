Raft
====

This is a Go implementation of the Raft distributed consensus protocol. Raft is a protocol by which a cluster of nodes can maintain in sync through the use of a replicated log.

For more details on Raft, you can read [In Search of an Understandable Consensus Algorithm by Diego Ongaro and John Ousterhout][s_src].
 [s_src]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

2 main events of Raft:

    1.Leader Election
    2.Replicated Log
    
With these two constructs, you can build a system that can maintain state across multiple servers -- even in the event of multiple failures.

Raft elects leader and replicate log entires to guarantees that the responses seen by clients of the cluster will be consistent EVEN in the phase of servers crashing in unpredictable ways (but not loosing data that was synched to disk), and networks introducing unpredictable delays or communication blockages.


[![GoDoc](https://godoc.org/github.com/nilangshah/Raft?status.png)](https://godoc.org/github.com/nilangshah/Raft)

TODO
====

* ~~Leader election~~ Done
* Log replication 
* ~~Basic unit tests~~ Done
* ~~Tcp transport~~ Done
* Configuration changes
* Log compaction
* Complex unit tests
* Robust application on top of Raft
    
Usage
===== 
[![Build Status](https://travis-ci.org/nilangshah/Raft.png?branch=master)](https://travis-ci.org/nilangshah/Raft)

* We need to setup zmq 4.0.3 first
 * sudo apt-get install autoconf
 * sudo apt-get install libtool
 * wget http://download.zeromq.org/zeromq-4.0.3.tar.gz
 * tar -xvzf zeromq-4.0.3.tar.gz
 * cd zeromq-4.0.3/ 
 * ./configure
 * make
 * sudo make install
 * go get github.com/pebbe/zmq4
* Checkout project from github
 * go get github.com/nilang.shah/Raft
* Run tests to test Raft Package
 * go test github.com/nilangshah/Raft
* Run tests to test cluster package
 * go test github.com/nilangshah/Raft/cluster

Zeromq is used for massage passing. Cluster Packge is build for peer to peer massgae passing. Raft package uses it for leader election and log replication.
