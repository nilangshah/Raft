Raft
====

This is a Go implementation of the Raft distributed consensus protocol. Raft is a protocol by which a cluster of nodes can maintain in sync through the use of a replicated log.

For more details on Raft, you can read [In Search of an Understandable Consensus Algorithm by Diego Ongaro and John Ousterhout][s_src].
 [s_src]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

2 main events of Raft:

    1.Leader Election
    2.Replicated Log
   
### Leader Election

The Raft protocol work as master-slave mechenism where master assign task to slave. Here, master is called as leader and all servers at start choose a leader among themselves. This leader will serve the client. 
Raft ensure that there will be only one leader at a time. This time is known as term, which is an integer which keeps on incresing when leader fail. 

Quorum is the very crucial in Raft. To win the leader election candidate must have get enough votes to meet qourum. Most of the cases this Quorum is n/2+1, where n is no of servers in cluster.

### Replicated Log

To maintain state, a log of commands is maintained.
Each command needs to be apply to change the state of the server and tis command is idempotent and commutative. So it will not matter even if you apply it more then once.
Log replication is also based on quorum. Leader send logentry to servers and if quorum has been made then only that log entry willl be commited.
Replicating the log under normal conditions is done by sending an `AppendEntries` from the leader to each of the other followers in the cluster.
Each follower will append the entries from the leader through a 2-phase commit process which ensure that a majority of followers in the cluster have entries written to log.
   
    
With these two constructs, you can build a system that can maintain state across multiple servers -- even in the event of multiple failures.

Raft elects leader and replicate log entires to guarantees that the responses seen by clients of the cluster will be consistent EVEN in the phase of servers crashing in unpredictable ways (but not loosing data that was synched to disk), and networks introducing unpredictable delays or communication blockages.


[![GoDoc](https://godoc.org/github.com/nilangshah/Raft?status.png)](https://godoc.org/github.com/nilangshah/Raft)

Features
====

* Leader election
* Log replication
* Basic unit tests
* Tcp transport using zeromq
* Log compaction done using LevelDB
* Http frontend
  
Requirements
============
* Need alteast go1.1 or newer
* Need zeromq-4.0.3 
* Need leveldb
(see usage for more info on zeromq and leveldb)

Packages
========
* Zeromq is used for massage passing.
* Cluster Packge is build for peer to peer massgae passing using zeromq.
* Raft package is built on top of cluster package and Raft uses it for leader election and log replication.

Usage
===== 
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
* Now we need to install leveldb 
 * go get github.com/syndtr/goleveldb/leveldb 
* Checkout project from github
 * go get github.com/nilang.shah/Raft
* Run tests to test Raft Package
 * make
* Run tests to test cluster package
 * go test github.com/nilangshah/Raft/cluster
 
Advance Usage
=============
* Change config.xml file in the cluster directory to change the number of servers and url
* Tests are written for 5 severs only , so appropriat changes needs to be done when you change the number of servers
* If you have any question file an issue and I will reach you as soon as I can.
