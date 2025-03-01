package main

import (
	"net/rpc"
	"sync"
	"time"
)

var Bullies []ServerConnection // all of the bully nodes
var BullyIDS []int

// BullyNode represents a single node in the cluster
type BullyNode struct {
	mutex  sync.Mutex // prevent race condition when multiple nodes tries to access shared data at once
	selfID int

	leaderID int
	myPort   string
	state    string //"Normal", "Election", "Leader"

	notifiedAllHigherNodes bool

	electionTimeout         *time.Timer
	electionTimeoutDuration time.Duration
	electionResultChan      chan int // a channerl to receive election results
}

// BullyMessageUpper when a node realizes that the leader is potentially failing,
// (Will they first send a message to the current leader? and wait until the timer is up?)
type BullyMessageUpper struct {
	upperID        []int
	upperResponded bool // True if at least one higher-ID node responded
}

// ServerConnection represents a connection to another node in the Raft cluster in the Raft cluster
// Need to think more about this design but theoretically unlike the Ring one, the bully is a complete graph
type ServerConnection struct {
	serverID      int
	address       string
	rpcConnection *rpc.Client
}

// -----------------------------------------------------------------------------
// Leader Election
// -----------------------------------------------------------------------------

/*
okay how should we do leader election in the bully algorithm?
1. after a set amout of time (let's have it as 3 times the mount of time that usual leader check in with the worker) if the worker
does hear from the leader, it will send a message to the leader. After the same set amout of time, if the worker still doesn't
hear anything back, it will send message to all nodes higher than itself.

2. When a node receive a message from a lower node about the failure of the current leader, it will mark leader as Null and send "ok"
back to the node that reached out. Then it will proceed to send message to all nodes above itself.

3. In case that it hears back from no higher nodes, then it will send the message to all lower nodes that they are the leader.

4. Upon receiving such message, all lower nodes will set leader node to the current leader.

*/

// regular leader wellness check
func (node *BullyNode) monitorLeader() error {
	return nil
}

func (node *BullyNode) messageLeader() error {
	return nil
}

// election process
func (node *BullyNode) startElection() error {
	return nil
}

func (node *BullyNode) sendElectionMessages() error {
	return nil
}

func (node *BullyNode) handleElectionTimeout() error {
	return nil
}

// handeling messages
func (node *BullyNode) replyOK() error {
	return nil
}

func (node *BullyNode) receiveOK() error {
	return nil
}

func (node *BullyNode) updateLeaerID() error {
	return nil
}

func (node *BullyNode) respondsToWellnessCheck() error {
	return nil
}

// leader functions
func (node *BullyNode) leaderDecision() error {
	return nil
}

func (node *BullyNode) leaderDeclaration() error {
	return nil
}
