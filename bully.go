package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// BullyNode represents a node in the distributed system
type BullyNode struct {
	mutex         sync.Mutex
	selfID        int
	leaderID      int
	myPort        string
	otherNodes    []ServerConnection
	nominatedSelf bool
	receiveChan   chan Message
	electionChan  chan Message
	isLeader      bool
}

// Message interface for different message types
type Message interface{}

// ElectionMessage represents messages exchanged during election
type ElectionMessage struct {
	SenderID int
}

// CoordinatorMessage is sent when a node declares itself leader
type CoordinatorMessage struct {
	LeaderID int
}

// OKMessage is sent in response to an Election message
type OKMessage struct {
	SenderID int
}

// ServerConnection represents a connection to another node
type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

// StartElection is called by a node to initiate an election
func (node *BullyNode) StartElection(args *ElectionMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received election message from Node %d\n", node.selfID, args.SenderID)
	*reply = "ack"

	// If my ID is higher, I will respond with OK and start my own election if needed
	if node.selfID > args.SenderID {
		fmt.Printf("Node %d: Sending OK to Node %d\n", node.selfID, args.SenderID)

		// Send OK message to the sender
		for _, conn := range node.otherNodes {
			if conn.serverID == args.SenderID {
				okMsg := OKMessage{SenderID: node.selfID}
				var okReply string
				go func(server ServerConnection) {
					err := server.rpcConnection.Call("BullyNode.ReceiveOK", okMsg, &okReply)
					if err != nil {
						log.Printf("Error sending OK: %v", err)
					}
				}(conn)
				break
			}
		}

		// Start my own election if I haven't already
		if !node.nominatedSelf {
			node.nominatedSelf = true
			go node.initiateElection()
		}
	}

	return nil
}

// ReceiveOK handles OK responses to election messages
func (node *BullyNode) ReceiveOK(args *OKMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received OK from Node %d\n", node.selfID, args.SenderID)
	*reply = "ack"

	// Someone with a higher ID responded, so I can't be the leader
	node.nominatedSelf = false

	return nil
}

// AnnounceCoordinator is called when a node declares itself as leader
func (node *BullyNode) AnnounceCoordinator(args *CoordinatorMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received coordinator announcement from Node %d\n", node.selfID, args.LeaderID)
	*reply = "ack"

	// Update leader information
	node.leaderID = args.LeaderID
	node.isLeader = (node.selfID == args.LeaderID)

	return nil
}

// GetLeaderID returns the current leader ID
func (node *BullyNode) GetLeaderID(args int, reply *int) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	*reply = node.leaderID
	return nil
}

// initiateElection starts an election by sending election messages to all higher-ID nodes
func (node *BullyNode) initiateElection() {
	node.mutex.Lock()
	myID := node.selfID
	node.mutex.Unlock()

	fmt.Printf("Node %d: Initiating election\n", myID)

	// Send election message to all nodes with higher IDs
	electionMsg := ElectionMessage{SenderID: myID}
	receivedResponse := false
	var wg sync.WaitGroup

	for _, conn := range node.otherNodes {
		if conn.serverID > myID {
			fmt.Printf("Node %d: Sending election message to Node %d\n", myID, conn.serverID)
			var reply string

			wg.Add(1)
			go func(server ServerConnection) {
				defer wg.Done()
				err := server.rpcConnection.Call("BullyNode.StartElection", electionMsg, &reply)
				if err == nil {
					node.mutex.Lock()
					receivedResponse = true
					node.mutex.Unlock()
				}
			}(conn)
		}
	}
	wg.Wait()

	if receivedResponse {
		fmt.Printf("Node %d: Election ongoing, waiting for coordinator message\n", myID)
	}

	// Wait for responses
	time.Sleep(2 * time.Second)

	node.mutex.Lock()
	nominatedSelf := node.nominatedSelf
	node.mutex.Unlock()

	// If no higher node responded and I still think I could be leader
	if nominatedSelf {
		// Declare self as leader
		node.mutex.Lock()
		node.leaderID = myID
		node.isLeader = true
		node.mutex.Unlock()

		fmt.Printf("Node %d: No higher nodes responded, declaring self as leader\n", myID)

		// Announce leadership to all other nodes
		coordMsg := CoordinatorMessage{LeaderID: myID}

		for _, conn := range node.otherNodes {
			fmt.Printf("Node %d: Announcing leadership to Node %d\n", myID, conn.serverID)
			var reply string

			go func(server ServerConnection) {
				err := server.rpcConnection.Call("BullyNode.AnnounceCoordinator", coordMsg, &reply)
				if err != nil {
					log.Printf("Error announcing coordinator: %v", err)
				}
			}(conn)
		}
	}
}

// StartElectionProcess periodically checks if election is needed
func (node *BullyNode) StartElectionProcess() {
	// Initial delay to let system stabilize
	time.Sleep(2 * time.Second)

	// Initial election
	go node.initiateElection()

	// Periodic leader check and election if needed
	for {
		time.Sleep(10 * time.Second)

		node.mutex.Lock()
		hasLeader := node.leaderID != -1
		isLeader := node.isLeader
		node.mutex.Unlock()

		if !hasLeader && !isLeader {
			fmt.Printf("Node %d: No leader detected, initiating new election\n", node.selfID)
			node.mutex.Lock()
			node.nominatedSelf = true
			node.mutex.Unlock()
			go node.initiateElection()
		}
	}
}

func main() {
	// Parse command line arguments
	args := os.Args
	if len(args) < 3 {
		fmt.Println("Usage: go run peer.go <node_id> <config_file>")
		return
	}

	// Get this server's ID
	myID, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatalf("Invalid node ID: %v", err)
	}

	// Read configuration file
	file, err := os.Open(args[2])
	if err != nil {
		log.Fatalf("Error opening config file: %v", err)
	}
	defer file.Close()

	// Initialize node
	node := &BullyNode{
		selfID:        myID,
		leaderID:      -1,
		otherNodes:    make([]ServerConnection, 0),
		nominatedSelf: false,
		isLeader:      false,
		mutex:         sync.Mutex{},
	}

	// Read all server addresses from config file
	scanner := bufio.NewScanner(file)
	addresses := make([]string, 0)
	index := 0

	for scanner.Scan() {
		address := scanner.Text()

		if index == myID {
			node.myPort = address
		}

		addresses = append(addresses, address)
		index++
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	// Register RPC
	err = rpc.Register(node)
	if err != nil {
		log.Fatalf("Error registering RPC: %v", err)
	}

	rpc.HandleHTTP()

	// Start HTTP server
	go func() {
		err := http.ListenAndServe(node.myPort, nil)
		if err != nil {
			log.Fatalf("Error starting HTTP server: %v", err)
		}
	}()

	log.Printf("Node %d: RPC server started on %s", node.selfID, node.myPort)

	// Wait a moment for all servers to start
	time.Sleep(3 * time.Second)

	// Connect to all other nodes
	for i, addr := range addresses {
		if i == myID {
			continue // Skip self
		}

		fmt.Printf("Node %d: Attempting to connect to Node %d at %s\n",
			myID, i, addr)

		// Try to establish connection to other node
		var client *rpc.Client
		var connErr error

		maxRetries := 5
		for retry := 0; retry < maxRetries; retry++ {
			client, connErr = rpc.DialHTTP("tcp", addr)
			if connErr == nil {
				break
			}

			log.Printf("Connection attempt %d to Node %d failed: %v. Retrying...",
				retry+1, i, connErr)
			time.Sleep(2 * time.Second)
		}

		if connErr != nil {
			log.Printf("Failed to connect to Node %d after %d attempts: %v",
				i, maxRetries, connErr)
			continue
		}

		// Save connection information
		node.otherNodes = append(node.otherNodes,
			ServerConnection{i, addr, client})
		fmt.Printf("Node %d: Connected to Node %d at %s\n", myID, i, addr)
	}

	// Create a simple HTTP endpoint to check the current leader
	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		node.mutex.Lock()
		leader := node.leaderID
		isLeader := node.isLeader
		node.mutex.Unlock()

		if isLeader {
			fmt.Fprintf(w, "Node %d: I am the leader\n", myID)
		} else if leader != -1 {
			fmt.Fprintf(w, "Node %d: Current leader is Node %d\n", myID, leader)
		} else {
			fmt.Fprintf(w, "Node %d: No leader elected yet\n", myID)
		}
	})

	// Start election process
	fmt.Printf("Node %d: Starting election process\n", myID)
	go node.StartElectionProcess()

	// Keep the process running
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
