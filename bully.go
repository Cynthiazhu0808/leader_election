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

type BullyNode struct {
	mutex         sync.Mutex
	selfID        int
	leaderID      int
	myPort        string
	otherNodes    []ServerConnection
	nominatedSelf bool
	isLeader      bool
}

type ElectionMessage struct {
	SenderID int
}

type LeaderMessage struct {
	LeaderID int
}

type OKMessage struct {
	SenderID int
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

// StartOwnElection is called by a node to initiate an election
func (node *BullyNode) StartOwnElection(ele *ElectionMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received election message from Node %d\n", node.selfID, ele.SenderID)
	*reply = "ok"

	// If self ID is higher, the node will respond with OK and start this own election if needed
	if node.selfID > ele.SenderID {
		fmt.Printf("Node %d: Sending OK to Node %d\n", node.selfID, ele.SenderID)

		// Send OK message to the sender
		for _, conn := range node.otherNodes {
			if conn.serverID == ele.SenderID {
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

		// If the node hasn't started the election yet, start the node's own election
		if !node.nominatedSelf {
			node.nominatedSelf = true
			go node.initiateElection()
		}
	}

	return nil
}

// ReceiveOK is called after receiving an ok reply from a node with a higher ID
func (node *BullyNode) ReceiveOK(msg *OKMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received OK from Node %d\n", node.selfID, msg.SenderID)
	*reply = "ok"

	// Someone with a higher ID responded, so I can't be the leader
	node.nominatedSelf = false

	return nil
}

// ConfirmLeader is called when a node wants to declare itself as leader
func (node *BullyNode) ConfirmLeader(msg *LeaderMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received leader confirmation from Node %d\n", node.selfID, msg.LeaderID)
	*reply = "ok"

	// Update leader information
	node.leaderID = msg.LeaderID
	node.isLeader = (node.selfID == msg.LeaderID)

	return nil
}

// initiateElection starts an election and send election messages to all the nodes with higher IDs
func (node *BullyNode) initiateElection() {
	node.mutex.Lock()
	myID := node.selfID
	node.mutex.Unlock()

	fmt.Printf("Node %d: Initiating election\n", myID)

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
				err := server.rpcConnection.Call("BullyNode.StartOwnElection", electionMsg, &reply)
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
		fmt.Printf("Node %d: waiting for leader confirmation message\n", myID)
	}

	// Wait for responses
	time.Sleep(2 * time.Second)

	node.mutex.Lock()
	nominatedSelf := node.nominatedSelf
	node.mutex.Unlock()

	// If no higher node responded
	if nominatedSelf {
		node.mutex.Lock()
		node.leaderID = myID
		node.isLeader = true
		node.mutex.Unlock()

		fmt.Printf("Node %d: declaring self as leader\n", myID)

		// Confirm leadership to all other nodes
		msg := LeaderMessage{LeaderID: myID}

		for _, conn := range node.otherNodes {
			fmt.Printf("Node %d: Announcing leadership to Node %d\n", myID, conn.serverID)
			var reply string

			go func(server ServerConnection) {
				err := server.rpcConnection.Call("BullyNode.ConfirmLeader", msg, &reply)
				if err != nil {
					log.Printf("An error occurred %v", err)
				}
			}(conn)
		}
	}
}

// LeaderElectio  periodically checks if election is needed
func (node *BullyNode) LeaderElection() {
	time.Sleep(2 * time.Second)

	// Initial election
	go node.initiateElection()

	// Periodic leader check if an election is required
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
	arguments := os.Args
	if len(arguments) < 3 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Get this server's ID
	myID, _ := strconv.Atoi(arguments[1])

	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
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
	lines := make([]string, 0)
	index := 0

	for scanner.Scan() {
		text := scanner.Text()

		if index == myID {
			node.myPort = text
		}

		lines = append(lines, text)
		index++
	}

	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Register RPC
	err = rpc.Register(node)
	if err != nil {
		log.Fatalf("Error registering RPCs", err)
	}

	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	log.Printf("serving rpc on port" + node.myPort)

	// Wait a moment for all servers to start
	time.Sleep(3 * time.Second)

	// Connect to all other nodes
	for i, addr := range lines {
		if i == myID {
			continue // Skip self
		}

		fmt.Printf("Node %d: Attempting to connect to Node %d at %s\n", myID, i, addr)

		// Try to establish connection to other node
		var client *rpc.Client
		var err error

		maxRetries := 5
		for retry := 0; retry < maxRetries; retry++ {
			client, err = rpc.DialHTTP("tcp", addr)
			if err == nil {
				break
			}

			log.Printf("Connection attempt to node %s failed, retrying to", addr)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			log.Printf("Failed to connect to Node")
			continue
		}

		// Save connection information
		node.otherNodes = append(node.otherNodes, ServerConnection{i, addr, client})
		fmt.Printf("Node %d: Connected to Node %d at %s\n", myID, i, addr)
	}

	// Create a simple HTTP endpoint to check the current leader
	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		node.mutex.Lock()
		leader := node.leaderID
		isLeader := node.isLeader
		node.mutex.Unlock()

		if isLeader {
			fmt.Printf("Node %s: I am the leader\n", myID)
		} else if leader != -1 {
			fmt.Printf("Current leader is Node %s\n", leader)
		} else {
			fmt.Printf("No leader elected yet\n")
		}
	})

	// Start election process
	fmt.Printf("Starting leader election\n")

	// Keep the process running
	var wg sync.WaitGroup
	wg.Add(1)
	go node.LeaderElection()
	wg.Wait()
}
