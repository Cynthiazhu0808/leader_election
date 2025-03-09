package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	receiveChan   chan Message // difference between the channels
	electionChan  chan Message
	isLeader      bool
}

type Message interface{}

// ?? maybe we can specify the type inside the struct instead of having three different similar functioning struct?
type ElectionMessage struct {
	//messageType MessageType
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

func (node *BullyNode) StartElection(args *ElectionMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received election message from Node %d\n", node.selfID, args.SenderID)
	*reply = "ack"

	// If the selfID is greater than the receivedID, respond OK  ??and start my own election if needed
	// ?? why are we starting a new election if needed? Oh, do you mean the nominating self?
	if node.selfID > args.SenderID {
		fmt.Printf("Node %d: Sending OK to Node %d\n", node.selfID, args.SenderID)

		// Send OK message to the sender
		for _, conn := range node.otherNodes { // is there a faster way rather than iterating through all to find this one node
			if conn.serverID == args.SenderID {
				okMsg := OKMessage{SenderID: node.selfID}
				var okReply string
				go func(server ServerConnection) {
					err := server.rpcConnection.Call("BullyNode.ReceiveOK", okMsg, &okReply)
					if err != nil {
						log.Printf("Error sending OK: %v", err)
					}
				}(conn) // ?? What does this do?
				break
			}
		}

		// Start Self Election
		if !node.nominatedSelf {
			node.nominatedSelf = true
			go node.initiateElection() //?? Doesn't we only need to check if higher functions are alive or not? The naming is a little confusing with start and initiate election
		}
	}
	return nil
}

func (node *BullyNode) ReceiveOK(args *OKMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received OK from Node %d\n", node.selfID, args.SenderID)
	*reply = "ack"

	// Someone with a higher ID responded, so I can't be the leader
	node.nominatedSelf = false

	return nil
}

func (node *BullyNode) AnnounceLeader(args *LeaderMessage, reply *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Printf("Node %d: Received Leader announcement from Node %d\n", node.selfID, args.LeaderID)
	*reply = "ack"

	// Update leader information
	node.leaderID = args.LeaderID
	node.isLeader = (node.selfID == args.LeaderID)

	return nil
}

func (node *BullyNode) GetLeaderID(args int, reply *int) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	*reply = node.leaderID
	return nil
}

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
		fmt.Printf("Node %d: Election ongoing, waiting for leader message\n", myID)
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
		leadMsg := LeaderMessage{LeaderID: myID}

		for _, conn := range node.otherNodes {
			fmt.Printf("Node %d: Announcing leadership to Node %d\n", myID, conn.serverID)
			var reply string

			go func(server ServerConnection) {
				err := server.rpcConnection.Call("BullyNode.AnnounceLeader", leadMsg, &reply)
				if err != nil {
					log.Printf("Error announcing leader: %v", err)
				}
			}(conn)
		}
	}
}

// StartElectionProcess periodically checks if election is needed
func (node *BullyNode) StartElectionProcess() {
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
	args := os.Args
	// The assumption here is that the command line arguemnts will contain
	// This server's ID (zero-based), location and name of the cluster configuration file
	if len(args) < 3 {
		fmt.Println("Please input go run peer.go <node_id> <config_file>")
		return
	}

	// Read the values sent in the command line
	// Get this server's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatalf("Invalid node ID: %v, please try again", err)
	}

	// Get the information of the cluster configuration file containing information on other servers
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

	// Static port creation start
	// Read the IP:port info from the cluster configuration file
	// scanner := bufio.NewScanner(file)
	// addresses := make([]string, 0)
	// index := 0

	// for scanner.Scan() {
	// 	// Get server IP:port
	// 	address := scanner.Text()
	// 	// Prints date, time, and index of the node being processed
	// 	log.Printf(address, index)

	// 	if index == myID {
	// 		node.myPort = address
	// 	}

	// 	addresses = append(addresses, address)
	// 	index++
	// }

	// if err := scanner.Err(); err != nil {
	// 	log.Fatalf("Error reading config file: %v", err)
	// }
	// Static port creation end

	// Dynamic port creation start
	basePort := 5000       //starting port number
	baseIP := "localhost:" // Base IP address
	addresses := make([]string, 0)
	index := 0

	fmt.Println("Please enter node info and type 'done' when finished:")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("Please input node %d (or 'done' to finish): ", index)
		scanner.Scan()
		input := scanner.Text()

		if strings.ToLower(input) == "done" {
			break
		}

		// generate new adderesses with incremental port number
		address := fmt.Sprintf("%s:%d", baseIP, basePort+index)
		log.Printf("Node %d address: %s", index, address)

		if index == myID {
			node.myPort = address
		}

		addresses = append(addresses, address)
		index++
	}

	// check if we have at least three node
	if len(addresses) < 3 {
		log.Fatal("Not Enough nodes were specified!")
	}

	// Dynamic port creation ends

	err = rpc.Register(node)
	if err != nil {
		log.Fatalf("Error registering RPC: %v", err)
	}

	rpc.HandleHTTP()
	// multi thread this process
	go func() {
		err := http.ListenAndServe(node.myPort, nil)
		log.Printf("serving rpc on port" + node.myPort)
		if err != nil {
			log.Fatalf("Error starting HTTP server: %v", err)
		}
	}()

	// wait for all servers to start, debugging use
	// time.Sleep(3 * time.Second)

	// Connect to all other nodes start
	for i, address := range addresses {
		if i == myID {
			continue // skip self
		}

		fmt.Printf("Bully #%d: Tyring to connect to Bully #%d at %s\n",
			myID, i, address)

		// establish connection to other bullyNodes
		var client *rpc.Client
		var err error

		// error handeling for connection to other node
		maxRetries := 5
		for retry := 0; retry < maxRetries; retry++ {
			client, err = rpc.DialHTTP("tcp", address)
			if err == nil {
				break
			}
			log.Printf("New connection attempt %d to connect to bully %d: %v. Retrying...",
				retry+1, i, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			// Record it in log
			log.Printf("Failed to connect to Node %d after %d attempts: %v",
				i, maxRetries, err)
			continue
		}

		// Once connection is established, save connection information in otherNodes
		node.otherNodes = append(node.otherNodes,
			ServerConnection{i, address, client})
		// Record that in log
		fmt.Printf("Node %d: Connected to Node %d at %s\n", myID, i, address)
	}
	// Connect to all other nodes end

	// Leader Checking starts: Create HTTP endpoint to check the current leader
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
	// Leader checking ends

	// Start election process
	// wait why is there no timer? who is the one starting the process?
	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Printf("Node %d: Started the election process\n", myID)
	go node.StartElectionProcess()
	wg.Wait()
}
