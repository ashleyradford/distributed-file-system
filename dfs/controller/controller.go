package main

import (
	"dfs/messages"
	"dfs/util"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

type controllerData struct {
	totalDiskSpace uint64
	nodeMap        map[string]*nodeData
	fileMap        map[string]map[string]*chunkData // { filename : { chunk : data } }
	idCount        int64
}

type nodeData struct {
	hb        time.Time
	addr      string
	diskSpace uint64
	requests  int64
}

type chunkData struct {
	size   int64
	time   time.Time       // last time since creation or replica received
	status string          // pending -> complete once all 3 nodes come in
	nodes  map[string]bool // node ids
}

var m = sync.RWMutex{}

func removeNodeMappings(nodeId string, controller *controllerData) {
	// { filename : { chunk : data } }
	for _, chunkMap := range controller.fileMap {
		for _, data := range chunkMap {
			delete(data.nodes, nodeId)
		}
	}
}

/* ------- Receive Messages ------- */
func receiveJoinRequest(msgHandler *messages.MessageHandler, joinReq *messages.JoinReq,
	controller *controllerData) string {

	controller.idCount += 1 // node01, node02, node03, ...
	nodeId := fmt.Sprintf("node%02d", controller.idCount)

	// add to map
	m.Lock()
	controller.totalDiskSpace += joinReq.DiskSpace
	controller.nodeMap[nodeId] = &nodeData{time.Now(), joinReq.NodeAddr, joinReq.DiskSpace, 0}
	m.Unlock()
	log.Println(nodeId + " has joined")

	msgHandler.SendJoinResponse(true, nodeId)

	return nodeId
}

func receiveHb(heartbeat *messages.Heartbeat, controller *controllerData) {
	log.Printf("Received <3 from %s", heartbeat.NodeId)

	m.RLock()
	node, alive := controller.nodeMap[heartbeat.NodeId]
	m.RUnlock()

	if alive {
		m.Lock()
		node.hb = time.Now() // update hb time

		// check if any new chunks were added to node
		if len(heartbeat.NewChunks) > 0 {
			temp := controller.nodeMap[heartbeat.NodeId].diskSpace

			// update node: diskSpace, requests
			controller.nodeMap[heartbeat.NodeId].diskSpace = heartbeat.DiskSpace
			controller.nodeMap[heartbeat.NodeId].requests = heartbeat.Requests

			// update controller: available space and fileMap
			delta := temp - heartbeat.DiskSpace
			controller.totalDiskSpace = controller.totalDiskSpace - delta

			// filemap looks like { filename : { chunk : data } }
			for filename := range heartbeat.NewChunks {
				if controller.fileMap[filename] != nil { // file may have been deleted in between heartbeats
					for _, chunk := range heartbeat.NewChunks[filename].Chunks {
						// fill in chunk data
						controller.fileMap[filename][chunk].time = time.Now()
						controller.fileMap[filename][chunk].nodes[heartbeat.NodeId] = true
						if len(controller.fileMap[filename][chunk].nodes) >= util.REPLICAS {
							// received the last replica
							controller.fileMap[filename][chunk].status = "complete"
						}
					}
				}
			}
		}
		m.Unlock()
	} else {
		// node needs to rejoin with new id
		killNode(controller.nodeMap[heartbeat.NodeId].addr)
	}
}

func receiveNodeRequest(msgHandler *messages.MessageHandler, controller *controllerData) {
	log.Println("Received nodes request from client")

	nodeReqMap := make(map[string]int64)

	m.RLock()
	// loop through controller node map and retreive node id and num of requests completed
	for nodeId, nodeData := range controller.nodeMap {
		nodeReqMap[nodeId] = nodeData.requests
	}
	msgHandler.SendNodesResponse(controller.totalDiskSpace, nodeReqMap)
	m.RUnlock()
}

func receiveListRequest(msgHandler *messages.MessageHandler, controller *controllerData) {
	log.Println("Received ls request from client")

	fileList := make([]string, 0)

	m.RLock()
	// loop through filenames in filemap
	for filename := range controller.fileMap {
		fileList = append(fileList, filename)
	}
	msgHandler.SendListResponse(fileList)
	m.RUnlock()
}

func receiveStorageRequest(msgHandler *messages.MessageHandler, storeReq *messages.CStorageReq,
	controller *controllerData) {

	log.Printf("Received storage request from client with %d chunks\n", len(storeReq.ChunkSizes))

	// first check if file already exists
	_, ok := controller.fileMap[storeReq.Filename]
	if ok {
		msgHandler.SendStorageResC(false, "Error: file already exists", nil, nil, nil)
		return
	}

	// then check if there are enough nodes for the replication level
	if len(controller.nodeMap) < util.REPLICAS {
		msgHandler.SendStorageResC(false, "Error: not enough nodes for replication level", nil, nil, nil)
		return
	}

	// then check if there is enough space in disk (doesn't check chunk spaces tho)
	if uint64(storeReq.Filesize) > controller.totalDiskSpace {
		msgHandler.SendStorageResC(false, "Error: not enough space in cluster", nil, nil, nil)
		return
	}

	// add filename to map ~ wont be filled out until storage node alerts us
	controller.fileMap[storeReq.Filename] = make(map[string]*chunkData)

	// create maps for response
	chunkMap := make(map[string]string)
	replicaNodes := make(map[string][]string)

	// create node set for response
	nodeConns := make(map[string]bool)

	// want to randomly select chunks
	rand.Seed(time.Now().UnixNano())
	nodes := make([]string, 0, len(controller.nodeMap))
	for node := range controller.nodeMap {
		nodes = append(nodes, node)
	}

	// now check where we can store the chunks
	var randomNode string
	for chunkname := range storeReq.ChunkSizes {
		destNodes := make(map[string]bool)
		destCount := 0

		// keep checking until we find a node big enough (ASSUMPTION)
		for destCount < util.REPLICAS { // how many total copies
			randomNode = nodes[rand.Intn(len(nodes))]
			_, taken := destNodes[randomNode]
			if controller.nodeMap[randomNode].diskSpace >= uint64(storeReq.ChunkSizes[chunkname]) && !taken {
				// add node to file and chunk map
				controller.fileMap[storeReq.Filename][chunkname] = new(chunkData)
				controller.fileMap[storeReq.Filename][chunkname].size = storeReq.ChunkSizes[chunkname]
				controller.fileMap[storeReq.Filename][chunkname].time = time.Now()
				controller.fileMap[storeReq.Filename][chunkname].status = "pending"
				controller.fileMap[storeReq.Filename][chunkname].nodes = make(map[string]bool)

				// update count
				destNodes[randomNode] = true
				destCount += 1
			}
		}

		// now add dest nodes (main node + replica nodes)
		var mainFlag bool = false
		for node := range destNodes {
			if !mainFlag {
				chunkMap[chunkname] = controller.nodeMap[node].addr // main nodes
				mainFlag = true
				nodeConns[controller.nodeMap[node].addr] = true // only add main node connections
			} else {
				replicaNodes[chunkname] = append(replicaNodes[chunkname], controller.nodeMap[node].addr)
			}
		}
	}

	// get list of node connections that need to be opened
	nodeList := []string{}
	for nodeAddr := range nodeConns {
		nodeList = append(nodeList, nodeAddr)
	}

	// send { chunk : node } and { chunk : replicas } mappings back to client
	msgHandler.SendStorageResC(true, "", nodeList, chunkMap, replicaNodes)
}

func receiveRetrievalRequest(msgHandler *messages.MessageHandler, retreiveReq *messages.CRetrievalReq,
	controller *controllerData) {

	log.Println("Received retrieval request from client")

	// first check if file is store in directory { filename : { chunk : data } }
	chunkMap, ok := controller.fileMap[retreiveReq.Filename]
	if !ok {
		msgHandler.SendRetrievalResC(false, "File is not stored.", 0, nil)
	}

	// create chunk map to return to client and find mappings
	numChunks := int64(0)
	nodeChunks := make(map[string][]string)
	for chunk, data := range chunkMap {
		// grab any one node from the { chunk : data.nodes } map
		for node, ok := range data.nodes {
			if !ok {
				nodeChunks[controller.nodeMap[node].addr] = make([]string, 0)
			}
			nodeChunks[controller.nodeMap[node].addr] = append(nodeChunks[controller.nodeMap[node].addr], chunk)
			numChunks++
			break
		}
	}

	msgHandler.SendRetrievalResC(true, "", numChunks, nodeChunks)
}

func receiveDeleteRequest(msgHandler *messages.MessageHandler, deleteReq *messages.DeleteReq,
	controller *controllerData) {

	log.Println("Received delete request from client")

	// first check if file is store in directory
	_, ok := controller.fileMap[deleteReq.Filename]
	if !ok {
		msgHandler.SendDeleteRes(false, "File does not exist.")
	}

	// delete file mapping
	m.Lock()
	delete(controller.fileMap, deleteReq.Filename)
	m.Unlock()

	msgHandler.SendDeleteRes(true, "File successfully deleted.")
}

func receiveReplicaRequest(msgHandler *messages.MessageHandler, replicaReq *messages.ReplicaReq,
	controller *controllerData) {

	var replicaNode string = ""
	// find a copy in map { filename : { chunk : data } }
	for node := range controller.fileMap[replicaReq.Filename][replicaReq.Chunkname].nodes {
		// grab first node that isn't node with corrupted file in it
		if node != replicaReq.NodeId {
			replicaNode = node
			break
		}
	}

	// send node back the address of node that has copy for it
	if replicaNode != "" {
		msgHandler.SendReplicaRes(true, replicaNode, controller.nodeMap[replicaNode].addr)
	} else {
		msgHandler.SendReplicaRes(false, "Cannot find node with a clean copy.", "")
	}
}

/* --------- Send Messages --------- */
func killNode(nodeAddr string) {
	// try to connect to storage node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Fatalln(err)
		return
	}
	msgHandler := messages.NewMessageHandler(conn)

	// let node know that it timed out
	msgHandler.SendKillNode(false)

	// close message handler and connection
	msgHandler.Close()
	conn.Close()
}

func prepareReplicaOrder(filename string, chunkname string, chunksize int64, nodeAddr string,
	nodes map[string]bool, replicaCount int, controller *controllerData) {

	log.Printf("Requesting replica(s) for %s\n", chunkname)

	// first find chunk(s) in cluster where a replica isn't already stored
	// want to randomly select chunks
	rand.Seed(time.Now().UnixNano())
	nodeList := make([]string, 0, len(controller.nodeMap))
	for node := range controller.nodeMap {
		nodeList = append(nodeList, node)
	}

	// now check where we can store the chunks
	var randomNode string
	destNodes := make(map[string]bool)
	destCount := 0

	// keep checking until we find a node big enough (ASSUMPTION)
	for destCount < util.REPLICAS-replicaCount { // how many total copies we need to reach rep count
		randomNode = nodeList[rand.Intn(len(nodeList))]
		_, taken := destNodes[randomNode]
		if controller.nodeMap[randomNode].diskSpace >= uint64(chunksize) && !taken {
			// now check that replica is not already at this random node
			if _, ok := nodes[randomNode]; !ok {
				destNodes[randomNode] = true
				destCount += 1
			}
		}
	}

	// then send replica request to node that already has file with a list of nodes to pass it on to pipeline style
	var replicaAddrs []string
	for node := range destNodes {
		replicaAddrs = append(replicaAddrs, controller.nodeMap[node].addr)
	}
	orderReplica(nodeAddr, filename, chunkname, chunksize, replicaAddrs)
}

func orderReplica(nodeAddr string, filename string, chunkname string, chunksize int64, replicaAddrs []string) {
	// open up a connection with first addr in replicaAddrs
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("Successfully connected to %s for replica req for %s\n", nodeAddr, chunkname)
	msgHandler := messages.NewMessageHandler(conn)

	// send storage req to specified node
	msgHandler.SendReplicaOrder(filename, chunkname, chunksize, replicaAddrs)

	// wait for storage node response letting controller know if replica has been passed on
	// don't care if it succeeded bc if it didn't it will be caught again anyways
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetOrderRes()
	log.Println(response.Message)

	// close message handler and connection
	msgHandler.Close()
	conn.Close()
}

/* ------ Controller Threads ------ */
func checkHeartBeat(controller *controllerData, threshold float64) {
	for {
		time.Sleep(util.HB_CHECK * time.Second)
		log.Println("Checking on nodes")
		for id, node := range controller.nodeMap {
			m.RLock()
			lastHb := time.Since(node.hb).Seconds()
			m.RUnlock()
			if lastHb > threshold {
				log.Println(id + " has died ):")

				m.Lock()
				// update total cluster disk space
				controller.totalDiskSpace -= controller.nodeMap[id].diskSpace
				// need to remove mappings of { chunk : data.nodes }
				removeNodeMappings(id, controller)
				// delete from node map
				delete(controller.nodeMap, id)
				m.Unlock()

				// check if number of nodes is in danger zone
				if len(controller.nodeMap) < util.REPLICAS {
					log.Println("ALERT: number of nodes less than replication level.")
				}
			}
		}
	}
}

func checkReplicationLevel(controller *controllerData) {
	for {
		time.Sleep(util.REP_CHECK * time.Second)
		log.Println("Checking replication level")
		// { filename : { chunk : data } }
		for filename, chunkMap := range controller.fileMap {
			for chunkname, data := range chunkMap {
				m.RLock()
				if len(data.nodes) < util.REPLICAS {
					if (data.status == "complete") ||
						(data.status == "pending" && time.Since(data.time).Seconds() > util.TIMEOUT) {

						// fix chunk that does not reach replication requirement
						log.Printf("ALERT: %s is below replication level\n", chunkname)

						// pick a node that has copy
						var reqNode string
						for node := range data.nodes {
							reqNode = controller.nodeMap[node].addr
						}

						// prepare and send replica order to reqNode
						go prepareReplicaOrder(filename, chunkname, data.size, reqNode, data.nodes, len(data.nodes), controller)
					}
				}
				m.RUnlock()
			}
		}
	}
}

func handleReq(conn net.Conn, controller *controllerData, msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	defer conn.Close()

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_JoinReq:
			receiveJoinRequest(msgHandler, msg.JoinReq, controller)
		case *messages.Wrapper_Heartbeat:
			receiveHb(msg.Heartbeat, controller)
		case *messages.Wrapper_NodesReq:
			receiveNodeRequest(msgHandler, controller)
		case *messages.Wrapper_CStorageReq:
			receiveStorageRequest(msgHandler, msg.CStorageReq, controller)
		case *messages.Wrapper_ListReq:
			receiveListRequest(msgHandler, controller)
		case *messages.Wrapper_CRetrievalReq:
			receiveRetrievalRequest(msgHandler, msg.CRetrievalReq, controller)
		case *messages.Wrapper_DeleteReq:
			receiveDeleteRequest(msgHandler, msg.DeleteReq, controller)
		case *messages.Wrapper_ReplicaReq: // when a file has been corrupted
			receiveReplicaRequest(msgHandler, msg.ReplicaReq, controller)
		case nil:
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
			return
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s listen-port\n", os.Args[0])
		return
	}

	threshold := util.RATE * util.MISSED

	// try to listen on a given port
	listener, err := net.Listen("tcp", ":"+os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Listening for node connections...")

	// initialize controller data struct
	var controller *controllerData = new(controllerData)
	controller.totalDiskSpace = 0
	controller.nodeMap = make(map[string]*nodeData)
	controller.fileMap = make(map[string]map[string]*chunkData)
	controller.idCount = 0

	// check heartbeats every so often
	go checkHeartBeat(controller, float64(threshold))

	// check replication level
	go checkReplicationLevel(controller)

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleReq(conn, controller, msgHandler)
			// defer conn.Close()
		}
	}
}
