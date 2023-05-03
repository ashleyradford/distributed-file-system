package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"dfs/util"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"plugin"
	"sync"
	"time"
)

var m = sync.RWMutex{}

const DEST = "/bigdata/students/aeradford/mydfs"

type MapReduce interface {
	Map(line_number int, line_text string) map[string]string
	Reduce()
}

type storageNode struct {
	nodeId      string
	nodeAddr    string
	ctrlrAddr   string
	diskSpace   uint64
	requests    int64
	newChunks   map[string][]string
	pluginCache map[string]*plugin.Plugin
}

func updateNodeInfo(node *storageNode, success bool, filename string, chunkname string) {
	// update successful requests and add new chunks to map
	log.Println("Updating requests, space, and chunks list.")
	if success {
		m.Lock()
		node.requests += 1
		node.newChunks[filename] = append(node.newChunks[filename], chunkname)
		m.Unlock()
	}

	// still update avialable space either way (bc bad data might be here)
	freeSpace, err := util.GetDiskSpace()
	if err != nil {
		log.Println("Error when getting node disk space:", err)
	}

	m.Lock()
	node.diskSpace = freeSpace
	m.Unlock()
}

/* ------ Receive Messages ------ */
func receiveStorageRequest(msgHandler *messages.MessageHandler, storeReq *messages.NStorageReq,
	node *storageNode, dest string) {

	log.Printf("Received storage request for %s", storeReq.Chunkname)

	// check if enough space on disk
	ok, err := util.CheckSpace(storeReq.Chunksize)
	if err != nil {
		msgHandler.SendStorageResN(false, err.Error())
		return
	} else if !ok {
		msgHandler.SendStorageResN(false, "Not enough space on node.")
		return
	}

	// open files to write data and checksum to
	chunkFile, err := os.OpenFile(dest+"/"+storeReq.Chunkname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		msgHandler.SendStorageResN(false, err.Error())
		return
	}
	defer chunkFile.Close()

	checksumFilename := fmt.Sprintf("%s.checksum", storeReq.Chunkname)
	checksumFile, err := os.OpenFile(dest+"/"+checksumFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		msgHandler.SendStorageResN(false, err.Error())
		return
	}
	defer checksumFile.Close()

	// byte buffer
	var buf bytes.Buffer

	// okay to store file, write and checksum as we go (also write to a buffer in memory to send replicas)
	msgHandler.SendStorageResN(ok, "Ok to send data.")
	md5Hash := md5.New()
	w := io.MultiWriter(chunkFile, md5Hash, &buf)
	io.CopyN(w, msgHandler, int64(storeReq.Chunksize))

	// store checksum contents to file
	nodeChecksum := md5Hash.Sum(nil)
	checksumFile.Write(nodeChecksum)

	// get checksum from client
	wrapper, _ := msgHandler.Receive()
	clientChecksum := wrapper.GetMd5().Md5

	// verify checksums match and update node info (in both cases)
	if util.VerifyChecksum(clientChecksum, nodeChecksum) {
		msgHandler.SendChecksumResponse(true, "Chunk successfully stored.")
		updateNodeInfo(node, true, storeReq.Filename, storeReq.Chunkname)
	} else {
		msgHandler.SendChecksumResponse(false, "Failed to store chunk, invalid checksum.")
		updateNodeInfo(node, false, "", "")
	}

	// TODO: send failed message to controller to let it know to delete, maybe don't send replicas too

	// now to pass on the replicas to other nodes
	if len(storeReq.ReplicaNodes) > 0 {
		destination := storeReq.ReplicaNodes[0]           // get the dest of first replica
		storeReq.ReplicaNodes = storeReq.ReplicaNodes[1:] // take it out of the replica slice
		sendReplicas(destination, storeReq, buf.Bytes(), nodeChecksum)
	}
}

func receiveReplicaOrder(msgHandler *messages.MessageHandler, replicaReq *messages.ReplicaOrder,
	node *storageNode, dest string) {

	// first open chunk that needs to be replicated and read into buffer
	// open chunk file and grab checksum
	chunkFile, err := os.OpenFile(dest+"/"+replicaReq.Chunkname, os.O_RDWR, 0644)
	if err != nil {
		msgHandler.SendOrderRes(false, err.Error())
		return
	}
	defer chunkFile.Close()

	// byte buffer
	var buf bytes.Buffer

	md5Hash := md5.New()
	w := io.MultiWriter(md5Hash, &buf)
	io.Copy(w, chunkFile)
	fileChecksum := md5Hash.Sum(nil)

	// open checksum file and grab contents (ie the checksum)
	checksumFile, err := os.Open(dest + "/" + replicaReq.Chunkname + ".checksum")
	if err != nil {
		msgHandler.SendOrderRes(false, err.Error())
		return
	}
	defer checksumFile.Close()

	storedChecksum := make([]byte, md5.Size)
	_, err = checksumFile.Read(storedChecksum)
	if err != nil && err != io.EOF {
		msgHandler.SendOrderRes(false, err.Error())
		return
	}

	// reset file pointer to beg of chunk file
	_, err = chunkFile.Seek(0, 0)
	if err != nil {
		msgHandler.SendOrderRes(false, err.Error())
		return
	}

	// compare the file checksum and the stored checksum
	if util.VerifyChecksum(fileChecksum, storedChecksum) {
		// let client know we will begin sending file
		msgHandler.SendOrderRes(true, "Chunk is valid, will pass on")
	} else {
		// ask controller for another copy
		log.Printf("ALERT: %s is corrupted\n", replicaReq.Chunkname)

		// ask for a clean copy
		bytes := sendReplicaRequest(node.ctrlrAddr, node.nodeId, replicaReq.Filename,
			replicaReq.Chunkname, chunkFile, storedChecksum, dest)

		// reset file pointer to beg of chunk file (since we overwrote it)
		_, err = chunkFile.Seek(0, 0)
		if err != nil {
			msgHandler.SendRetrievalResN(false, err.Error(), 0)
			return
		}

		// write the correct file into buffer
		io.Copy(&buf, chunkFile)
		buf.Truncate(bytes)

		msgHandler.SendRetrievalResN(true, "Chunk is valid, will pass on", int64(bytes))
	}

	// send out replicas to other node(s)
	if len(replicaReq.ReplicaNodes) > 0 {
		log.Printf("Replica being passed on to %s", replicaReq.ReplicaNodes[0])
		destination := replicaReq.ReplicaNodes[0]             // get the dest of first replica
		replicaReq.ReplicaNodes = replicaReq.ReplicaNodes[1:] // take it out of the replica slice
		sendReplicas(destination, (*messages.NStorageReq)(replicaReq), buf.Bytes(), storedChecksum)
	}
}

func receiveRetrievalRequest(msgHandler *messages.MessageHandler, retrieveReq *messages.NRetrievalReq,
	node *storageNode, dest string) {

	log.Printf("Received retrieval request for %s", retrieveReq.Chunkname)

	// get file size and check if it exists
	info, err := os.Stat(dest + "/" + retrieveReq.Chunkname)
	if err != nil {
		msgHandler.SendRetrievalResN(false, err.Error(), 0)
		return
	}

	// open chunk file and grab checksum
	chunkFile, err := os.OpenFile(dest+"/"+retrieveReq.Chunkname, os.O_RDWR, 0644)
	if err != nil {
		msgHandler.SendRetrievalResN(false, err.Error(), 0)
		return
	}
	defer chunkFile.Close()

	md5Hash := md5.New()
	io.Copy(md5Hash, chunkFile)
	fileChecksum := md5Hash.Sum(nil)

	// open checksum file and grab contents (ie the checksum)
	checksumFile, err := os.Open(dest + "/" + retrieveReq.Chunkname + ".checksum")
	if err != nil {
		msgHandler.SendRetrievalResN(false, err.Error(), 0)
		return
	}
	defer checksumFile.Close()

	storedChecksum := make([]byte, md5.Size)
	_, err = checksumFile.Read(storedChecksum)
	if err != nil && err != io.EOF {
		msgHandler.SendRetrievalResN(false, err.Error(), 0)
		return
	}

	// reset file pointer to beg of chunk file
	_, err = chunkFile.Seek(0, 0)
	if err != nil {
		msgHandler.SendRetrievalResN(false, err.Error(), 0)
		return
	}

	// compare the file checksum and the stored checksum
	if util.VerifyChecksum(fileChecksum, storedChecksum) {
		// let client know we will begin sending file
		msgHandler.SendRetrievalResN(true, "Chunk is valid, will begin transfer", info.Size())
	} else {
		// ask controller for another copy
		log.Printf("ALERT: %s is corrupted\n", retrieveReq.Chunkname)

		// ask for a clean copy
		bytes := sendReplicaRequest(node.ctrlrAddr, node.nodeId, retrieveReq.Filename,
			retrieveReq.Chunkname, chunkFile, storedChecksum, dest)

		// reset file pointer to beg of chunk file (since we overwrote it)
		_, err = chunkFile.Seek(0, 0)
		if err != nil {
			msgHandler.SendRetrievalResN(false, err.Error(), 0)
			return
		}

		msgHandler.SendRetrievalResN(true, "Chunk is valid, will begin transfer", int64(bytes))
	}

	// send chunk contents
	io.CopyN(msgHandler, chunkFile, info.Size())

	// send storage node checksum
	if retrieveReq.Checksum {
		msgHandler.SendChecksum(storedChecksum)
	}
}

func getPlugin(mapOrder *messages.MapOrder, dest string, node *storageNode) (MapReduce, error) {

	// check if so file has been hashed
	var jobPlugin *plugin.Plugin
	if _, ok := node.pluginCache[mapOrder.JobHash]; ok {
		jobPlugin = node.pluginCache[mapOrder.JobHash]
	} else {
		// create temp file for so bytes
		tmpfile, err := ioutil.TempFile(dest, "job_*.so")
		if err != nil {
			return nil, err
		}
		defer os.Remove(tmpfile.Name())

		// write bytes to tempfile
		_, err = tmpfile.Write(mapOrder.Job)
		if err != nil {
			return nil, err
		}
		tmpfile.Close()

		// load the plugin from the temporary file
		jobPlugin, err = plugin.Open(tmpfile.Name())
		if err != nil {
			return nil, err
		}

		// add plugin to cache
		node.pluginCache[mapOrder.JobHash] = jobPlugin
	}

	// look up MapReduce symbol (exported function or variable)
	mapReduce, err := jobPlugin.Lookup("MapReduce")
	if err != nil {
		return nil, err
	}

	// assert that loaded symbol is of MapReduce type
	var job MapReduce
	job, ok := mapReduce.(MapReduce)
	if !ok {
		return nil, errors.New("unexpected type from module symbol")
	}

	return job, nil
}

func mapFile(filepath string, job MapReduce, tmpfile *os.File) (linesParsed int, err error) {
	// open chunk file
	chunkFile, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer chunkFile.Close()

	// perform map job for each line, max length of buf is 64 * 1024 bytes
	buf := 512 * 1024
	scanner := bufio.NewScanner(chunkFile)
	scanner.Buffer(make([]byte, buf), buf)
	lineNum := 0

	var kvPairs map[string]string
	for scanner.Scan() {
		lineText := scanner.Text()
		kvPairs = job.Map(lineNum, lineText)
		if len(kvPairs) > 0 {
			// write key value pairs to temp file
			for key, value := range kvPairs {
				if _, err = tmpfile.Write(append([]byte(key), []byte("\n")...)); err != nil {
					return 0, err
				}
				if _, err = tmpfile.Write(append([]byte(value), []byte("\n")...)); err != nil {
					return 0, err
				}
			}
			lineNum++
		}
	}

	// throw error if line is too big
	if scanner.Err() != nil {
		log.Println(scanner.Err())
	}

	return lineNum, nil
}

func receiveMapOrder(msgHandler *messages.MessageHandler, mapOrder *messages.MapOrder,
	node *storageNode, dest string) {

	log.Println("Received map reduce job")

	job, err := getPlugin(mapOrder, dest, node)
	if job == nil {
		// send failed message back
		msgHandler.SendMapStatus(false, err.Error())
		return
	}

	// create temp file for emitted key value pairs
	tmpfile, err := ioutil.TempFile(dest, "kv_pairs_*.txt")
	if err != nil {
		// send failed message back
		msgHandler.SendMapStatus(false, err.Error())
		return
	}

	// perform map task on each chunk
	var linesParsed int
	for _, chunkFile := range mapOrder.Chunks {
		filepath := dest + "/" + chunkFile
		linesParsed, err = mapFile(filepath, job, tmpfile)
		if err != nil {
			// send failed message back
			msgHandler.SendMapStatus(false, err.Error())
			return
		}
	}

	tmpfile.Close()
	// os.Remove(tmpfile.Name())

	// send map status back to resource manager
	msgHandler.SendMapStatus(true, fmt.Sprintf("Successfully completed map task, %d lines parsed", linesParsed))
}

/* ------ Send Messages ------ */
func sendHeartbeat(node *storageNode) error {
	// try to connect to controller
	conn, err := net.Dial("tcp", node.ctrlrAddr)
	if err != nil {
		log.Printf("Could not connect to controller address: %s", node.ctrlrAddr)
		return err
	}
	msgHandler := messages.NewMessageHandler(conn)

	// create and send heartbeat
	msgHandler.SendHeartbeat(node.nodeId, node.diskSpace, node.requests, node.newChunks)

	// reset new chunks map if not empty
	if len(node.newChunks) != 0 {
		m.Lock()
		log.Printf("Chunk map sent to controller: %v\n", node.newChunks)
		for key := range node.newChunks {
			delete(node.newChunks, key)
		}
		m.Unlock()
	}

	// close message handler and connection
	msgHandler.Close()
	conn.Close()

	return nil
}

func sendJoin(ctrlrAddr string, node *storageNode) error {
	// try to connect to controller
	conn, err := net.Dial("tcp", ctrlrAddr)
	if err != nil {
		log.Printf("Could not connect to controller address: %s", node.ctrlrAddr)
		return err
	}
	msgHandler := messages.NewMessageHandler(conn)

	// send join request
	log.Println("Sending join request")
	msgHandler.SendJoinRequest(node.nodeAddr, node.diskSpace)

	// wait for join response
	wrapper, _ := msgHandler.Receive()
	msg, _ := wrapper.Msg.(*messages.Wrapper_JoinRes)
	if msg.JoinRes.Accept {
		node.nodeId = msg.JoinRes.NodeId
		log.Println("Successfully joined controller")
	} else {
		log.Println("Could not join controller.")
	}

	// close connection and return nodeId
	msgHandler.Close()
	conn.Close()

	return nil
}

// create a new storeReq for the node that we are sending to
func sendReplicas(nodeAddr string, storeReq *messages.NStorageReq, contents []byte, checksum []byte) {
	// open up a tcp connection with specified node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("Sending %s replica to %s\n", storeReq.Chunkname, nodeAddr)
	msgHandler := messages.NewMessageHandler(conn)

	msgHandler.SendStorageReqN(storeReq.Filename, storeReq.Chunkname, storeReq.Chunksize, storeReq.ReplicaNodes)

	// wait for storage node response
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetNStorageRes()
	if !response.Ok {
		log.Println(response.Message)
		return
	}

	// okay to send file contents
	io.CopyN(msgHandler, bytes.NewReader(contents), storeReq.Chunksize)

	// send storage node checksum
	msgHandler.SendChecksum(checksum)

	// receive response
	wrapper, _ = msgHandler.Receive()
	checksumResponse := wrapper.GetChecksumRes()
	if !checksumResponse.Ok {
		log.Println(checksumResponse.Message)
		return
	}

	// close connection and message handler
	conn.Close()
	msgHandler.Close()
}

func sendReplicaRequest(ctrlrAddr string, nodeId string, filename string,
	chunkname string, chunkFile *os.File, storedChecksum []byte, dest string) int {

	log.Println("Asking controller for help")

	// try to open a connection to controller
	conn, err := net.Dial("tcp", ctrlrAddr)
	if err != nil {
		log.Printf("Could not connect to controller address: %s", ctrlrAddr)
		return 0
	}
	ctrlrMsgHandler := messages.NewMessageHandler(conn)

	// ask controller for new copy
	ctrlrMsgHandler.SendReplicaReq(nodeId, filename, chunkname)

	// wait for controller response
	wrapper, _ := ctrlrMsgHandler.Receive()
	replicaResponse := wrapper.GetReplicaRes()
	if !replicaResponse.Ok {
		log.Println(replicaResponse.NodeId)
		return 0
	}

	// close controller connection and message handler
	ctrlrMsgHandler.Close()
	conn.Close()

	log.Printf("Asking %s for a new copy\n", replicaResponse.NodeAddr)

	// open up a connection with node that has clean copy
	conn, err = net.Dial("tcp", replicaResponse.NodeAddr)
	if err != nil {
		log.Println(err)
		return 0
	}
	nodeMsgHandler := messages.NewMessageHandler(conn)

	// send retrieval request to node with clean copy
	nodeMsgHandler.SendRetrievalReqN(filename, chunkname, false)

	// wait for response and confirm checksum matches
	wrapper, _ = nodeMsgHandler.Receive()
	retrievalResponse := wrapper.GetNRetrievalRes()
	if !retrievalResponse.Ok {
		log.Println(retrievalResponse.Message)
		return 0
	}

	// okay to overwrite chunkfile
	if err := chunkFile.Truncate(retrievalResponse.ChunkSize); err != nil {
		log.Println(err)
		return 0
	}

	// write and checksum as we go
	md5Hash := md5.New()
	w := io.MultiWriter(chunkFile, md5Hash)
	io.CopyN(w, nodeMsgHandler, retrievalResponse.ChunkSize)

	// verify chunk checksum matches stored checksum
	if !util.VerifyChecksum(md5Hash.Sum(nil), storedChecksum) {
		log.Printf("Did not receive a valid replica: %s\n", chunkname)
		return 0
	}

	// close node connection and message handler
	nodeMsgHandler.Close()
	conn.Close()

	return int(retrievalResponse.ChunkSize)
}

/* ------ Storage Node Threads ------ */
func handleMessages(conn net.Conn, msgHandler *messages.MessageHandler, ctrlrAddr string, node *storageNode, dest string) {
	defer msgHandler.Close()
	defer conn.Close()

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_KillNode:
			log.Println("I am dead")
			m.Lock()
			sendJoin(ctrlrAddr, node) // update node id
			m.Unlock()
		case *messages.Wrapper_NStorageReq:
			receiveStorageRequest(msgHandler, msg.NStorageReq, node, dest)
		case *messages.Wrapper_NRetrievalReq:
			receiveRetrievalRequest(msgHandler, msg.NRetrievalReq, node, dest)
		case *messages.Wrapper_ReplicaOrder:
			receiveReplicaOrder(msgHandler, msg.ReplicaOrder, node, dest)
		case *messages.Wrapper_MapOrder:
			receiveMapOrder(msgHandler, msg.MapOrder, node, dest)
		case nil:
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
			return
		}
	}
}

func listen(ctrlAddr string, node *storageNode, dest string) error {
	// try to listen on a given port
	listener, err := net.Listen("tcp", node.nodeAddr)
	if err != nil {
		log.Printf("Could not listen on %s", node.nodeAddr)
		os.Exit(1)
	}
	log.Println("Listening for client, controller, and yarm messages...")

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleMessages(conn, msgHandler, ctrlAddr, node, dest)
			// defer conn.Close()
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s host:port listen_port {dest}\n", os.Args[0])
		os.Exit(0)
	}

	// set storage node address
	nodeHostname, err := os.Hostname()
	if err != nil {
		log.Fatalln(err)
	}

	// check if target directory exists
	dest := DEST
	if len(os.Args) == 4 {
		dest = os.Args[3]
	}

	// get disk space info
	freeSpace, err := util.GetDiskSpace()
	if err != nil {
		log.Fatalln(err)
	}

	// set up node data struct
	var node *storageNode = new(storageNode)
	node.nodeAddr = nodeHostname + ":" + os.Args[2]
	node.ctrlrAddr = os.Args[1]
	node.diskSpace = freeSpace
	node.requests = 0
	node.newChunks = make(map[string][]string)
	node.pluginCache = make(map[string]*plugin.Plugin)

	// listen for client, controller, or yarm messages
	go listen(node.ctrlrAddr, node, dest)

	// join cluster and send heartbeats
	err = sendJoin(node.ctrlrAddr, node) // update node id
	if err != nil {
		return
	}

	// send heartbeats
	for {
		time.Sleep(util.RATE * time.Second)
		// time.Sleep(20 * time.Second) // timed out node
		err = sendHeartbeat(node)
		if err != nil {
			return
		}
	}
}
