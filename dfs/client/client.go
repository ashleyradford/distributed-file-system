package main

import (
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"dfs/util"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

var m = sync.RWMutex{}

const DEST = "/bigdata/students/aeradford/retrieval"

type nodeConn struct {
	handler *messages.MessageHandler
	channel chan chunkData
}

type chunkData struct {
	filename     string
	chunkname    string
	size         int64
	replicaNodes []string
	contents     []byte
}

func makeChunkMap(filename string, filesize int64, chunksize int64) (chunkMap map[string]int64) {
	// initialize chunk data map
	chunkMap = make(map[string]int64)

	numChunks := int(math.Ceil(float64(filesize) / float64(chunksize)))
	log.Printf("Number of chunks: %d", numChunks)

	offset := int64(0)
	remainingBytes := filesize

	// add chunk names to map
	for i := 0; i < numChunks; i++ {
		chunkname := fmt.Sprintf("%s_chunk%d", filename, offset)
		chunkMap[chunkname] = int64(math.Min(float64(chunksize), float64(remainingBytes)))

		offset += chunksize
		remainingBytes -= chunksize
	}

	return chunkMap
}

func readFile(filepath string, filename string, chunkSize map[string]int64, chunkDest map[string]string,
	replicaNodes map[string]*messages.NodeList, connMap map[string]nodeConn) {

	// open and read in file
	file, err := os.Open(filepath)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	// create a buffer to hold the read bytes
	offset := 0
	for {
		// check if chunk exists
		chunkname := fmt.Sprintf("%s_chunk%d", filename, offset)
		chunksize, ok := chunkSize[chunkname]
		if !ok {
			break
		}

		// set up chunk data
		var chunk *chunkData = new(chunkData)
		chunk.filename = filename
		chunk.chunkname = chunkname
		chunk.size = chunksize
		chunk.replicaNodes = replicaNodes[chunkname].Nodes

		// read in data
		buf := make([]byte, chunk.size)
		bytesRead, err := file.Read(buf) // can only read up to 1GB ??
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Println(err)
		}

		// log.Printf("Bytes read: %d\n", bytesRead)
		chunk.contents = buf[0:bytesRead]

		// send data to channel for specified destination
		nodeDest := chunkDest[chunk.chunkname]
		// log.Printf("Sending chunk: %s to: %s", chunk.chunkname, nodeDest)
		connMap[nodeDest].channel <- *chunk

		offset += bytesRead
	}
}

func sendDataToNode(msgHandler *messages.MessageHandler, chunkStream chan chunkData, results chan bool) {
	for {
		// receive data from the dataStream channel specific to this connection
		chunkStruct := <-chunkStream

		// send storage req to specified node
		msgHandler.SendStorageReqN(chunkStruct.filename, chunkStruct.chunkname,
			chunkStruct.size, chunkStruct.replicaNodes)

		// wait for storage node response
		wrapper, _ := msgHandler.Receive()
		response := wrapper.GetNStorageRes()
		if !response.Ok {
			log.Printf("Chunk: %s: %s", chunkStruct.chunkname, response.Message)
			results <- false
			return
		}

		// okay to send file contents
		md5 := md5.New()
		w := io.MultiWriter(msgHandler, md5)
		io.CopyN(w, bytes.NewReader(chunkStruct.contents), chunkStruct.size)

		// send client checksum
		checksum := md5.Sum(nil)
		msgHandler.SendChecksum(checksum)

		// get final response from storage node
		wrapper, _ = msgHandler.Receive()
		checksumResponse := wrapper.GetChecksumRes()
		if !checksumResponse.Ok {
			results <- false
			return
		}

		// add success status to channel
		// log.Printf("Finished sending chunk data for chunk: %s", chunkStruct.chunkname)
		results <- true
	}
}

func getChunkFromNode(filename string, chunkname string, nodeAddr string, file *os.File, results chan bool) {
	// open up a tcp connection with specified node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		results <- false
		return
	}

	// log.Printf("Successfully connected to %s for retrieval\n", nodeAddr)
	msgHandler := messages.NewMessageHandler(conn)

	// send storage req to specified node
	msgHandler.SendRetrievalReqN(filename, chunkname, true)

	// wait for storage node response
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetNRetrievalRes()
	if !response.Ok {
		log.Println(response.Message)
		results <- false
		return
	}

	// find the byte to start at by looking at the chunkname
	re := regexp.MustCompile(`.*_chunk(\d+)`)
	offset, err := strconv.Atoi(re.FindStringSubmatch(chunkname)[1])
	if err != nil {
		log.Println(err)
		results <- false
		return
	}

	// move file pointer to offset and write to file
	m.Lock()
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		log.Println(err)
		results <- false
		return
	}

	// okay to store file, write and checksum as we go
	md5Hash := md5.New()
	w := io.MultiWriter(file, md5Hash)
	io.CopyN(w, msgHandler, response.ChunkSize)
	m.Unlock()

	// get chunk checksum from storage node
	wrapper, _ = msgHandler.Receive()
	nodeChecksum := wrapper.GetMd5().Md5

	// verify chunk checksums match
	if util.VerifyChecksum(nodeChecksum, md5Hash.Sum(nil)) {
		results <- true
	} else {
		log.Printf("Invalid chunk checksum: %s\n", chunkname)
		results <- false
	}

	// close message handler and connection
	msgHandler.Close()
	conn.Close()
}

/* ------ Client Actions ------ */
func storeFile(msgHandler *messages.MessageHandler, filepath string, chunksize int64) {
	// get file size and check if it exists
	info, err := os.Stat(filepath)
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}
	filesize := int64(info.Size())

	// send filename and chunk size map to controller
	filename := path.Base(filepath)
	chunkSizeMap := makeChunkMap(filename, filesize, chunksize)
	msgHandler.SendStorageReqC(filename, filesize, chunkSizeMap)

	// wait for response from controller
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetCStorageRes()
	if !response.Ok {
		log.Println(response.Message)
		return
	}

	// start up node connections and add to addr map
	connMap := make(map[string]nodeConn)
	for _, nodeAddr := range response.Nodes {
		// open up a tcp connection with specified node
		conn, err := net.Dial("tcp", nodeAddr)
		if err != nil {
			log.Println(err)
			return
		}
		defer conn.Close()

		log.Printf("Successfully connected to %s\n", nodeAddr)
		msgHandler := messages.NewMessageHandler(conn)
		defer msgHandler.Close()

		// add handler and channel to map
		connMap[nodeAddr] = nodeConn{
			handler: msgHandler,
			channel: make(chan chunkData),
		}
	}

	// start a goroutine to send data to each connection
	numChunks := len(response.ChunkMap)
	results := make(chan bool, numChunks)
	for addr := range connMap {
		go sendDataToNode(connMap[addr].handler, connMap[addr].channel, results)
	}

	readFile(filepath, filename, chunkSizeMap, response.ChunkMap, response.ReplicaNodes, connMap)

	// check that all non-replica chunks (aka the first node addr) got sent succesfully
	var failed bool
	for i := 0; i < numChunks; i++ {
		success := <-results
		if !success {
			failed = true
		}
	}

	if failed {
		fmt.Println("Failed to store successfully.")
	} else {
		fmt.Println("File successfully stored.")
	}
}

func retrieveFile(msgHandler *messages.MessageHandler, filename string, dest string) {
	// send request first before we open file
	msgHandler.SendRetrievalReqC(filename)

	// wait for controller response
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetCRetrievalRes()
	if !response.Ok {
		fmt.Println(response.Message)
		return
	}
	// log.Printf("Received chunk mappings from controller: %v", response.ChunkMap)

	// open file to write to (check if file already exists)
	file, err := os.OpenFile(dest+"/"+filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			fmt.Println("File already exists, will not overwrite.")
		} else {
			fmt.Println(err)
		}
		return
	}
	defer file.Close()

	// open up connections with nodes to get chunks
	numChunks := len(response.ChunkMap)
	results := make(chan bool, numChunks)
	for chunkname, nodeAddr := range response.ChunkMap {
		go getChunkFromNode(filename, chunkname, nodeAddr, file, results)
	}

	// check that we have receievd all chunks successfully
	var failed bool
	for i := 0; i < numChunks; i++ {
		success := <-results
		if !success {
			failed = true
		}
	}

	if failed {
		os.Remove(dest + "/" + filename)
		fmt.Println("Failed to retrieve successfully.")
	} else {
		fmt.Println("File successfully retrieved, calculating checksum.")

		// go back to beg of file
		_, err = file.Seek(0, 0)
		if err != nil {
			log.Println(err)
			return
		}

		// read file into hash function
		md5Hash := md5.New()
		_, err = io.Copy(md5Hash, file)
		if err != nil {
			log.Println(err)
		}

		fmt.Printf("%s: %x\n", filename, md5Hash.Sum(nil))
	}
}

func deleteFile(msgHandler *messages.MessageHandler, filename string) {
	msgHandler.SendDeleteReq(filename)

	// wait for response from controller
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetDeleteRes()
	fmt.Println(response.Message)
}

func getList(msgHandler *messages.MessageHandler) {
	// send ls request to controller
	msgHandler.SendListRequest(true)

	// wait for response from controller
	wrapper, _ := msgHandler.Receive()
	msg, _ := wrapper.Msg.(*messages.Wrapper_ListRes)

	sort.Strings(msg.ListRes.Filenames)

	for _, filename := range msg.ListRes.Filenames {
		fmt.Println(filename)
	}
}

func getNodes(msgHandler *messages.MessageHandler) {
	// send nodes request to controller
	msgHandler.SendNodesRequest()

	// wait for response from controller
	wrapper, _ := msgHandler.Receive()
	msg, _ := wrapper.Msg.(*messages.Wrapper_NodesRes)
	remSpace := float64(msg.NodesRes.DiskSpace) / math.Pow(2, 30)
	nodeReqMap := msg.NodesRes.NodeReqs

	fmt.Printf("Available space: %.2f GB\n", remSpace)

	// sort the nodes for output
	nodes := make([]string, 0, len(nodeReqMap))
	for node := range nodeReqMap {
		nodes = append(nodes, node)
	}

	// sort the nodes
	sort.Strings(nodes)

	for _, node := range nodes {
		fmt.Printf("%s: %d requests successfully completed\n", node, nodeReqMap[node])
	}
}

func printUsage() {
	fmt.Println("Usage: ./client host:port action {file-name} {chunk-size MB} {dest}")
}

func main() {
	if len(os.Args) < 3 {
		printUsage()
		return
	}

	var filepath string
	if len(os.Args) > 3 {
		filepath = os.Args[3]
	}

	// check for support action
	action := os.Args[2]
	switch action {
	case "put", "get", "delete", "ls", "nodes":
		break
	default:
		fmt.Println("Not a supported action:", action)
		fmt.Println("Available actions: { \"put\", \"get\", \"delete\", \"ls\", \"nodes\" }")
		return
	}

	// try to connect to given controller
	hostAddr := os.Args[1]
	conn, err := net.Dial("tcp", hostAddr)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	// send request to controller
	msgHandler := messages.NewMessageHandler(conn)
	switch action {
	case "put":
		// set chunk size
		var chunksize int64
		if len(os.Args) == 5 {
			val, _ := strconv.ParseFloat(os.Args[4], 64)
			chunksize = int64(val * math.Pow(2, 20)) // convert from MB to bytes
		} else {
			chunksize = int64(math.Pow(2, 20)) // 1MB default chunk size
		}

		if len(os.Args) > 5 {
			printUsage()
			return
		}
		storeFile(msgHandler, filepath, chunksize)
	case "get":
		// check if target directory exists
		dest := DEST
		if len(os.Args) == 5 {
			dest = os.Args[5]
		}

		if len(os.Args) > 5 {
			printUsage()
			return
		}
		retrieveFile(msgHandler, filepath, dest) // should be a filename
	case "delete":
		if len(os.Args) > 4 {
			printUsage()
			return
		}
		deleteFile(msgHandler, filepath) // should be a filename
	case "ls":
		if len(os.Args) > 3 {
			printUsage()
			return
		}
		getList(msgHandler)
	case "nodes":
		if len(os.Args) > 3 {
			printUsage()
			return
		}
		getNodes(msgHandler)
	}
	defer msgHandler.Close()
}
