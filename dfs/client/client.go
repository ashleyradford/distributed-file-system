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

type chunkData struct {
	offset   int64
	size     int64
	contents []byte
}

func splitFile(filepath string, filesize int64, chunksize uint64) (chunkMap map[string]*chunkData) {
	// open and read in file
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	// get filename
	filename := path.Base(os.Args[3])

	// create chunk data map
	chunkMap = make(map[string]*chunkData)

	// create a buffer to hold the read bytes
	numChunks := 0
	offset := 0
	for {
		var chunk *chunkData = new(chunkData)
		buf := make([]byte, chunksize)
		bytesRead, err := file.Read(buf) // can only read up to 1GB ??
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatalln(err)
		}

		// log.Printf("Bytes read: %d\n", bytesRead)
		chunk.offset = int64(offset)
		chunk.size = int64(bytesRead)
		chunk.contents = buf[0:bytesRead]

		chunkname := fmt.Sprintf("%s_chunk%d", filename, offset)
		chunkMap[chunkname] = chunk

		offset += bytesRead
		numChunks += 1
	}

	log.Printf("Number of chunks: %d", numChunks)

	return chunkMap
}

func sendDataToNode(filename string, chunkname string, nodeAddr string,
	replicaNodes []string, chunkStruct *chunkData, results chan bool) {

	// open up a tcp connection with specified node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		results <- false
		return
	}

	// log.Printf("Successfully connected to %s to store %s\n", nodeAddr, chunkname)
	msgHandler := messages.NewMessageHandler(conn)

	// send storage req to specified node
	msgHandler.SendStorageReqN(filename, chunkname, chunkStruct.size, replicaNodes)

	// wait for storage node response
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetNStorageRes()
	if !response.Ok {
		fmt.Println(response.Message)
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
	results <- true

	// close message handler and connection
	msgHandler.Close()
	conn.Close()
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
		fmt.Println(response.Message)
		results <- false
		return
	}

	// find the byte to start at by looking at the chunkname
	re := regexp.MustCompile(`.*_chunk(\d+)`)
	offset, err := strconv.Atoi(re.FindStringSubmatch(chunkname)[1])
	if err != nil {
		log.Fatal(err)
		results <- false
		return
	}

	// move file pointer to offset and write to file
	m.Lock()
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		log.Fatal(err)
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
func storeFile(msgHandler *messages.MessageHandler, filepath string, chunksize uint64) {
	// get file size and check if it exists
	info, err := os.Stat(filepath)
	if err != nil {
		fmt.Println("Error opening file", err)
		return
	}
	filesize := int64(info.Size())

	// create chunk data map and { chunk : size } map (for requests)
	chunkDataMap := splitFile(filepath, filesize, chunksize)
	chunkSizeMap := make(map[string]int64)

	// fill in chunkSizeMap for controller request
	for chunkname, data := range chunkDataMap {
		chunkSizeMap[chunkname] = data.size
	}

	// send filename and chunk size map to controller
	filename := path.Base(filepath)
	msgHandler.SendStorageReqC(filename, filesize, chunkSizeMap)

	// wait for response from controller
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetCStorageRes()
	if !response.Ok {
		log.Println(response.Message)
		return
	}

	// for each main chunk : node mapping, open a connection and send that data over
	numChunks := len(response.ChunkMap)
	results := make(chan bool, numChunks)
	for chunkname, nodeAddr := range response.ChunkMap {
		replicaNodes := response.ReplicaNodes[chunkname]
		go sendDataToNode(filename, chunkname, nodeAddr, replicaNodes.Nodes, chunkDataMap[chunkname], results)
	}

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
			log.Fatal(err)
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
		log.Fatalln(err)
		return
	}
	defer conn.Close()

	// send request to controller
	msgHandler := messages.NewMessageHandler(conn)
	switch action {
	case "put":
		// set chunk size
		var chunksize uint64
		if len(os.Args) == 5 {
			val, _ := strconv.ParseFloat(os.Args[4], 64)
			chunksize = uint64(val * math.Pow(2, 20)) // convert from MB to bytes
		} else {
			chunksize = uint64(math.Pow(2, 20)) // 1MB default chunk size
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
