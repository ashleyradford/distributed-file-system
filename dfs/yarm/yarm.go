package main

import (
	"bytes"
	"crypto/md5"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strconv"
)

const INTER_DEST = "/bigdata/students/aeradford/mydfs/intermediate"
const FINAL_DEST = "/bigdata/students/aeradford/mydfs/jobs"

func getFileMapping(msgHandler *messages.MessageHandler, filename string) map[string][]string {
	// send request first before we open file
	msgHandler.SendMapReq(filename)

	// wait for controller response
	wrapper, _ := msgHandler.Receive()
	response := wrapper.GetMapRes()
	if !response.Ok {
		fmt.Println(response.Message)
		return nil
	}

	chunkNodeMap := make(map[string][]string)
	for chunk := range response.ChunkNodes {
		chunkNodeMap[chunk] = response.ChunkNodes[chunk].Nodes
	}
	fmt.Println("Received chunk mappings from controller.")

	return chunkNodeMap
}

func selectMappers(chunkNodeMap map[string][]string) map[string][]string {
	chunkCountMap := make(map[string]int)
	nodeChunkMap := make(map[string][]string)

	for chunk, nodes := range chunkNodeMap {
		minNode := nodes[0]
		minCount := chunkCountMap[minNode] // zero value of int is 0
		for i := 1; i < len(nodes); i++ {
			if chunkCountMap[nodes[i]] < minCount {
				minNode = nodes[i]
				minCount = chunkCountMap[nodes[i]]
			}
		}

		// add chunk to node with smallest count
		if _, ok := nodeChunkMap[minNode]; ok {
			nodeChunkMap[minNode] = make([]string, 0)
		}
		nodeChunkMap[minNode] = append(nodeChunkMap[minNode], chunk)
		chunkCountMap[minNode] = chunkCountMap[minNode] + 1
	}

	fmt.Println("-------------\nMapper nodes:\n-------------")
	for node, chunks := range nodeChunkMap {
		fmt.Printf("%s: %d chunks\n", node, len(chunks))
	}
	fmt.Println()

	return nodeChunkMap
}

func selectReducers(nodeChunkMap map[string][]string, numReducers int) map[string]bool {
	nodeSet := make(map[string]bool)
	count := 0
	for node := range nodeChunkMap {
		nodeSet[node] = true
		count++
		if count == numReducers {
			break
		}
	}

	fmt.Println("--------------\nReducer nodes:\n--------------")
	for node := range nodeSet {
		fmt.Printf("%s\n", node)
	}
	fmt.Println()

	return nodeSet
}

func sendJob(nodeAddr string, job_hash string, job []byte, isReducer bool, chunks []string, reducerNodes []string,
	numMappers int, jobChan chan bool) {

	// try to connect to given node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		jobChan <- false
		return
	}

	// create message handler for nodes
	msgHandler := messages.NewMessageHandler(conn)

	// send the map order
	msgHandler.SendJobOrder(job_hash, job, isReducer, chunks, reducerNodes, numMappers)

	// wait for map response
	wrapper, _ := msgHandler.Receive()
	msg, _ := wrapper.Msg.(*messages.Wrapper_JobStatus)
	if msg.JobStatus.Ok {
		jobChan <- true
		fmt.Printf("%s: %s\n", nodeAddr, msg.JobStatus.Message)
	} else {
		jobChan <- false
		fmt.Printf("%s: %s\n", nodeAddr, msg.JobStatus.Message)
	}

	// close message handler and connection
	msgHandler.Close()
	conn.Close()
}

func main() {
	if len(os.Args) < 4 || len(os.Args) > 5 {
		fmt.Printf("Usage: %s host:port job input-file {reducer-nodes}\n", os.Args[0])
		return
	}

	// get filepath and filename
	filepath := os.Args[3]
	filename := path.Base(filepath)

	// open the so file
	soFile, err := os.Open(os.Args[2])
	if err != nil {
		log.Println(err)
		return
	}
	defer soFile.Close()

	// set up hash for so contents and copy bytes over
	md5Hash := md5.New()
	soBytes := new(bytes.Buffer)
	w := io.MultiWriter(soBytes, md5Hash)
	_, err = io.Copy(w, soFile)
	if err != nil {
		log.Println(err)
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

	// create message handler for controller
	msgHandler := messages.NewMessageHandler(conn)
	defer msgHandler.Close()

	// check number of reducers given
	numReducers := 4
	if len(os.Args) == 5 {
		numReducers, err = strconv.Atoi(os.Args[4])
		if err != nil {
			log.Println(err)
			return
		}
	}

	// check that file exists and map chunks to nodes
	chunkNodeMap := getFileMapping(msgHandler, filename)
	if chunkNodeMap == nil {
		log.Println("Error determining nodes for mapper jobs")
		return
	}

	// determine mapper and reducer nodes
	nodeChunkMap := selectMappers(chunkNodeMap)
	reducerNodeSet := selectReducers(nodeChunkMap, numReducers)
	reducerNodeList := make([]string, 0)
	for node := range reducerNodeSet {
		reducerNodeList = append(reducerNodeList, node)
	}

	// send the job to the mapper nodes
	fmt.Println("Starting map phase.")
	jobChan := make(chan bool, len(nodeChunkMap))
	for nodeAddr, chunks := range nodeChunkMap {
		isReducer := false
		if _, ok := reducerNodeSet[nodeAddr]; ok {
			isReducer = true
		}
		go sendJob(nodeAddr, string(fmt.Sprintf("%x", md5Hash.Sum(nil))), soBytes.Bytes(),
			isReducer, chunks, reducerNodeList, len(nodeChunkMap), jobChan)
	}

	// check that all job tasks are complete
	var failed bool
	for i := 0; i < len(nodeChunkMap)*2+len(reducerNodeSet); i++ {
		success := <-jobChan
		if !success {
			failed = true
		}
		if success && i == len(nodeChunkMap)*2-1 {
			fmt.Println("\nAll data has been sent to reducers.")
		}
	}

	if failed {
		fmt.Println("Failed to complete map reduce job, aborting job.")
		return
	} else {
		fmt.Println("Map reduce job successfully completed, starting shuffle phase.")
	}
}
