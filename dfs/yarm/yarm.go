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
	"time"
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

func sendJob(nodeAddr string, job_hash string, job []byte, chunks []string, reducerNodes []string,
	numMappers int, ok chan bool, shuffleCh chan []string) {
	// try to connect to given node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		ok <- false
		return
	}

	// create message handler for nodes
	msgHandler := messages.NewMessageHandler(conn)

	// send the map order
	msgHandler.SendJobOrder(job_hash, job, chunks, reducerNodes, numMappers)

	// wait for map response
	wrapper, _ := msgHandler.Receive()
	msg, _ := wrapper.Msg.(*messages.Wrapper_MapStatus)
	if msg.MapStatus.Ok {
		ok <- true
		fmt.Printf("%s: %s\n", nodeAddr, msg.MapStatus.Message)
	} else {
		ok <- false
		fmt.Printf("%s: %s\n", nodeAddr, msg.MapStatus.Message)
	}

	// check when ready to shuffle, wait for map phase completion
	// reducerNodes := <-shuffleCh
	// if reducerNodes != nil {
	// 	msgHandler.SendReducerList(reducerNodes)
	// }

	// wait for response letting manager know its done sending data to reducer

	// close message handler and connection
	msgHandler.Close()
	conn.Close()
}

func selectReducers(nodeChunkMap map[string][]string, numReducers int) []string {
	var nodes []string
	count := 0
	for node := range nodeChunkMap {
		nodes = append(nodes, node)
		count++
		if count == numReducers {
			break
		}
	}

	fmt.Println("--------------\nReducer nodes:\n--------------")
	for _, node := range nodes {
		fmt.Printf("%s\n", node)
	}
	fmt.Println()

	return nodes
}

// func notifyReducer(nodeAddr string, numMappers int) error {
// 	// open up a tcp connection with specified node
// 	conn, err := net.Dial("tcp", nodeAddr)
// 	if err != nil {
// 		return err
// 	}

// 	// log.Printf("Successfully connected to reducer %s\n", nodeAddr)
// 	msgHandler := messages.NewMessageHandler(conn)

// 	// let reducer know data is coming
// 	// msgHandler.SendReduceNotice(numMappers)

// 	// close message handler and connection
// 	msgHandler.Close()
// 	conn.Close()

// 	return nil
// }

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
	reducerNodes := selectReducers(nodeChunkMap, numReducers)

	// for _, nodeAddr := range reducerNodes {
	// 	err := notifyReducer(nodeAddr, len(nodeChunkMap))
	// 	if err != nil {
	// 		fmt.Println("Failed to notify reducer node, aborting job.")
	// 		return
	// 	}
	// }

	// send the job to the mapper nodes
	fmt.Println("Starting map phase.")
	mapStatus := make(chan bool, len(nodeChunkMap))
	shuffleCh := make(chan []string, len(nodeChunkMap))
	for nodeAddr, chunks := range nodeChunkMap {
		go sendJob(nodeAddr, string(fmt.Sprintf("%x", md5Hash.Sum(nil))), soBytes.Bytes(),
			chunks, reducerNodes, len(nodeChunkMap), mapStatus, shuffleCh)
	}

	// check that all map tasks are complete
	var failed bool
	for i := 0; i < len(nodeChunkMap); i++ {
		success := <-mapStatus
		if !success {
			failed = true
		}
	}

	if failed {
		fmt.Println("Failed to complete map phase, aborting job.")
		return
	} else {
		fmt.Printf("Map phase successfully completed, starting shuffle phase.\n\n")
	}

	// now let opened node connections know where the reducers are
	for i := 0; i < len(nodeChunkMap); i++ {
		shuffleCh <- reducerNodes
	}

	time.Sleep(5 * time.Second)
	// push job to computation nodes
	// pushJob(nodeChunkMap, numReducers)
}
