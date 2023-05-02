package main

import (
	"dfs/messages"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
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
	log.Printf("Received chunk mappings from controller.")

	return chunkNodeMap
}

func determineMappers(chunkNodeMap map[string][]string) map[string][]string {
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

	log.Println("Mapper nodes: ")
	for node, chunks := range nodeChunkMap {
		log.Printf("    %s: %d chunks", node, len(chunks))
	}

	return nodeChunkMap
}

func sendJob(nodeAddr string, job []byte, chunks []string) {
	// try to connect to given node
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println(err)
		return
	}

	// create message handler for nodes
	msgHandler := messages.NewMessageHandler(conn)

	// send the map order
	msgHandler.SendMapOrder(job, chunks)

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

	// get the bytes of the so file
	soBytes, err := ioutil.ReadFile(os.Args[2])
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
	// numReducers := 4
	// if len(os.Args) == 5 {
	// 	numReducers, err = strconv.Atoi(os.Args[4])
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}
	// }

	// check that file exists and map chunks to nodes
	chunkNodeMap := getFileMapping(msgHandler, filename)
	if chunkNodeMap == nil {
		log.Println("Error determining nodes for mapper jobs")
		return
	}
	nodeChunkMap := determineMappers(chunkNodeMap)

	// send the job to the mapper nodes
	for nodeAddr, chunks := range nodeChunkMap {
		go sendJob(nodeAddr, soBytes, chunks)
	}

	time.Sleep(20 * time.Second)

	// push job to compumaketation nodes
	// pushJob(nodeChunkMap, numReducers)
}
