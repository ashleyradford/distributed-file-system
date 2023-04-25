package main

import (
	"dfs/messages"
	"fmt"
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

	chunkNodes := make(map[string][]string)
	for chunk := range response.ChunkNodes {
		chunkNodes[chunk] = response.ChunkNodes[chunk].Nodes
	}
	log.Printf("Received chunk mappings from controller: %v", chunkNodes)

	return chunkNodes
}

func pushJob(chunkNodes map[string][]string, numReducers int) {

}

func main() {
	if len(os.Args) < 4 || len(os.Args) > 5 {
		fmt.Printf("Usage: %s host:port job input-file {reducer-nodes}\n", os.Args[0])
		return
	}

	// get filepath and filename
	filepath := os.Args[3]
	filename := path.Base(filepath)

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
			fmt.Println(err)
			return
		}
	}

	// check that file exists and get chunks
	chunkNodes := getFileMapping(msgHandler, filename)
	if chunkNodes == nil {
		return
	}

	// push job to computation nodes
	pushJob(chunkNodes, numReducers)
}
