package messages

import (
	"encoding/binary"
	"net"

	"google.golang.org/protobuf/proto"
)

/* ------------- Inherit net con capabilities ------------- */
type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	m := &MessageHandler{
		conn: conn,
	}
	return m
}

/* ------- Implement Read and Write for io functions ------- */
func (m *MessageHandler) Read(p []byte) (n int, err error) {
	return m.conn.Read(p)
}

func (m *MessageHandler) Write(p []byte) (n int, err error) {
	return m.conn.Write(p)
}

/* --------- Sending and Receiving Raw Bytes --------- */
func (m *MessageHandler) readN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) { // loop ensures reliable reading
		n, err := m.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *MessageHandler) writeN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) { // loop ensures reliable writing
		n, err := m.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

/* ------------- Send and Receive Messages ------------- */
func (m *MessageHandler) Send(wrapper *Wrapper) error {
	// converting from Go structs into binary data that we can send across the network
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.writeN(prefix)
	m.writeN(serialized)

	return nil
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	m.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.readN(payload) // read payload amount of data

	wrapper := &Wrapper{}
	// convert data back into Go structs and use as normal
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

/* --------------- Build Messages --------------- */
func (m *MessageHandler) SendJoinRequest(nodeAddr string, diskSpace uint64) error {
	joinReq := JoinReq{NodeAddr: nodeAddr, DiskSpace: diskSpace}
	joinWrapper := &Wrapper{
		Msg: &Wrapper_JoinReq{JoinReq: &joinReq},
	}
	return m.Send(joinWrapper)
}

func (m *MessageHandler) SendJoinResponse(accept bool, nodeId string) error {
	joinRes := JoinRes{Accept: accept, NodeId: nodeId}
	joinWrapper := &Wrapper{
		Msg: &Wrapper_JoinRes{JoinRes: &joinRes},
	}
	return m.Send(joinWrapper)
}

func (m *MessageHandler) SendKillNode(kill bool) error {
	status := KillNode{Kill: kill}
	statusWrapper := &Wrapper{
		Msg: &Wrapper_KillNode{KillNode: &status},
	}
	return m.Send(statusWrapper)
}

func (m *MessageHandler) SendHeartbeat(nodeId string, diskSpace uint64, requests int64,
	newChunks map[string][]string) error {

	hb := Heartbeat{NodeId: nodeId, DiskSpace: diskSpace,
		Requests: requests, NewChunks: make(map[string]*ChunkList)}

	// add node list to map
	for chunk := range newChunks {
		hb.NewChunks[chunk] = &ChunkList{Chunks: newChunks[chunk]}
	}

	hbWrapper := &Wrapper{
		Msg: &Wrapper_Heartbeat{Heartbeat: &hb},
	}
	return m.Send(hbWrapper)
}

func (m *MessageHandler) SendNodesRequest() error {
	node := NodesReq{Nodes: true}
	nodeWrapper := &Wrapper{
		Msg: &Wrapper_NodesReq{NodesReq: &node},
	}
	return m.Send(nodeWrapper)
}

func (m *MessageHandler) SendNodesResponse(diskSpace uint64, nodeReqMap map[string]int64) error {
	nodeRes := NodesRes{DiskSpace: diskSpace, NodeReqs: nodeReqMap}
	nodeWrapper := &Wrapper{
		Msg: &Wrapper_NodesRes{NodesRes: &nodeRes},
	}
	return m.Send(nodeWrapper)
}

func (m *MessageHandler) SendStorageReqC(filename string, filesize int64, chunkSizeMap map[string]int64) error {
	msg := CStorageReq{Filename: filename, Filesize: filesize, ChunkSizes: chunkSizeMap}
	wrapper := &Wrapper{
		Msg: &Wrapper_CStorageReq{CStorageReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendStorageResC(ok bool, message string, nodes []string, chunkMap map[string]string,
	replicaNodes map[string][]string) error {

	msg := CStorageRes{Ok: ok, Message: message, Nodes: nodes, ChunkMap: chunkMap, ReplicaNodes: make(map[string]*NodeList)}

	// add node list to map
	for chunk := range replicaNodes {
		msg.ReplicaNodes[chunk] = &NodeList{Nodes: replicaNodes[chunk]}
	}

	wrapper := &Wrapper{
		Msg: &Wrapper_CStorageRes{CStorageRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendStorageReqN(filename string, chunkname string, chunksize int64, replicaNodes []string) error {
	msg := NStorageReq{Filename: filename, Chunkname: chunkname, Chunksize: chunksize, ReplicaNodes: replicaNodes}
	wrapper := &Wrapper{
		Msg: &Wrapper_NStorageReq{NStorageReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendStorageResN(ok bool, message string) error {
	msg := NStorageRes{Ok: ok, Message: message}
	wrapper := &Wrapper{
		Msg: &Wrapper_NStorageRes{NStorageRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendChecksum(checksum []byte) error {
	msg := Checksum{Md5: checksum}
	wrapper := &Wrapper{
		Msg: &Wrapper_Md5{Md5: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendChecksumResponse(ok bool, message string) error {
	msg := ChecksumRes{Ok: ok, Message: message}
	wrapper := &Wrapper{
		Msg: &Wrapper_ChecksumRes{ChecksumRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendListRequest(ls bool) error {
	msg := ListReq{Ls: true}
	wrapper := &Wrapper{
		Msg: &Wrapper_ListReq{ListReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendListResponse(filenames []string) error {
	msg := ListRes{Filenames: filenames}
	wrapper := &Wrapper{
		Msg: &Wrapper_ListRes{ListRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendRetrievalReqC(filename string) error {
	msg := CRetrievalReq{Filename: filename}
	wrapper := &Wrapper{
		Msg: &Wrapper_CRetrievalReq{CRetrievalReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendRetrievalResC(ok bool, message string, numChunks int64, nodeChunks map[string][]string) error {
	msg := CRetrievalRes{Ok: ok, Message: message, NumChunks: numChunks, NodeChunks: make(map[string]*NodeList)}

	// add node list to map
	for chunk := range nodeChunks {
		msg.NodeChunks[chunk] = &NodeList{Nodes: nodeChunks[chunk]}
	}

	wrapper := &Wrapper{
		Msg: &Wrapper_CRetrievalRes{CRetrievalRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendRetrievalReqN(filename string, chunkname string, checksum bool) error {
	msg := NRetrievalReq{Filename: filename, Chunkname: chunkname, Checksum: checksum}
	wrapper := &Wrapper{
		Msg: &Wrapper_NRetrievalReq{NRetrievalReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendRetrievalResN(ok bool, message string, chunksize int64) error {
	msg := NRetrievalRes{Ok: ok, Message: message, ChunkSize: chunksize}
	wrapper := &Wrapper{
		Msg: &Wrapper_NRetrievalRes{NRetrievalRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendDeleteReq(filename string) error {
	msg := DeleteReq{Filename: filename}
	wrapper := &Wrapper{
		Msg: &Wrapper_DeleteReq{DeleteReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendDeleteRes(ok bool, message string) error {
	msg := DeleteRes{Ok: ok, Message: message}
	wrapper := &Wrapper{
		Msg: &Wrapper_DeleteRes{DeleteRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendReplicaOrder(filename string, chunkname string, chunksize int64, replicaNodes []string) error {
	msg := ReplicaOrder{Filename: filename, Chunkname: chunkname, Chunksize: chunksize, ReplicaNodes: replicaNodes}
	wrapper := &Wrapper{
		Msg: &Wrapper_ReplicaOrder{ReplicaOrder: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendOrderRes(ok bool, message string) error {
	msg := OrderRes{Ok: ok, Message: message}
	wrapper := &Wrapper{
		Msg: &Wrapper_OrderRes{OrderRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendReplicaReq(nodeId string, filename string, chunkname string) error {
	msg := ReplicaReq{NodeId: nodeId, Filename: filename, Chunkname: chunkname}
	wrapper := &Wrapper{
		Msg: &Wrapper_ReplicaReq{ReplicaReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendReplicaRes(ok bool, node_id string, node_addr string) error {
	msg := ReplicaRes{Ok: ok, NodeId: node_id, NodeAddr: node_addr}
	wrapper := &Wrapper{
		Msg: &Wrapper_ReplicaRes{ReplicaRes: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}
