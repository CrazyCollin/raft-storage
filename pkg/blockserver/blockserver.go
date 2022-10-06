package blockserver

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
	"rstorage/pkg/raft"
	"sync"
)

type BlockServer struct {
	id      int
	groupID int

	mu sync.Mutex
	r  *raft.Raft

	applyCh   chan *pb.ApplyMsg
	notifyChs map[int64]chan *pb.FileBlockOpResponse

	dataPath string

	stm map[string]string

	stopApplyCh chan interface{}
	lastApplied int

	pb.UnimplementedRaftServiceServer
	pb.UnimplementedFileBlockServiceServer
}

func NewBlockServer(nodes map[int]string, nodeID int, groupID int, dataPath string) *BlockServer {
	var clientEnds []*raft.RaftClientEnd
	//建立raft节点通信
	for i, addr := range nodes {
		clientEnd := raft.NewRaftClientEnd(uint64(i), addr)
		clientEnds = append(clientEnds, clientEnd)
	}
	applyCh := make(chan *pb.ApplyMsg)

	logDb := engine.KvStoreFactory("leveldb", fmt.Sprintf("%s/data_log_%d", dataPath, nodeID))

	r := raft.BuildRaft(clientEnds, nodeID, logDb, applyCh, 500, 1500)

	blockServer := &BlockServer{
		id:        nodeID,
		groupID:   groupID,
		r:         r,
		applyCh:   applyCh,
		dataPath:  dataPath,
		notifyChs: make(map[int64]chan *pb.FileBlockOpResponse),
	}
	blockServer.stopApplyCh = make(chan interface{})
	blockServer.restoreSnapshot(r.ReadSnapshot())
	go blockServer.ApplyingToSTM(blockServer.stopApplyCh)
	return blockServer
}

func (s *BlockServer) restoreSnapshot(snapshot []byte) {
	//todo
}

// takeSnapshot
// @Description: take snapshot
func (s *BlockServer) takeSnapshot(index int) {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(s.stm); err != nil {
		log.Log.Debugf("encode snapshot error:%v", err)
	}
	s.r.Snapshot(index, byteBuffer.Bytes())
}

func (s *BlockServer) ApplyingToSTM(stopApplyCh chan<- interface{}) {
	//todo
}

func (s *BlockServer) StopApply() {
	close(s.stopApplyCh)
}
