package metaserver

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
	"rstorage/pkg/raft"
	"sync"
)

//
// MetaServer
// @Description: MetaServer
//
type MetaServer struct {
	mu sync.Mutex
	r  *raft.Raft

	applyCh   chan *pb.ApplyMsg
	notifyChs map[int64]chan *pb.ServerGroupMetaConfigResponse

	db engine.KvStore

	stm       ConfigSTM
	configSTM map[string]string

	stopApplyCh chan interface{}
	lastApplied int

	pb.UnimplementedRaftServiceServer
	pb.UnimplementedMetaServiceServer
}

//
// NewMetaServer
// @Description: NewMetaServer
//
func NewMetaServer(nodes map[int]string, nodeID int, dataPath string) *MetaServer {

	var clientEnds []*raft.RaftClientEnd
	//建立raft节点通信
	for i, addr := range nodes {
		clientEnd := raft.NewRaftClientEnd(uint64(i), addr)
		clientEnds = append(clientEnds, clientEnd)
	}
	applyCh := make(chan *pb.ApplyMsg)

	logDb := engine.KvStoreFactory("leveldb", fmt.Sprintf("%s/data_log_%d", dataPath, nodeID))

	r := raft.BuildRaft(clientEnds, nodeID, logDb, applyCh, 500, 1500)

	db := engine.KvStoreFactory("leveldb", fmt.Sprintf("%s/data_meta_%d", dataPath, nodeID))

	metaServer := &MetaServer{
		r:         r,
		applyCh:   make(chan *pb.ApplyMsg),
		notifyChs: make(map[int64]chan *pb.ServerGroupMetaConfigResponse),
		stm:       NewPersistConfig(db),
		db:        db,

		stopApplyCh: make(chan interface{}),
	}
	//todo 从快照中恢复数据

	go metaServer.ApplyToSTM(metaServer.stopApplyCh)
	return metaServer
}

//
// restoreSnapshot
// @Description: restore snapshot
//
func (s *MetaServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil {
		return
	}
	buffer := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&s.configSTM); err != nil {
		log.Log.Errorf("decode snapshot error:%v", err)
	}
	jsonSTM, _ := json.Marshal(s.configSTM)
	log.Log.Debugf("restore snapshot:%s", string(jsonSTM))
}

//
// takeSnapshot
// @Description: take snapshot
//
func (s *MetaServer) takeSnapshot(index int) {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	if err := encoder.Encode(s.configSTM); err != nil {
		log.Log.Debugf("encode snapshot error:%v", err)
	}
	s.r.Snapshot(index, byteBuffer.Bytes())
}

func (s *MetaServer) ApplyToSTM(stopApplyCh <-chan interface{}) {
	for {
		select {
		case <-stopApplyCh:
			return
		case applyMsg := <-s.applyCh:
			if applyMsg.CommandValid {
				//todo command apply to stm

			} else if applyMsg.SnapshotValid {
				s.mu.Lock()
				if s.r.CondInstallSnapshot(int(applyMsg.SnapshotTerm), int(applyMsg.SnapshotIndex), applyMsg.Snapshot) {
					log.Log.Debugf("install snapshot")
					s.restoreSnapshot(applyMsg.Snapshot)
					s.lastApplied = int(applyMsg.SnapshotIndex)
				}
				s.mu.Unlock()
			}
		}
	}
}
