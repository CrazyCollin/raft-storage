package metaserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
	"rstorage/pkg/raft"
	"sync"
	"time"
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
	//读取已存快照
	metaServer.restoreSnapshot(r.ReadSnapshot())
	//启动apply协程
	go metaServer.ApplyToSTM(metaServer.stopApplyCh)
	return metaServer
}

//
// RequestVote
// @Description: 处理其他meta server的投票请求
//
func (s *MetaServer) RequestVote(ctx context.Context, request *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	resp := &pb.RequestVoteResp{}
	log.Log.Debugf("receive request vote:%v\n", request)
	s.r.HandleRequestVote(request, resp)
	log.Log.Debugf("response request vote:%v\n", resp)
	return resp, nil
}

//
// AppendEntries
// @Description: 处理其他meta server的日志同步请求
//
func (s *MetaServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	//todo append entries
	resp := &pb.AppendEntriesResp{}
	log.Log.Debugf("receive append entries:%v\n", request)
	s.r.HandleAppendEntries(request, resp)
	log.Log.Debugf("response append entries:%v\n", resp)
	return resp, nil
}

//
// Snapshot
// @Description: 处理其他meta server的快照安装请求
//
func (s *MetaServer) Snapshot(ctx context.Context, request *pb.InstallSnapshotReq) (*pb.InstallSnapshotResp, error) {
	resp := &pb.InstallSnapshotResp{}
	log.Log.Debugf("receive snapshot:%v\n", request)
	s.r.HandleInstallSnapshot(request, resp)
	log.Log.Debugf("response snapshot:%v\n", resp)
	return resp, nil
}

//
// ServerGroupMeta
// @Description: 获取server group meta
//
func (s *MetaServer) ServerGroupMeta(ctx context.Context, req *pb.ServerGroupMetaConfigRequest) (*pb.ServerGroupMetaConfigResponse, error) {
	log.Log.Debugf("receive server group meta request:%v\n", req)
	resp := &pb.ServerGroupMetaConfigResponse{}
	//todo encode request
	lastLogIndex, _, isLeader := s.r.Propose(make([]byte, 0))

	//如果不是leader，直接返回
	if !isLeader {
		resp.ErrCode = pb.ErrCode_WRONG_LEADER_ERR
		resp.LeaderId = s.r.GetLeaderID()
		return resp, nil
	}

	//获取响应通知channel，等待apply协程处理完毕，返回结果
	s.mu.Lock()
	ch := s.getRespNotifyChannel(int64(lastLogIndex))
	s.mu.Unlock()
	select {
	case res := <-ch:
		resp.ServerGroupMetas = res.ServerGroupMetas
		resp.BucketOpRes = res.BucketOpRes
		resp.ErrCode = res.ErrCode
	case <-time.After(time.Second * 3):
		resp.ErrCode = pb.ErrCode_RPC_CALL_TIMEOUT_ERR
	}
	go func() {
		s.mu.Lock()
		delete(s.notifyChs, int64(lastLogIndex))
		s.mu.Unlock()
	}()
	return resp, nil
}

//
// getRespNotifyChannel
// @Description: 获取响应通知channel
//
func (s *MetaServer) getRespNotifyChannel(index int64) chan *pb.ServerGroupMetaConfigResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.notifyChs[index]; !ok {
		s.notifyChs[index] = make(chan *pb.ServerGroupMetaConfigResponse, 1)
	}
	return s.notifyChs[index]
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

				resp := &pb.ServerGroupMetaConfigResponse{}

				ch := s.getRespNotifyChannel(applyMsg.GetCommandIndex())
				ch <- resp
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

func (s *MetaServer) StopApply() {
	close(s.stopApplyCh)
}
