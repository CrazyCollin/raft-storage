package blockserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
	"rstorage/pkg/raft"
	"sync"
	"time"
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

func (s *BlockServer) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	resp := &pb.RequestVoteResp{}
	log.Log.Debugf("block server %d receive request vote from %d\n", s.id, req.CandidateId)
	s.r.HandleRequestVote(req, resp)
	log.Log.Debugf("block server %d response request vote to %d\n", s.id, req.CandidateId)
	return resp, nil
}

func (s *BlockServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesReq) (*pb.AppendEntriesResp, error) {
	resp := &pb.AppendEntriesResp{}
	log.Log.Debugf("block server %d receive append entries from %d\n", s.id, req.LeaderId)
	s.r.HandleAppendEntries(req, resp)
	log.Log.Debugf("block server %d response append entries to %d\n", s.id, req.LeaderId)
	return resp, nil
}

func (s *BlockServer) Snapshot(ctx context.Context, req *pb.InstallSnapshotReq) (*pb.InstallSnapshotResp, error) {
	resp := &pb.InstallSnapshotResp{}
	log.Log.Debugf("block server %d receive snapshot from %d\n", s.id, req.LeaderId)
	s.r.HandleInstallSnapshot(req, resp)
	log.Log.Debugf("block server %d response snapshot to %d\n", s.id, req.LeaderId)
	return resp, nil
}

// FileBlockOp
// @Description: file block操作
func (s *BlockServer) FileBlockOp(ctx context.Context, req *pb.FileBlockOpRequest) (*pb.FileBlockOpResponse, error) {
	resp := &pb.FileBlockOpResponse{}
	encodedRequest := EncodeBlockServerRequest(req)
	lastLogIndex, _, isLeader := s.r.Propose(encodedRequest)
	if !isLeader {
		resp.ErrCode = pb.ErrCode_WRONG_LEADER_ERR
		resp.LeaderId = s.r.GetLeaderID()
		return resp, nil
	}

	s.mu.Lock()
	ch := s.getRespNotifyChannel(int64(lastLogIndex))
	s.mu.Unlock()

	select {
	case res := <-ch:
		resp.BlockContent = res.BlockContent
		resp.ErrCode = res.ErrCode
		resp.LeaderId = res.LeaderId
	case <-time.After(time.Second * 10):
		resp.ErrCode = pb.ErrCode_RPC_CALL_TIMEOUT_ERR
	}

	go func() {
		s.mu.Lock()
		delete(s.notifyChs, int64(lastLogIndex))
		s.mu.Unlock()
	}()

	return resp, nil
}

// getRespNotifyChannel
// @Description: 获取响应通道，响应通道中存放对file block的操作结果
func (s *BlockServer) getRespNotifyChannel(index int64) chan *pb.FileBlockOpResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.notifyChs[index]; !ok {
		s.notifyChs[index] = make(chan *pb.FileBlockOpResponse, 1)
	}
	return s.notifyChs[index]
}

func (s *BlockServer) restoreSnapshot(snapshot []byte) {
	if snapshot != nil {
		return
	}
	var buffer bytes.Buffer
	buffer.Write(snapshot)
	decoder := gob.NewDecoder(&buffer)
	var stm map[string]string
	if err := decoder.Decode(&stm); err != nil {
		log.Log.Debugf("block server decode snapshot error:%v", err)
	}
	jsonSTM, _ := json.Marshal(stm)
	log.Log.Debugf("block server restore snapshot:%s", jsonSTM)
	s.stm = stm
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

func (s *BlockServer) ApplyingToSTM(stopApplyCh <-chan interface{}) {
	for {
		select {
		case <-stopApplyCh:
			return
		case applyMsg := <-s.applyCh:
			if applyMsg.CommandValid {
				req := DecodeBlockServerRequest(applyMsg.Command)
				resp := &pb.FileBlockOpResponse{}
				switch req.OpType {
				case pb.FileBlockOpType_OP_BLOCK_READ:
					fileBlockPath := fmt.Sprintf("%s/%d_%d_%s_%d.wwd", s.dataPath, s.id, s.groupID, req.FileName, req.FileBlocksMeta.BlockId)
					fileBlock, err := os.ReadFile(fileBlockPath)
					if err != nil {
						resp.ErrCode = pb.ErrCode_READ_FILE_BLOCK_ERR
					}
					resp.BlockContent = fileBlock
				case pb.FileBlockOpType_OP_BLOCK_WRITE:
					fileBlockPath := fmt.Sprintf("%s/%d_%d_%s_%d.wwd", s.dataPath, s.id, s.groupID, req.FileName, req.FileBlocksMeta.BlockId)
					// create block file if not exist
					file, err := os.OpenFile(fileBlockPath, os.O_CREATE|os.O_RDWR, 0766)
					if err != nil {
						log.Log.Debugf("create block file failed:%v\n", err)
					}
					// write block content into block file
					_, err = file.Write(req.BlockContent)
					if err != nil {
						resp.ErrCode = pb.ErrCode_WRITE_FILE_BLOCK_ERR
					}
					file.Close()
				}
				s.lastApplied = int(applyMsg.CommandIndex)
				if s.r.GetLogCounts() > 20 {
					s.takeSnapshot(s.lastApplied)
				}
				s.mu.Lock()
				ch := s.getRespNotifyChannel(applyMsg.CommandIndex)
				s.mu.Unlock()
				ch <- resp
			} else if applyMsg.SnapshotValid {
				s.mu.Lock()
				if s.r.CondInstallSnapshot(int(applyMsg.SnapshotTerm), int(applyMsg.SnapshotIndex), applyMsg.Snapshot) {
					s.restoreSnapshot(applyMsg.Snapshot)
					s.lastApplied = int(applyMsg.SnapshotIndex)
				}
				s.mu.Unlock()
			}
		}
	}
}

func (s *BlockServer) StopApply() {
	close(s.stopApplyCh)
}
