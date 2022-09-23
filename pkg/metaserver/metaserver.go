package metaserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"rstorage/pkg/common"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
	"rstorage/pkg/raft"
	"strings"
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
// @Description: 处理server group meta
//
func (s *MetaServer) ServerGroupMeta(ctx context.Context, req *pb.ServerGroupMetaConfigRequest) (*pb.ServerGroupMetaConfigResponse, error) {
	log.Log.Debugf("receive server group meta request:%v\n", req)
	resp := &pb.ServerGroupMetaConfigResponse{}
	encodedRequestBytes := EncodeServerGroupMetaRequest(req)
	lastLogIndex, _, isLeader := s.r.Propose(encodedRequestBytes)

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

//
// ApplyToSTM
// @Description: apply协程，处理apply消息
//
func (s *MetaServer) ApplyToSTM(stopApplyCh <-chan interface{}) {
	for {
		//等待apply消息
		select {
		case <-stopApplyCh:
			return
		case applyMsg := <-s.applyCh:
			if applyMsg.CommandValid {
				req := DecodeServerGroupMetaRequest(applyMsg.Command)
				resp := &pb.ServerGroupMetaConfigResponse{}

				//处理server group meta请求
				switch req.OpType {
				case pb.ConfigServerGroupMetaOpType_OP_SERVER_GROUP_JOIN:
					var serverGroups map[int][]string
					for i, group := range req.ServerGroups {
						serverGroups[int(i)] = strings.Split(group, ",")
					}
					err := s.stm.Join(serverGroups)
					if err != nil {
						resp.ErrCode = pb.ErrCode_APPLY_JOIN_TODO_TO_STM_ERR
					}
				case pb.ConfigServerGroupMetaOpType_OP_SERVER_GROUP_QUERY:
					versionID := req.ConfigVersion
					metaConfig, err := s.stm.Query(int(versionID))
					if err != nil {
						resp.ErrCode = pb.ErrCode_APPLY_QUERY_TOPO_CONF_ERR
					}

					resp.ServerGroupMetas = &pb.ServerGroupMetas{
						ConfigVersion: int64(metaConfig.Version),
						ServerGroups:  make(map[int64]string),
					}
					//add server group
					for i, group := range metaConfig.ServerGroups {
						resp.ServerGroupMetas.ServerGroups[int64(i)] = strings.Join(group, ",")
					}
					//add server slot
					for _, slot := range metaConfig.Slots {
						resp.ServerGroupMetas.Slots = append(resp.ServerGroupMetas.Slots, int64(slot))
					}
				case pb.ConfigServerGroupMetaOpType_OP_SERVER_GROUP_LEAVE:
					var groupIDs []int
					for _, gid := range req.Gids {
						groupIDs = append(groupIDs, int(gid))
					}
					err := s.stm.Leave(groupIDs)
					if err != nil {
						resp.ErrCode = pb.ErrCode_APPLY_LEAVE_TODO_TO_STM_ERR
					}
				case pb.ConfigServerGroupMetaOpType_OP_OSS_BUCKET_ADD:
					bucketID := common.GenUUID()
					bucket := &pb.Bucket{
						BucketId:   bucketID,
						BucketName: req.BucketOpReq.BucketName,
					}
					encodedBucketKeyBytes := EncodeBucketKey(bucketID)
					encodedBucketValueBytes := EncodeBucket(bucket)
					err := s.db.Put(encodedBucketKeyBytes, encodedBucketValueBytes)
					if err != nil {
						resp.ErrCode = pb.ErrCode_PUT_BUCKET_TO_ENG_ERR
					}
					resp.ErrCode = pb.ErrCode_NO_ERR
				case pb.ConfigServerGroupMetaOpType_OP_OSS_BUCKET_DEL:
					bucketIDKey := req.BucketOpReq.BucketId
					encodedBucketKeyBytes := EncodeBucketKey(bucketIDKey)
					err := s.db.Del(encodedBucketKeyBytes)
					if err != nil {
						resp.ErrCode = pb.ErrCode_DEL_BUCKET_FROM_ENG_ERR
					}
					resp.ErrCode = pb.ErrCode_NO_ERR
				case pb.ConfigServerGroupMetaOpType_OP_OSS_BUCKET_LIST:
					var bucketList []*pb.Bucket
					_, bucketData, err := s.db.GetPrefixRangeKvs(common.BUCKET_META_PREFIX)
					if err != nil {
						log.Log.Errorf("get bucket list from db error:%v\n", err)
						return
					}
					resp.BucketOpRes = &pb.BucketOpResponse{}
					log.Log.Debugf("start to decode bucket data:%v\n", bucketData)
					for _, bucket := range bucketData {
						bucketList = append(bucketList, DecodeBucket([]byte(bucket)))
					}
					resp.BucketOpRes.Buckets = bucketList
					resp.ErrCode = pb.ErrCode_NO_ERR
				case pb.ConfigServerGroupMetaOpType_OP_OSS_OBJECT_GET:
					//todo

				case pb.ConfigServerGroupMetaOpType_OP_OSS_OBJECT_PUT:
					objectID := common.GenUUID()
					object := &pb.Object{
						ObjectId:         objectID,
						ObjectName:       req.BucketOpReq.BucketName,
						FromBucketId:     req.BucketOpReq.BucketId,
						ObjectBlocksMeta: req.BucketOpReq.Object.ObjectBlocksMeta,
					}
					encodedObjectKeyBytes := EncodeObjectKey(objectID)
					encodedObjectValueBytes := EncodeObject(object)
					err := s.db.Put(encodedObjectKeyBytes, encodedObjectValueBytes)
					if err != nil {
						resp.ErrCode = pb.ErrCode_PUT_OBJECT_META_TO_ENG_ERR
					}
					resp.ErrCode = pb.ErrCode_NO_ERR
				case pb.ConfigServerGroupMetaOpType_OP_OSS_OBJECT_LIST:
					var objectList []*pb.Object
					_, objectData, err := s.db.GetPrefixRangeKvs(common.OBJECT_META_PREFIX)
					if err != nil {
						log.Log.Errorf("get object list from db error:%v\n", err)
						return
					}
					resp.BucketOpRes = &pb.BucketOpResponse{}
					log.Log.Debugf("start to decode object data:%v\n", objectData)
					for _, object := range objectData {
						decodedObject := DecodeObject([]byte(object))
						if decodedObject.FromBucketId == req.BucketOpReq.BucketId {
							objectList = append(objectList, DecodeObject([]byte(object)))
						}
					}

					resp.BucketOpRes.Objects = objectList
					resp.ErrCode = pb.ErrCode_NO_ERR
				}

				s.lastApplied = int(applyMsg.CommandIndex)
				//when raft logs are too large, take a snapshot
				if s.r.GetLogCounts() > 20 {
					s.takeSnapshot(s.lastApplied)
				}
				//notify the response channel
				s.mu.Lock()
				ch := s.getRespNotifyChannel(applyMsg.GetCommandIndex())
				s.mu.Unlock()
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
