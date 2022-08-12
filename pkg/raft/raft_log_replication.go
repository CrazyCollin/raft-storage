package raft

import (
	"context"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

//
// Propose
// @Description: 处理
//
func (r *Raft) Propose(payload []byte) (logIndex int, term int, isAccepted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	//todo 待完善重定向至leader节点
	if r.role != LEADER {
		return -1, -1, false
	}
	newEntry := r.AppendLogEntry(payload)
	r.Broadcast(false)
	return int(newEntry.GetIndex()), int(newEntry.GetTerm()), true
}

//
// AppendLogEntry
// @Description: 本地追加entry
//
func (r *Raft) AppendLogEntry(payload []byte) *pb.Entry {
	lastLogEntry := r.logs.GetLastEntry()
	newEntry := &pb.Entry{
		Index: lastLogEntry.GetIndex() + 1,
		Term:  uint64(r.currTerm),
		Data:  payload,
	}
	//更新日志条目index
	r.matchIdx[r.me] = int(newEntry.GetIndex())
	r.nextIdx[r.me] = int(newEntry.GetIndex()) + 1
	r.logs.AppendEntry(newEntry)
	r.PersistState()
	return newEntry
}

//
// Broadcast
// @Description: 用于广播append信息(false)或者是心跳保持(true)
//
func (r *Raft) Broadcast(isHeartbeat bool) {
	for _, peer := range r.peers {
		if int(peer.id) == r.me {
			continue
		}
		if isHeartbeat {
			log.Log.Debugf("leader-%d-send heartbeat to follower-%d-", r.me, peer.id)
			r.replicateOneRound(peer)
		} else {
			//通知协程开始复制
			log.Log.Debugf("leader-%d-send heartbeat to follower-%d-", r.me, peer.id)
			r.replicatorCond[peer.Id()].Signal()
		}
	}
}

//
// Replicator
// @Description: log replication的协程
//
func (r *Raft) Replicator(peer *RaftClientEnd) {
	r.replicatorCond[peer.Id()].L.Lock()
	defer r.replicatorCond[peer.Id()].L.Unlock()
	for !r.IsDead() {
		log.Log.Debugf("leader-%d-send heartbeat to follower-%d-", r.me, peer.id)
		//判断是否需要日志同步，不需要阻塞，需要则持续复制
		for !(r.role == LEADER && r.matchIdx[peer.Id()] < int(r.logs.lastIdx)) {
			//阻塞复制
			r.replicatorCond[peer.Id()].Wait()
		}
		r.replicateOneRound(peer)
	}
}

//
// replicateOneRound
// @Description: 从leader复制log至follower，用于心跳检测和log replication
//
func (r *Raft) replicateOneRound(peer *RaftClientEnd) {
	//todo 复制
	r.mu.RLock()
	//角色不是leader，返回
	if r.role != LEADER {
		r.mu.RUnlock()
		return
	}
	//peer节点已经同步的最后一个日志的index
	prevLogIndex := uint64(r.nextIdx[peer.Id()] - 1)
	if prevLogIndex < r.logs.GetFirstLogID() {
		//todo 发送快照
	} else {
		//构造请求
		log.Log.Debugf("leader-%d-start send append entries", r.me)
		appendEntriesReq := r.InitAppendEntriesReq(prevLogIndex)
		r.mu.RUnlock()
		//发送请求
		appendEntriesResp, err := (*peer.raftServiceCli).AppendEntries(context.Background(), appendEntriesReq)
		if err != nil {
			log.Log.Errorf("leader-%d-send append entries failed to peer-%d-", r.me, peer.id)
		}
		if r.role == LEADER && appendEntriesResp != nil {
			if appendEntriesResp.Success {
				//追加成功
				log.Log.Errorf("peer-%d-received heartbeat success", peer.id)
				//更新leader上的易失性状态
				//追加成功，匹配完全
				r.matchIdx[peer.Id()] = int(appendEntriesReq.PrevLogIndex) + len(appendEntriesReq.Entries)
				r.nextIdx[peer.Id()] = r.matchIdx[peer.Id()] + 1
				r.ManageLeaderCommitIndex()
			} else {
				if appendEntriesResp.Term > appendEntriesReq.Term {
					r.SwitchRole(FOLLOWER)
					r.currTerm = appendEntriesResp.GetTerm()
					r.voteFor = None
					r.PersistState()
				} else {
					//todo 日志冲突，待追加同步
					//重置冲突peer的nextIndex
					r.nextIdx[peer.Id()] = int(appendEntriesResp.ConflictIndex)

				}
			}
		}

	}
}

//
// InitAppendEntriesReq
// @Description: 构造append entries请求
//
func (r *Raft) InitAppendEntriesReq(prevLogIndex uint64) *pb.AppendEntriesReq {
	//获取entries
	entries := make([]*pb.Entry, r.logs.GetLastLogID()-prevLogIndex)
	copy(entries, r.logs.EraseBefore(int64(prevLogIndex+1)))
	appendEntriesReq := &pb.AppendEntriesReq{
		Term:         r.currTerm,
		LeaderId:     int64(r.me),
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  int64(r.logs.GetEntry(int64(prevLogIndex)).GetTerm()),
		LeaderCommit: r.commitIdx,
		Entries:      entries,
	}
	return appendEntriesReq
}

func (r *Raft) HandleAppendEntries(req *pb.AppendEntriesReq, resp *pb.AppendEntriesResp) {
	//todo 处理append请求&处理心跳
	r.mu.Lock()
	defer r.mu.Unlock()

	//req term较小
	if req.GetTerm() < r.currTerm {
		resp.Term = r.currTerm
		resp.Success = false
		return
	}
	//req term较大
	if req.GetTerm() > r.currTerm {
		r.currTerm = req.GetTerm()
		r.voteFor = None
	}

	r.SwitchRole(FOLLOWER)
	//todo 接收到（心跳）消息，重置选举计时器
	r.electionTimer.Reset(0)
	r.leaderId = req.LeaderId

	//leader log
	if req.PrevLogIndex < int64(r.logs.firstIdx) {
		resp.Term = 0
		resp.Success = false
		log.Log.Debugf("node-%d-received wrong log index which req's prev log index is small than current first log index", r.me)
		return
	}
	if !r.MatchLog(req.PrevLogIndex, req.PrevLogTerm) {
		//todo 处理log mismatch
		return
	}
	//todo log match的情况
}

//
// ManageLeaderCommitIndex
// @Description: 根据复制进度处理提交
//
func (r *Raft) ManageLeaderCommitIndex() {
	//todo 处理复制进度，准备提交
}

//
// ManageFollowerCommitIndex
// @Description: 处理follower的commit
//
func (r *Raft) ManageFollowerCommitIndex() {
	//todo 处理follower提交
}

func (r *Raft) MatchLog(index, term int64) bool {
	return index >= int64(r.logs.firstIdx) && index <= int64(r.logs.lastIdx) && uint64(term) == r.logs.GetEntry(index).GetTerm()
}