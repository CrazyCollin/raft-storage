package raft

import (
	"rstorage/pkg/log"
	"rstorage/pkg/protocol"
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
// @Description: 为日志添加新条目
//
func (r *Raft) AppendLogEntry(payload []byte) *protocol.Entry {
	lastLogEntry := r.logs.GetLastEntry()
	newEntry := &protocol.Entry{
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
// @Description: 用于广播append信息或者是心跳保持
//
func (r *Raft) Broadcast(isHeartbeat bool) {
	for _, peer := range r.peers {
		if int(peer.id) == r.me {
			continue
		}
		if isHeartbeat {
			log.Log.Debugf("leader-%d-send heartbeat to follower-%d-", r.me, peer.id)
			//todo 通知发送心跳
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
// @Description: 复制相关
//
func (r *Raft) Replicator(peer *RaftClientEnd) {
	r.replicatorCond[peer.Id()].L.Lock()
	defer r.replicatorCond[peer.Id()].L.Unlock()
	//todo 处理日志复制相关
	for !r.IsDead() {

	}
}

//
// replicateOneRound
// @Description: 从leader复制log至follower，用于心跳检测和log replication
//
func (r *Raft) replicateOneRound(peer *RaftClientEnd) {
	//todo 单次复制
}
