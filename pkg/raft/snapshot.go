package raft

import (
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

//
// Snapshot
// @Description: 生成快照
//
func (r *Raft) Snapshot(index int, snapshot []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.isSnapshotting = true
	firstIndex := r.logs.GetFirstEntry().GetIndex()
	if index <= int(firstIndex) {
		r.isSnapshotting = false
		log.Log.Debugf("reject snapshot,current snapshot index is larger than the first index of log")
		return
	}
	log.Log.Debugf("take snapshot at index %d", index)
	_, _ = r.logs.EraseBefore(int64(index), true)
	r.isSnapshotting = false
	r.logs.PersistSnapshot(snapshot)
}

//
// HandleInstallSnapshot
// @Description: 处理leader发来的安装快照请求
//
func (r *Raft) HandleInstallSnapshot(req *pb.InstallSnapshotReq, resp *pb.InstallSnapshotResp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	resp.Term = r.currTerm
	if req.GetTerm() < r.currTerm {
		return
	} else if req.GetTerm() > r.currTerm {
		r.currTerm = req.GetTerm()
		r.voteFor = None
		r.PersistState()
	}

	r.SwitchRole(FOLLOWER)
	r.electionTimer.Reset(RandomElectionTimeout(r.electionTimeout))
	if req.LastIncludedIndex <= r.commitIdx {
		return
	}

	go func() {
		r.applyCh <- &pb.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      req.Data,
			SnapshotTerm:  req.LastIncludedTerm,
			SnapshotIndex: req.LastIncludedIndex,
		}
	}()
}

//
// CondInstallSnapshot
// @Description: 判断是否需要安装快照
//
func (r *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.Log.Debugf("peer-%d-cond install snapshot\n", r.me)

	if lastIncludedIndex <= int(r.commitIdx) {
		return false
	}

	if lastIncludedIndex > int(r.logs.lastIdx) {
		log.Log.Debugf("reinit logs and install snapshot")
		_ = r.logs.ReInitLogs()
	} else {
		log.Log.Debugf("install snapshot and delete old logs")
		_, _ = r.logs.EraseBefore(int64(lastIncludedIndex), true)
	}

	err := r.logs.SetEntryFirstIndexAndTerm(int64(lastIncludedIndex), int64(lastIncludedTerm))
	if err != nil {
		return false
	}

	r.lastApplied = int64(lastIncludedIndex)
	r.commitIdx = int64(lastIncludedIndex)

	return true
}

func (r *Raft) ReadSnapshot() []byte {
	snapshot, err := r.logs.ReadSnapshot()
	if err != nil {
		log.Log.Debugf("read snapshot error:%v", err)
	}
	return snapshot
}
