package raft

import (
	"rstorage/pkg/common"
	"rstorage/pkg/engine"
	"rstorage/pkg/protocol"
)

type StateOfRaftLog struct {
	currTerm int64
	votedFor int64
}

//
// BuildPersistentRaftLog
// @Description: 初始化持久性raft log
// @param dbEngine
// @return *RaftLog
//
func BuildPersistentRaftLog(dbEngine engine.KvStore) *RaftLog {
	entry := &protocol.Entry{}
	encodedEntry := EncodeEntry(entry)
	dbEngine.Put(EncodeRaftLogKey(common.INIT_LOG_INDEX), encodedEntry)
	return &RaftLog{db: dbEngine}
}

//
// PersistStateOfRaftLog
// @Description: 持久化当前raft状态
// @receiver l
// @param currTerm
// @param votedFor
//
func (l *RaftLog) PersistStateOfRaftLog(currTerm, votedFor int64) {
	state := &StateOfRaftLog{
		currTerm: currTerm,
		votedFor: votedFor,
	}
	l.db.Put(common.RAFT_STATE_KEY, EncodeRaftState(state))
}
