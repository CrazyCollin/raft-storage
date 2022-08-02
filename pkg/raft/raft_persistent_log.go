package raft

import (
	"rstorage/pkg/common"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
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
// AppendEntry
// @Description: 添加日志条目
// @receiver l
// @param entry
//
func (l *RaftLog) AppendEntry(entry *protocol.Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lastLogID, err := l.db.SeekPrefixKeyIdMax(common.RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	encodedEntry := EncodeEntry(entry)
	l.db.Put(EncodeRaftLogKey(lastLogID+1), encodedEntry)
}

//
// GetFirstEntry
// @Description: 获取日志中首个日志条目
// @receiver l
// @return *protocol.Entry
//
func (l *RaftLog) GetFirstEntry() *protocol.Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	logKey, logValue, err := l.db.SeekPrefixFirst(common.RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	firstLogID := DecodeRaftLogKey(logKey)
	log.Log.Debugf("get first entry with id:%d", firstLogID)
	firstEntry := DecodeEntry(logValue)
	return firstEntry
}

func (l *RaftLog) GetLastEntry() *protocol.Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	logKey, logValue, err := l.db.SeekPrefixLast(common.RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	lastLogID := DecodeRaftLogKey(logKey)
	log.Log.Debugf("get last entry with id:%d", lastLogID)
	lastEntry := DecodeEntry(logValue)
	return lastEntry
}

func (l *RaftLog) GetEntry(index int) *protocol.Entry {
	firstLogID := l.GetFirstLogID()
	encodedEntry, err := l.db.Get(EncodeRaftLogKey(firstLogID + uint64(index)))
	if err != nil {
		log.Log.Debugf("get specific entry error:%v", err)
		panic(err)
	}
	entry := DecodeEntry(encodedEntry)
	return entry
}

func (l *RaftLog) GetRangeEntries(firstIdx, lastIdx int) []*protocol.Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	entries := make([]*protocol.Entry, lastIdx-firstIdx+1)
	for i := firstIdx; i < lastIdx; i++ {
		entries[i] = l.GetEntry(i)
	}
	return entries
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
	_ = l.db.Put(common.RAFT_STATE_KEY, EncodeRaftState(state))
}

func (l *RaftLog) GetFirstLogID() uint64 {
	logKey, _, err := l.db.SeekPrefixFirst(common.RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	firstLogID := DecodeRaftLogKey(logKey)
	return firstLogID
}

func (l *RaftLog) GetLastLogID() uint64 {
	lastLogID, err := l.db.SeekPrefixKeyIdMax(common.RAFTLOG_PREFIX)
	if err != nil {
		panic(err)
	}
	return lastLogID
}
