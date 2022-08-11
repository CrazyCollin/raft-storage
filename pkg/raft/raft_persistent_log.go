package raft

import (
	"rstorage/pkg/common"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
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
	entry := &pb.Entry{}
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
func (l *RaftLog) AppendEntry(entry *pb.Entry) {
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
func (l *RaftLog) GetFirstEntry() *pb.Entry {
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

func (l *RaftLog) GetLastEntry() *pb.Entry {
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

//
// GetEntry
// @Description: 获取指定entry
//
func (l *RaftLog) getEntry(index int64) *pb.Entry {
	encodedEntry, err := l.db.Get(EncodeRaftLogKey(uint64(index)))
	if err != nil {
		log.Log.Debugf("get specific entry error:%v", err)
		panic(err)
	}
	entry := DecodeEntry(encodedEntry)
	return entry
}

func (l *RaftLog) GetEntry(index int64) *pb.Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getEntry(index)
}

func (l *RaftLog) GetRangeEntries(firstIdx, lastIdx int64) []*pb.Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	entries := make([]*pb.Entry, lastIdx-firstIdx+1)
	for i := firstIdx; i < lastIdx; i++ {
		entries[i] = l.getEntry(i)
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
	//logKey, _, err := l.db.SeekPrefixFirst(common.RAFTLOG_PREFIX)
	//if err != nil {
	//	panic(err)
	//}
	//firstLogID := DecodeRaftLogKey(logKey)
	//return firstLogID
	return l.firstIdx
}

func (l *RaftLog) GetLastLogID() uint64 {
	//lastLogID, err := l.db.SeekPrefixKeyIdMax(common.RAFTLOG_PREFIX)
	//if err != nil {
	//	panic(err)
	//}
	//return lastLogID
	return l.lastIdx
}

func (l *RaftLog) EraseBefore(idx int64) []*pb.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var entries []*pb.Entry
	lastLogId := l.GetLastLogID()
	log.Log.Debugf("get log [%d:%d] ", idx, lastLogId)
	for i := idx; i <= int64(lastLogId); i++ {
		entries = append(entries, l.GetEntry(i))
	}
	return entries
}
