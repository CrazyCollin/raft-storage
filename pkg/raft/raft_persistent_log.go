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
//
func BuildPersistentRaftLog(dbEngine engine.KvStore) *RaftLog {
	entry := &pb.Entry{}
	encodedEntry := EncodeEntry(entry)
	dbEngine.Put(EncodeRaftLogKey(common.INIT_LOG_INDEX), encodedEntry)
	return &RaftLog{db: dbEngine}
}

//
// ReInitLogs
// @Description: 初始化日志
//
func (l *RaftLog) ReInitLogs() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.db.DelPrefixKeys(common.RAFTLOG_PREFIX); err != nil {
		return err
	}
	emptyEntry := &pb.Entry{}
	emptyEntryEncodeBytes := EncodeEntry(emptyEntry)
	l.firstIdx, l.lastIdx = 0, 0
	return l.db.Put(EncodeRaftLogKey(common.INIT_LOG_INDEX), emptyEntryEncodeBytes)
}

//
// AppendEntry
// @Description: 添加日志条目
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

//
// GetRangeEntries
// @Description: [firstIdx,lastIdx)
//
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
//
func (l *RaftLog) PersistStateOfRaftLog(currTerm, votedFor int64) {
	state := &StateOfRaftLog{
		currTerm: currTerm,
		votedFor: votedFor,
	}
	_ = l.db.Put(common.RAFT_STATE_KEY, EncodeRaftState(state))
}

func (l *RaftLog) PersistSnapshot(snapshot []byte) {
	l.db.Put(common.RAFT_SNAPSHOT_KEY, snapshot)
}

//
// ReadStateOfRaftLog
// @Description: 读取当前raft持久化状态
//
func (l *RaftLog) ReadStateOfRaftLog() (int64, int64) {
	stateBytes, err := l.db.Get(common.RAFT_STATE_KEY)
	if err != nil {
		return 0, -1
	}
	state := DecodeRaftState(stateBytes)
	return state.currTerm, state.votedFor
}

func (l *RaftLog) GetFirstLogID() uint64 {
	return l.firstIdx
}

func (l *RaftLog) GetLastLogID() uint64 {
	return l.lastIdx
}

func (l *RaftLog) EraseAfterIdx(idx int64) []*pb.Entry {
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

func (l *RaftLog) EraseAfter(idx int64, del bool) []*pb.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	firstLogId := l.GetFirstLogID()
	log.Log.Debugf("start erase after log index:%d\n", idx)
	if del {
		for i := idx; i <= int64(l.GetLastLogID()); i++ {
			if err := l.db.Del(EncodeRaftLogKey(uint64(i))); err != nil {
				panic(err)
			}
		}
		l.lastIdx = uint64(idx) - 1
	}
	var entries []*pb.Entry
	for i := firstLogId; i < uint64(idx); i++ {
		entries = append(entries, l.GetEntry(int64(i)))
	}
	return entries
}

//
// EraseBefore
// @Description: 删除指定索引之前的日志
//
func (l *RaftLog) EraseBefore(idx int64, del bool) ([]*pb.Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if del {
		firstLogId := l.GetFirstLogID()
		for i := firstLogId; i < uint64(idx); i++ {
			if err := l.db.Del(EncodeRaftLogKey(i)); err != nil {
				log.Log.Debugf("erase error\n")
				return nil, err
			}
			log.Log.Debugf("del log with index %d success", i)
		}
		l.firstIdx = uint64(idx)
		log.Log.Debugf("after erase log,first log index:%d", l.firstIdx)
		return nil, nil
	}
	var entries []*pb.Entry
	lastLogId := l.GetLastLogID()
	for i := idx; i <= int64(lastLogId); i++ {
		entries = append(entries, l.GetEntry(i))
	}
	return entries, nil
}

func (l *RaftLog) SetEntryFirstIndexAndTerm(index, term int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	firstIdx := l.GetFirstLogID()

	encodedBytes, err := l.db.Get(EncodeRaftLogKey(firstIdx))
	if err != nil {
		log.Log.Debugf("get first log error:%v\n", err)
		return err
	}

	//删除首个entry
	log.Log.Debugf("delete first log with index %d\n", firstIdx)
	if err := l.db.Del(EncodeRaftLogKey(firstIdx)); err != nil {
		log.Log.Debugf("delete first log error:%v\n", err)
		return err
	}
	//更新首个entry的index和term
	entry := DecodeEntry(encodedBytes)
	entry.Index = index
	entry.Term = uint64(term)
	log.Log.Debugf("set first log index:%d,term:%d\n", index, term)

	l.firstIdx = uint64(index)
	l.lastIdx = uint64(index)
	//重新写入首个entry
	if err := l.db.Put(EncodeRaftLogKey(uint64(index)), EncodeEntry(entry)); err != nil {
		log.Log.Debugf("set first log error:%v\n", err)
		return err
	}
	return nil
}
