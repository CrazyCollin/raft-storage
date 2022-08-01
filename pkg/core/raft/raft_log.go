package raft

import (
	"rstorage/pkg/engine"
	"rstorage/pkg/protocol"
	"sync"
)

type RaftLog struct {
	mu       sync.RWMutex
	firstIdx uint64
	lastIdx  uint64
	items    []*protocol.Entry
	db       engine.KvStore
}

func BuildRaftLog() *RaftLog {
	entry := &protocol.Entry{}
	var items []*protocol.Entry
	items = append(items, entry)
	return &RaftLog{
		firstIdx: 0,
		lastIdx:  1,
		items:    items,
	}
}

func (l *RaftLog) GetFirst() *protocol.Entry {
	return l.items[0]
}

func (l *RaftLog) GetLast() *protocol.Entry {
	return l.items[len(l.items)-1]
}

func (l *RaftLog) AppendEntry(entry *protocol.Entry) {
	l.items = append(l.items, entry)
}

func (l *RaftLog) GetEntry(index int) *protocol.Entry {
	return l.items[index]
}

//
// GetBeforeIdx
// @Description: 获取index及之前的entry
// @receiver l
// @param index
// @return []*protocol.Entry
//
func (l *RaftLog) GetBeforeIdx(index int) []*protocol.Entry {
	return l.items[:index+1]
}

func (l *RaftLog) GetAfterIdx(index int) []*protocol.Entry {
	return l.items[index:]
}

func (l *RaftLog) GetRangeEntries(first, last int) []*protocol.Entry {
	return l.items[first : last+1]
}

//
// DelBeforeIdx
// @Description: 将index及之前的entry删掉
// @receiver l
// @param index
// @return []*protocol.Entry
//
func (l *RaftLog) DelBeforeIdx(index int) []*protocol.Entry {
	l.items = l.items[:index+1]
	return l.items
}

func (l *RaftLog) DelAfterIdx(index int) []*protocol.Entry {
	l.items = l.items[index:]
	return l.items
}
