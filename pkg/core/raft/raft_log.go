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

func (l *RaftLog) GetMemFirst() *protocol.Entry {
	return l.items[0]
}

func (l *RaftLog) GetMemLast() *protocol.Entry {
	return l.items[len(l.items)-1]
}

func (l *RaftLog) AppendMemEntry(entry *protocol.Entry) {
	l.items = append(l.items, entry)
}

func (l *RaftLog) GetMemEntry(index int) *protocol.Entry {
	return l.items[index]
}

// GetMemBeforeIdx
// @Description: 获取index及之前的entry
// @receiver l
// @param index
// @return []*protocol.Entry
//
func (l *RaftLog) GetMemBeforeIdx(index int) []*protocol.Entry {
	return l.items[:index+1]
}

func (l *RaftLog) GetMemAfterIdx(index int) []*protocol.Entry {
	return l.items[index:]
}

func (l *RaftLog) GetMemRangeEntries(first, last int) []*protocol.Entry {
	return l.items[first : last+1]
}

// DelMemBeforeIdx
// @Description: 将index及之前的entry删掉
// @receiver l
// @param index
// @return []*protocol.Entry
//
func (l *RaftLog) DelMemBeforeIdx(index int) []*protocol.Entry {
	l.items = l.items[:index+1]
	return l.items
}

func (l *RaftLog) DelMemAfterIdx(index int) []*protocol.Entry {
	l.items = l.items[index:]
	return l.items
}
