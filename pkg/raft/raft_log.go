package raft

import (
	"rstorage/pkg/engine"
	pb "rstorage/pkg/protocol"
	"sync"
)

type RaftLog struct {
	mu       sync.RWMutex
	firstIdx uint64
	lastIdx  uint64
	items    []*pb.Entry
	db       engine.KvStore
}

func BuildRaftLog() *RaftLog {
	entry := &pb.Entry{}
	var items []*pb.Entry
	items = append(items, entry)
	return &RaftLog{
		firstIdx: 0,
		lastIdx:  1,
		items:    items,
	}
}

func (l *RaftLog) GetMemFirst() *pb.Entry {
	return l.items[0]
}

func (l *RaftLog) GetMemLast() *pb.Entry {
	return l.items[len(l.items)-1]
}

func (l *RaftLog) AppendMemEntry(entry *pb.Entry) {
	l.items = append(l.items, entry)
}

func (l *RaftLog) GetMemEntry(index int) *pb.Entry {
	return l.items[index]
}

// GetMemBeforeIdx
// @Description: è·å–indexåŠä¹‹å‰çš„entry
//
func (l *RaftLog) GetMemBeforeIdx(index int) []*pb.Entry {
	return l.items[:index+1]
}

func (l *RaftLog) GetMemAfterIdx(index int) []*pb.Entry {
	return l.items[index:]
}

func (l *RaftLog) GetMemRangeEntries(first, last int) []*pb.Entry {
	return l.items[first : last+1]
}

// DelMemBeforeIdx
// @Description: å°†indexåŠä¹‹å‰çš„entryåˆ æ‰
//
func (l *RaftLog) DelMemBeforeIdx(index int) []*pb.Entry {
	l.items = l.items[:index+1]
	return l.items
}

func (l *RaftLog) DelMemAfterIdx(index int) []*pb.Entry {
	l.items = l.items[index:]
	return l.items
}

func (l *RaftLog) LogCounts() int {
	//todo ¼ÆËãlogÊıÁ¿
	return 0
}
