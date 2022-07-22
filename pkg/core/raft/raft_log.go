package raft

import (
	"CrazyCollin/personalProjects/raft-db/pkg/engine"
	"CrazyCollin/personalProjects/raft-db/pkg/protocol"
	"sync"
)

type RaftLog struct {
	mu       sync.RWMutex
	firstIdx uint64
	lastIdx  uint64
	items    *[]protocol.Entry
	db       engine.KvStore
}
