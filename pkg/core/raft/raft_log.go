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
	items    *[]protocol.Entry
	db       engine.KvStore
}
