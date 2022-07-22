package raft

import "sync"

type RAFTROLE uint8

const (
	FOLLOWER RAFTROLE = iota
	CANDIDATE
	LEADER
)

func RoleToString(role RAFTROLE) string {
	switch role {
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	case LEADER:
		return "Leader"
	}
	return "error"
}

type Raft struct {
	mu    *sync.RWMutex
	peers *[]RaftClientEnd
}
