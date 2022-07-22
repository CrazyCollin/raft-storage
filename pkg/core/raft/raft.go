package raft

import (
	"CrazyCollin/personalProjects/raft-db/pkg/protocol"
	"sync"
	"time"
)

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
	mu sync.RWMutex
	//rpc客户端
	peers *[]RaftClientEnd
	//自己的id
	me int
	//节点状态
	dead int32
	//apply协程通道
	applyCh chan *protocol.ApplyMsg
	//apply流程控制的信号量
	applyCond *sync.Cond
	//复制操作控制的信号量
	replicatorCond []*sync.Cond
	//节点当前状态
	role RAFTROLE

	//当前任期
	currTerm int64
	//为哪个节点投票
	voteFor int64
	//已获得票数
	grantedVotes int
	//日志信息
	logs *RaftLog
	//日志持久化
	persister *RaftLog

	//已经提交的最大日志id
	commitIdx int64
	//已经applied的最大日志id
	lastApplied int64
	//leader节点到其他节点下一个匹配的日志id信息
	nextIdx []int
	//leader节点到其他节点当前匹配的日志id信息
	matchIdx       []int
	isSnapshotting bool

	//集群当前leader的id
	leaderId int64
	//选举超时计时器
	electionTimer *time.Timer
	//心跳超时定时器
	heartbeatTimer *time.Timer
	//心跳超时时间
	heartbeatTimeout uint64
	//选举超时时间
	electionTimeout uint64
}
