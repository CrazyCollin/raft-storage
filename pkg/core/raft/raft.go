package raft

import (
	"rstorage/pkg/engine"
	"rstorage/pkg/protocol"
	"sync"
	"sync/atomic"
	"time"
)

type ROLE uint8

const None int64 = -1

const (
	FOLLOWER ROLE = iota
	CANDIDATE
	LEADER
)

func RoleToString(role ROLE) string {
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
	peers []*RaftClientEnd
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
	role ROLE

	//持久性状态
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

	//所有服务器上的易失性状态
	//已经提交的最大日志id
	commitIdx int64
	//已经applied的最大日志id
	lastApplied int64

	//leader上的易失性状态
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

func BuildRaft(peers []*RaftClientEnd, me int, dbEngine engine.KvStore, applyCh chan *protocol.ApplyMsg, heartbeatTimeout uint64, electionTimeout uint64) *Raft {
	raft := &Raft{
		peers:            peers,
		me:               me,
		dead:             0,
		applyCh:          applyCh,
		replicatorCond:   make([]*sync.Cond, len(peers)),
		role:             FOLLOWER,
		currTerm:         0,
		voteFor:          None,
		grantedVotes:     0,
		logs:             BuildPersistentRaftLog(dbEngine),
		persister:        BuildPersistentRaftLog(dbEngine),
		commitIdx:        0,
		lastApplied:      0,
		nextIdx:          make([]int, len(peers)),
		matchIdx:         make([]int, len(peers)),
		heartbeatTimer:   nil,
		electionTimer:    nil,
		heartbeatTimeout: heartbeatTimeout,
		electionTimeout:  electionTimeout,
	}
	raft.applyCond = sync.NewCond(&raft.mu)
	go raft.Ticker()
	return raft
}

//
// Ticker
// @Description: 处理选举超时和心跳超时
// @receiver r
//
func (r *Raft) Ticker() {
	for !r.IsDead() {
		select {
		case <-r.electionTimer.C:
			r.mu.Lock()
			//todo 开始选举 follower->candidate
			r.IncrCurrTerm()
			r.SwitchRole(CANDIDATE)
			r.StartElection()
			r.mu.Unlock()
		case <-r.heartbeatTimer.C:
			if r.role == LEADER {
				r.SendHeartbeat()
			}
		}
	}
}

func (r *Raft) StartElection() {
	//todo 开始新一轮选举
}

func (r *Raft) SendHeartbeat() {
	for _, peer := range r.peers {
		if int(peer.id) == r.me {
			continue
		}
		//todo 保持心跳
	}
}

func (r *Raft) IsDead() bool {
	return atomic.LoadInt32(&r.dead) == 1
}

// SwitchRole
// @Description: 改变当前节点角色
// @receiver r
// @param role
//
func (r *Raft) SwitchRole(role ROLE) {
	if r.role == role {
		return
	}
	r.role = role
	switch role {
	case FOLLOWER:
	case CANDIDATE:
	case LEADER:
	}
}

func (r *Raft) IncrCurrTerm() {
	atomic.AddInt64(&r.currTerm, 1)
}
