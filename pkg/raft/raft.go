package raft

import (
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
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
	applyCh chan *pb.ApplyMsg
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
	//leader节点到其他节点下一个匹配的日志index信息
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

func BuildRaft(peers []*RaftClientEnd, me int, dbEngine engine.KvStore, applyCh chan *pb.ApplyMsg, heartbeatTimeout uint64, electionTimeout uint64) *Raft {
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
		heartbeatTimer:   time.NewTimer(time.Millisecond * time.Duration(heartbeatTimeout)),
		electionTimer:    time.NewTimer(RandomElectionTimeout(electionTimeout)),
		heartbeatTimeout: heartbeatTimeout,
		electionTimeout:  electionTimeout,
	}

	raft.currTerm, raft.voteFor = raft.persister.ReadStateOfRaftLog()

	raft.ReInitLogs()

	raft.applyCond = sync.NewCond(&raft.mu)

	for _, peer := range peers {
		log.Log.Debugf("init peer-%d-config in-%d-,which address is %s", peer.id, raft.me, peer.addr)
		raft.nextIdx[peer.id], raft.matchIdx[peer.id] = int(raft.logs.lastIdx+1), 0
		//初始化复制协程
		if peer.id != uint64(raft.me) {
			raft.replicatorCond[peer.id] = sync.NewCond(&sync.Mutex{})
			go raft.Replicator(peer)
		}
	}

	go raft.Ticker()
	go raft.Applier()

	return raft
}

//
// Ticker
// @Description: 处理选举超时和心跳超时
//
func (r *Raft) Ticker() {
	for !r.IsDead() {
		select {
		case <-r.electionTimer.C:
			r.mu.Lock()
			r.IncrCurrTerm()
			r.SwitchRole(CANDIDATE)
			r.StartElection()
			r.electionTimer.Reset(RandomElectionTimeout(r.electionTimeout))
			r.mu.Unlock()
		case <-r.heartbeatTimer.C:
			if r.role == LEADER {
				r.Broadcast(true)
				r.heartbeatTimer.Reset(time.Millisecond * time.Duration(r.heartbeatTimeout))
			}
		}
	}
}

//
// Applier
// @Description: apply协程
//
func (r *Raft) Applier() {
	for !r.IsDead() {
		r.mu.Lock()
		//等待apply
		for r.lastApplied >= r.commitIdx {
			r.applyCond.Wait()
		}
		commitIdx, lastApplied := r.commitIdx, r.lastApplied
		entries := make([]*pb.Entry, commitIdx-lastApplied)
		log.Log.Debugf("peer-%d-start apply log entries from %d to %d in %d", r.me, lastApplied, commitIdx, r.currTerm)
		copy(entries, r.logs.GetRangeEntries(lastApplied+1, commitIdx+1))
		r.mu.Unlock()
		for _, entry := range entries {
			r.applyCh <- &pb.ApplyMsg{
				CommandValid: true,
				Command:      entry.GetData(),
				CommandTerm:  int64(entry.GetTerm()),
				CommandIndex: entry.GetIndex(),
			}
		}
		r.mu.Lock()
		r.lastApplied = int64(Max(int(r.lastApplied), int(commitIdx)))
		r.mu.Unlock()
	}
}

//
// IsDead
// @Description: 如果已dead返回true
//
func (r *Raft) IsDead() bool {
	return atomic.LoadInt32(&r.dead) == 1
}

// SwitchRole
// @Description: 改变当前节点角色
//
func (r *Raft) SwitchRole(role ROLE) {
	if r.role == role {
		return
	}
	r.role = role
	log.Log.Debugf("peer-%d-switch role into %s", r.me, RoleToString(role))
	switch role {
	case FOLLOWER:
		r.heartbeatTimer.Stop()
		r.electionTimer.Reset(RandomElectionTimeout(r.electionTimeout))
	case CANDIDATE:
	case LEADER:
		r.leaderId = int64(r.me)
		for i := 0; i < len(r.peers); i++ {
			r.nextIdx[i] = int(r.logs.lastIdx + 1)
			r.matchIdx[i] = 0
		}
		r.heartbeatTimer.Stop()
		r.electionTimer.Reset(RandomElectionTimeout(r.electionTimeout))
	}
}

func (r *Raft) IncrCurrTerm() {
	atomic.AddInt64(&r.currTerm, 1)
}

func (r *Raft) IncrGrantedVotes() {
	r.grantedVotes += 1
}

func (r *Raft) PersistState() {
	r.persister.PersistStateOfRaftLog(r.currTerm, r.voteFor)
}

func (r *Raft) GetLogCounts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.logs.LogCounts()
}

func (r *Raft) ReInitLogs() {
	err := r.logs.ReInitLogs()
	if err != nil {
		log.Log.Errorf("peer-%d-reinit logs error:%v", r.me, err)
		return
	}
}
