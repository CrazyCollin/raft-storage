package raft

import (
	"context"
	"rstorage/pkg/log"
	"rstorage/pkg/protocol"
)

//
// StartElection
// @Description: 当前节点发起选举
// @receiver r
//
func (r *Raft) StartElection() {
	//todo 开始新一轮选举
	log.Log.Debugf("node-%d-:start a new election", r.me)
	r.grantedVotes = 1
	r.voteFor = int64(r.me)
	//构造选举请求
	requestVoteReq := &protocol.RequestVoteReq{
		Term:         r.currTerm,
		CandidateId:  int64(r.me),
		LastLogIndex: r.logs.GetLastEntry().Index,
		LastLogTerm:  int64(r.logs.GetLastEntry().Term),
	}
	//持久化当前状态
	r.persister.PersistStateOfRaftLog(r.currTerm, r.voteFor)
	//
	for _, peer := range r.peers {
		if uint64(r.me) == peer.id {
			continue
		}
		//发送选举请求
		go func(peer *RaftClientEnd) {
			log.Log.Debugf("node-%d-start send vote request to peer-%d-", r.me, peer.id)

			requestVoteResp, err := (*peer.raftServiceCli).RequestVote(context.Background(), requestVoteReq)
			if err != nil {
				log.Log.Errorf("node-%d-start send vote request failed to peer-%d-", r.me, peer.id)
			}

			//获取选举请求回复
			if requestVoteResp != nil {
				//todo 处理选举请求回复
				r.mu.Lock()
				defer r.mu.Unlock()
				log.Log.Debugf("node-%d-reveived vote response from peer-%d-", r.me, peer.id)
				//处理选举回复
				if r.currTerm == requestVoteReq.Term && r.role == CANDIDATE {
					if requestVoteResp.VoteGranted {
						//选举自己，增加票数
						r.IncrGrantedVotes()
						log.Log.Debugf("node-%d-got a vote from peer-%d-", r.me, peer.id)
						//获得集群1/2节点的选举，选举成功，开始转换角色
						if r.grantedVotes > len(r.peers)/2 {
							r.SwitchRole(LEADER)
							r.Broadcast(true)
							r.grantedVotes = 0
							log.Log.Debugf("node-%d-become a new leader in cluster", r.me)
						}
					} else if requestVoteResp.Term > r.currTerm {
						//回复任期大于当前任期，恢复成follower
						r.SwitchRole(FOLLOWER)
						r.currTerm = requestVoteResp.Term
						r.voteFor = -1
						r.persister.PersistStateOfRaftLog(r.currTerm, r.voteFor)
					}
				}
			}
		}(peer)
	}
}

//
// HandleRequestVote
// @Description: 处理选举请求etcd
// @receiver r
// @param request
// @param resp
//
func (r *Raft) HandleRequestVote(request *protocol.RequestVoteReq, resp *protocol.RequestVoteResp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.Log.Debugf("node-%d-start to handle a vote request from peer", r.me)

	//查看和当前节点的log相比，进行选举的log是否更新
	expired := r.CheckDataExpired(request.LastLogIndex, request.LastLogTerm)
	//满足之前投的票就是当前请求的节点，或者是没有给其他节点投过票，或者请求消息的term比当前节点的任期要大
	canVote := r.voteFor == request.CandidateId || (r.voteFor == None && r.leaderId == None) || request.Term > r.currTerm

	if expired && canVote {
		//可以投票，赞成
		resp.Term, resp.VoteGranted = r.currTerm, true
	} else {
		//不能投赞成票
		resp.Term, resp.VoteGranted = r.currTerm, false
		return
	}
	r.voteFor = request.CandidateId
	//todo 重置选举计时器
}

//
// CheckDataExpired
// @Description: 检查当前节点日志状态是否过期，已过期则返回true，总要保证选举节点是最新状态
// @receiver r
// @param lastIdx
// @param term
// @return bool
//
func (r *Raft) CheckDataExpired(lastIdx, term int64) bool {
	lastEntry := r.logs.GetLastEntry()
	var dataIsExpired bool
	//请求的任期较大
	if term > int64(lastEntry.GetTerm()) {
		dataIsExpired = true
		return dataIsExpired
	}
	//请求的任期和目前相等，但log更加新
	if term == int64(lastEntry.GetTerm()) && lastIdx >= lastEntry.GetIndex() {
		dataIsExpired = true
		return dataIsExpired
	}
	return dataIsExpired
}
