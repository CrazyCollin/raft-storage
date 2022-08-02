package raft

import (
	"context"
	"rstorage/pkg/log"
	"rstorage/pkg/protocol"
)

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
							r.SendHeartbeat()
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
