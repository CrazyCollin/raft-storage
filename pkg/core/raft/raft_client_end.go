package raft

import (
	"google.golang.org/grpc"
	"rstorage/pkg/log"
	"rstorage/pkg/protocol"
)

type RaftClientEnd struct {
	id             uint64
	addr           string
	conns          []*grpc.ClientConn
	raftServiceCli *protocol.RaftServiceClient
}

func NewRaftClientEnd(id uint64, addr string) *RaftClientEnd {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Log.Errorf("failed to connect:%v", err)
	}
	var conns []*grpc.ClientConn
	conns = append(conns, conn)
	rpcClient := protocol.NewRaftServiceClient(conn)
	return &RaftClientEnd{
		id:             id,
		addr:           addr,
		conns:          conns,
		raftServiceCli: &rpcClient,
	}
}

func (rce *RaftClientEnd) Id() uint64 {
	return rce.id
}

func (rce *RaftClientEnd) GetRaftServiceCli() *protocol.RaftServiceClient {
	return rce.raftServiceCli
}

func (rce *RaftClientEnd) CloseConn() {
	for _, conn := range rce.conns {
		conn.Close()
	}
}
