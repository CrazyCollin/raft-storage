package blockserver

import (
	"context"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

type BlockServiceCli struct {
	endpoints []*BlockServiceClientEnd
}

func NewBlockServiceCli(addresses []string) *BlockServiceCli {
	var endpoints []*BlockServiceClientEnd
	for i, addr := range addresses {
		endpoint := NewBlockServiceClientEnd(uint64(i), addr)
		endpoints = append(endpoints, endpoint)
	}
	return &BlockServiceCli{
		endpoints: endpoints,
	}
}

func (cli *BlockServiceCli) BlockFileOp(req *pb.FileBlockOpRequest) (resp *pb.FileBlockOpResponse) {
	resp = &pb.FileBlockOpResponse{}
	var err error
	for _, endpoint := range cli.endpoints {
		client := endpoint.GetBlockServiceClient()
		resp, err = (*client).FileBlockOp(context.Background(), req)
		if err != nil {
			log.Log.Warnf("block file op from node address %s failed, err:%v\n", endpoint.addr, err)
			continue
		}
		// handle error code
		switch resp.ErrCode {
		case pb.ErrCode_NO_ERR:
			return resp
		case pb.ErrCode_WRONG_LEADER_ERR:
			//retry rpc call with leader
			log.Log.Debugf("redirect to leader %d", resp.LeaderId)
			client := cli.endpoints[resp.LeaderId].GetBlockServiceClient()
			resp, err = (*client).FileBlockOp(context.Background(), req)
			if err != nil {
				log.Log.Errorf("block file op from leader address %s failed, err:%v\n", endpoint.addr, err)
				continue
			}
			if resp.ErrCode == pb.ErrCode_RPC_CALL_TIMEOUT_ERR {
				log.Log.Errorf("block file op from leader address %s timeout\n", endpoint.addr)
			}
			return resp
		}
	}
	return resp
}
