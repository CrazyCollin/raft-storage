package metaserver

import (
	"context"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

//
// MetaServiceCli
// @Description: meta服务客户端
//
type MetaServiceCli struct {
	endpoints []*MetaServiceClientEnd
}

func NewMetaServiceCli(addresses []string) *MetaServiceCli {
	var endpoints []*MetaServiceClientEnd
	for i, addr := range addresses {
		endpoint := NewMetaServiceClientEnd(uint64(i), addr)
		endpoints = append(endpoints, endpoint)
	}
	return &MetaServiceCli{
		endpoints: endpoints,
	}
}

//
// GetServerGroupMeta
// @Description: 获取server group meta
//
func (cli *MetaServiceCli) GetServerGroupMeta(req *pb.ServerGroupMetaConfigRequest) (resp *pb.ServerGroupMetaConfigResponse) {
	resp.ServerGroupMetas = &pb.ServerGroupMetas{}
	var err error
	for _, endpoint := range cli.endpoints {
		client := endpoint.GetMetaServiceClient()
		resp, err = (*client).ServerGroupMeta(context.Background(), req)
		if err != nil {
			log.Log.Errorf("get server group meta from node address %s failed, err:%v\n", endpoint.addr, err)
			continue
		}
		switch resp.ErrCode {
		case pb.ErrCode_NO_ERR:
			return resp
		case pb.ErrCode_WRONG_LEADER_ERR:
			//retry rpc call with leader
			client := cli.endpoints[resp.LeaderId].GetMetaServiceClient()
			resp, err = (*client).ServerGroupMeta(context.Background(), req)
			if err != nil {
				log.Log.Errorf("get server group meta from leader address %s failed, err:%v\n", endpoint.addr, err)
			}
			if resp.ErrCode == pb.ErrCode_RPC_CALL_TIMEOUT_ERR {
				log.Log.Errorf("get server group meta from leader address %s timeout\n", endpoint.addr)
			}
			return resp
		}
	}
	return resp
}
