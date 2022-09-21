package metaserver

import pb "rstorage/pkg/protocol"

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
	for _, _ = range cli.endpoints {
		//todo call endpoint
	}
	return resp
}
