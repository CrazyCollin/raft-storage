package metaserver

import (
	"google.golang.org/grpc"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

type MetaServiceClientEnd struct {
	id         uint64
	addr       string
	serviceCli *pb.MetaServiceClient
	conns      []*grpc.ClientConn
}

func NewMetaServiceClientEnd(id uint64, addr string) *MetaServiceClientEnd {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Log.Errorf("err: %v ", err.Error())
	}
	var conns []*grpc.ClientConn
	conns = append(conns, conn)
	rpcClient := pb.NewMetaServiceClient(conn)
	return &MetaServiceClientEnd{
		id:         id,
		addr:       addr,
		serviceCli: &rpcClient,
		conns:      conns,
	}
}

func (e *MetaServiceClientEnd) CloseClientConns() {
	for _, conn := range e.conns {
		_ = conn.Close()
	}
}
