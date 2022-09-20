package metaserver

import (
	"google.golang.org/grpc"
	pb "rstorage/pkg/protocol"
)

type MetaServiceClientEnd struct {
	serviceCli *pb.MetaServiceClient
	id         uint64
	addr       string
	conns      []*grpc.ClientConn
}
