package blockserver

import (
	"google.golang.org/grpc"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

// BlockServiceClientEnd
// @Description: BlockServiceClientEnd
type BlockServiceClientEnd struct {
	blockServiceClient *pb.FileBlockServiceClient
	id                 uint64
	addr               string
	conns              []*grpc.ClientConn
}

func NewBlockServiceClientEnd(id uint64, addr string) *BlockServiceClientEnd {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Log.Errorf("err: %v ", err.Error())
	}
	var conns []*grpc.ClientConn
	conns = append(conns, conn)
	client := pb.NewFileBlockServiceClient(conn)
	return &BlockServiceClientEnd{
		id:                 id,
		addr:               addr,
		blockServiceClient: &client,
		conns:              conns,
	}
}

func (e *BlockServiceClientEnd) GetBlockServiceClient() *pb.FileBlockServiceClient {
	return e.blockServiceClient
}

func (e *BlockServiceClientEnd) CloseClientConns() {
	for _, conn := range e.conns {
		_ = conn.Close()
	}
}
