package client

import (
	"bufio"
	"errors"
	"io"
	"os"
	"rstorage/pkg/blockserver"
	"rstorage/pkg/common"
	"rstorage/pkg/metaserver"
	pb "rstorage/pkg/protocol"
	"strings"
)

type Client struct {
	metaSvrCli  *metaserver.MetaServiceCli
	blockSvrCli *blockserver.BlockServiceCli
}

func NewClient(metaSvrAdders string, accessKey string, accessSecret string) *Client {
	addresses := strings.Split(metaSvrAdders, ",")
	metaCli := metaserver.NewMetaServiceCli(addresses)
	return &Client{
		metaSvrCli: metaCli,
	}
}

func (c *Client) GetMetaSvrCli() *metaserver.MetaServiceCli {
	return c.metaSvrCli
}

func (c *Client) GetBlockSvrCil() *blockserver.BlockServiceCli {
	return c.blockSvrCli
}

func (c *Client) ListBuckets() ([]*pb.Bucket, error) {
	return nil, nil
}

func (c *Client) CreateBucket(name string) error {
	return nil
}

func (c *Client) DeleteBucket() error {
	return nil
}

func (c *Client) Bucket(name string) (*pb.Bucket, error) {
	return nil, nil
}

func (c *Client) UploadFile(localPath string, bucketId string) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	fileReader := bufio.NewReader(file)
	blockBuffer := make([]byte, common.FILE_BLOCK_SIZE)
	var fileBlockMetas []*pb.FileBlockMeta
	objRandId := common.GenUUID()
	index := 0
	for {
		n, err := fileReader.Read(blockBuffer)
		if err != nil && err != io.EOF {
			return "", err
		}
		// read last file block
		if n == 0 {
			break
		}
		// query server group meta
		req := pb.ServerGroupMetaConfigRequest{
			ConfigVersion: -1,
			OpType:        pb.ConfigServerGroupMetaOpType_OP_SERVER_GROUP_QUERY,
		}
		// get meta config
		serverGroupMetaResp := c.metaSvrCli.GetServerGroupMeta(&req)
		if err != nil {
			return "", err
		}

		blockStr := ""
		if n < 64 {
			blockStr = string(blockBuffer[:n])
		} else {
			blockStr = string(blockBuffer[:64])
		}
		// transform block to slot id
		//todo hash
		slot := common.StrToSlot(blockStr)
		blockMeta := &pb.FileBlockMeta{
			BlockId:     int64(index),
			BlockSlotId: int64(slot),
		}
		// get nodes of selected cluster
		slotsToGroupArr := serverGroupMetaResp.ServerGroupMetas.Slots
		serverGroupAdders := serverGroupMetaResp.ServerGroupMetas.ServerGroups[slotsToGroupArr[slot]]
		serverAddrArr := strings.Split(serverGroupAdders, ",")
		// get block server client
		c.blockSvrCli = blockserver.NewBlockServiceCli(serverAddrArr)
		fileBlockRequest := &pb.FileBlockOpRequest{
			FileName:       objRandId,
			FileBlocksMeta: blockMeta,
			BlockContent:   blockBuffer[:n],
			OpType:         pb.FileBlockOpType_OP_BLOCK_WRITE,
		}
		writeBlockResp := c.blockSvrCli.BlockFileOp(fileBlockRequest)
		if err != nil {
			return "", err
		}
		if writeBlockResp.ErrCode != pb.ErrCode_NO_ERR {
			return "", errors.New("write error")
		}
		fileBlockMetas = append(fileBlockMetas, blockMeta)
		index += 1
	}
	// write object meta to meta server
	req := pb.ServerGroupMetaConfigRequest{
		OpType: pb.ConfigServerGroupMetaOpType_OP_OSS_OBJECT_PUT,
		BucketOpReq: &pb.BucketOpRequest{
			Object: &pb.Object{
				ObjectId:         localPath,
				ObjectName:       objRandId,
				FromBucketId:     bucketId,
				ObjectBlocksMeta: fileBlockMetas,
			},
			BucketId: bucketId,
		},
	}
	serverGroupMetaResp := c.metaSvrCli.GetServerGroupMeta(&req)
	if err != nil {
		return "", err
	}
	if serverGroupMetaResp.ErrCode != pb.ErrCode_NO_ERR {
		return "", errors.New("write meta error")
	}
	return objRandId, nil
}

func (c *Client) DownloadFile(bucketId string, objName string, localFilePath string) error {
	//1.FileBlockMeta find file block meta
	// query object list
	req := pb.ServerGroupMetaConfigRequest{
		ConfigVersion: -1,
		OpType:        pb.ConfigServerGroupMetaOpType_OP_OSS_OBJECT_LIST,
		BucketOpReq: &pb.BucketOpRequest{
			BucketId: bucketId,
		},
	}
	// get meta config
	objListMetaResp := c.metaSvrCli.GetServerGroupMeta(&req)
	if objListMetaResp.ErrCode != pb.ErrCode_NO_ERR {
		return errors.New("query object list err")
	}
	file, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	//
	writer := bufio.NewWriter(file)
	for _, obj := range objListMetaResp.BucketOpRes.Objects {
		if obj.ObjectName == objName {
			// find block meta
			// query server group meta
			req := pb.ServerGroupMetaConfigRequest{
				ConfigVersion: -1,
				OpType:        pb.ConfigServerGroupMetaOpType_OP_SERVER_GROUP_QUERY,
			}
			serverGroupMetaResp := c.metaSvrCli.GetServerGroupMeta(&req)
			slotsToGroupArr := serverGroupMetaResp.ServerGroupMetas.Slots
			for _, blockMeta := range obj.ObjectBlocksMeta {
				serverGroupAdders := serverGroupMetaResp.ServerGroupMetas.ServerGroups[slotsToGroupArr[blockMeta.BlockSlotId]]
				// find block server
				serverAddrArr := strings.Split(serverGroupAdders, ",")
				c.blockSvrCli = blockserver.NewBlockServiceCli(serverAddrArr)
				fileBlockRequest := &pb.FileBlockOpRequest{
					FileName:       objName,
					FileBlocksMeta: blockMeta,
					OpType:         pb.FileBlockOpType_OP_BLOCK_READ,
				}
				readBlockResp := c.blockSvrCli.BlockFileOp(fileBlockRequest)
				_, _ = writer.Write(readBlockResp.BlockContent)

				_ = writer.Flush()
			}
		}
	}
	return nil
}
