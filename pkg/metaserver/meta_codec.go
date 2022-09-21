package metaserver

import (
	"bytes"
	"encoding/gob"
	"rstorage/pkg/common"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

//
// EncodeServerGroupMetaRequest
// @Description: encode server group meta request to bytes sequence
//
func EncodeServerGroupMetaRequest(req *pb.ServerGroupMetaConfigRequest) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(req)
	if err != nil {
		log.Log.Debugf("encode server group meta request failed, err:%v\n", err)
		return nil
	}
	return buffer.Bytes()
}

//
// DecodeServerGroupMetaRequest
// @Description: decode bytes sequence to server group meta request
//
func DecodeServerGroupMetaRequest(data []byte) *pb.ServerGroupMetaConfigRequest {
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)
	req := pb.ServerGroupMetaConfigRequest{}
	err := decoder.Decode(&req)
	if err != nil {
		log.Log.Debugf("decode server group meta request failed, err:%v\n", err)
	}
	return &req
}

//
// EncodeBucketKey
// @Description: encode bucket key to bytes sequence = BUCKET_META_PREFIX + bucketID
//
func EncodeBucketKey(bucketID string) []byte {
	var buffer bytes.Buffer
	buffer.Write(common.BUCKET_META_PREFIX)
	buffer.Write([]byte(bucketID))
	return buffer.Bytes()
}

//
// DecodeBucketKey
// @Description: decode bytes sequence to bucket key
//
func DecodeBucketKey(data []byte) string {
	return string(data[len(common.BUCKET_META_PREFIX):])
}

func EncodeBucket(bucket *pb.Bucket) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(bucket)
	if err != nil {
		log.Log.Debugf("encode bucket failed, err:%v\n", err)
		return nil
	}
	return buffer.Bytes()
}

func DecodeBucket(data []byte) *pb.Bucket {
	var buffer bytes.Buffer
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)
	bucket := pb.Bucket{}
	err := decoder.Decode(&bucket)
	if err != nil {
		log.Log.Debugf("decode bucket failed, err:%v\n", err)
	}
	return &bucket
}
