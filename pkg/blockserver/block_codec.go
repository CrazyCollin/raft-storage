package blockserver

import (
	"bytes"
	"encoding/gob"
	"rstorage/pkg/log"
	pb "rstorage/pkg/protocol"
)

func EncodeBlockServerRequest(req *pb.FileBlockOpRequest) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(req)
	if err != nil {
		log.Log.Errorf("encode block server request error: %v", err)
		return nil
	}
	return buffer.Bytes()
}

func DecodeBlockServerRequest(date []byte) *pb.FileBlockOpRequest {
	var buffer bytes.Buffer
	decoder := gob.NewDecoder(&buffer)
	req := pb.FileBlockOpRequest{}
	err := decoder.Decode(&req)
	if err != nil {
		log.Log.Errorf("decode block server request error: %v", err)
		return nil
	}
	return &req
}
