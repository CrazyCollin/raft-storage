package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"rstorage/pkg/common"
	pb "rstorage/pkg/protocol"
)

//
// EncodeRaftLogKey
// @Description: 序列化raft log的key
//
func EncodeRaftLogKey(index uint64) []byte {
	var byteBuffer bytes.Buffer
	byteBuffer.Write(common.RAFTLOG_PREFIX)
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, index)
	byteBuffer.Write(data)
	return byteBuffer.Bytes()
}

//
// DecodeRaftLogKey
// @Description: 反序列化raft log的key
//
func DecodeRaftLogKey(dataBytes []byte) uint64 {
	//从第五个字节开始返回
	return binary.BigEndian.Uint64(dataBytes[4:])
}

//
// EncodeEntry
// @Description: 序列化日志条目
//
func EncodeEntry(entry *pb.Entry) []byte {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	_ = encoder.Encode(entry)
	return byteBuffer.Bytes()
}

//
// DecodeEntry
// @Description: 反序列化日志条目
//
func DecodeEntry(dataBytes []byte) *pb.Entry {
	decoder := gob.NewDecoder(bytes.NewBuffer(dataBytes))
	entry := &pb.Entry{}
	_ = decoder.Decode(entry)
	return entry
}

func EncodeRaftState(raftState *StateOfRaftLog) []byte {
	var byteBuffer bytes.Buffer
	encoder := gob.NewEncoder(&byteBuffer)
	_ = encoder.Encode(raftState)
	return byteBuffer.Bytes()
}

func DecodeRaftState(dataBytes []byte) *StateOfRaftLog {
	decoder := gob.NewDecoder(bytes.NewBuffer(dataBytes))
	state := &StateOfRaftLog{}
	_ = decoder.Decode(state)
	return state
}
