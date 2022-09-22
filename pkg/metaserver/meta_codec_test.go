package metaserver

import (
	pb "rstorage/pkg/protocol"
	"testing"
)

func TestDecodeBucketKey(t *testing.T) {
	encodedData := EncodeBucketKey("01")
	decodedData := DecodeBucketKey(encodedData)
	if decodedData != "01" {
		t.Errorf("decode bucket key failed, decodedData:%v\n", decodedData)
	} else {
		t.Logf("decode bucket key success, decodedData:%v\n", decodedData)
	}
}

func TestDecodeObjectKey(t *testing.T) {
	encodedData := EncodeObjectKey("01")
	decodedData := DecodeObjectKey(encodedData)
	if decodedData != "01" {
		t.Errorf("decode object key failed, decodedData:%v\n", decodedData)
	} else {
		t.Logf("decode object key success, decodedData:%v\n", decodedData)
	}
}

func TestDecodeObject(t *testing.T) {
	object := &pb.Object{
		ObjectId:     "01",
		ObjectName:   "test",
		FromBucketId: "01",
	}
	object.ObjectBlocksMeta = []*pb.FileBlockMeta{
		{
			BlockId:     01,
			BlockSlotId: 01,
		},
	}
	encodedData := EncodeObject(object)
	decodedData := DecodeObject(encodedData)
	// return true if two objects are equal
	checkObjectBlocksMeta := func(enc, dec *[]*pb.FileBlockMeta) bool {
		for i := 0; i < len(*enc); i++ {
			if (*enc)[i].BlockId != (*dec)[i].BlockId || (*enc)[i].BlockSlotId != (*dec)[i].BlockSlotId {
				return false
			}
		}
		return true
	}
	if decodedData.ObjectId != "01" || decodedData.ObjectName != "test" || decodedData.FromBucketId != "01" || !checkObjectBlocksMeta(&object.ObjectBlocksMeta, &decodedData.ObjectBlocksMeta) {
		t.Errorf("decode object failed, decodedData:%v\n", decodedData)
	} else {
		t.Logf("decode object success, decodedData:%v\n", decodedData)
	}
}
