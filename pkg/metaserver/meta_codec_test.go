package metaserver

import "testing"

func TestDecodeBucketKey(t *testing.T) {
	encodedData := EncodeBucketKey("01")
	decodedData := DecodeBucketKey(encodedData)
	if decodedData != "01" {
		t.Errorf("decode bucket key failed, decodedData:%v\n", decodedData)
	} else {
		t.Logf("decode bucket key success, decodedData:%v\n", decodedData)
	}
}
