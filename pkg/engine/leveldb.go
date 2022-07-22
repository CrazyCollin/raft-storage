package engine

import (
	"CrazyCollin/personalProjects/raft-db/pkg/log"
	"encoding/binary"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type KvStoreLevelDB struct {
	path string
	db   *leveldb.DB
}

func BuildLevelDBKvStore(path string) (*KvStoreLevelDB, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{})
	if err != nil {
		return nil, err
	}
	return &KvStoreLevelDB{
		path: path,
		db:   db,
	}, err
}

func (k *KvStoreLevelDB) Put(key []byte, val []byte) error {
	return k.db.Put(key, val, nil)
}

func (k *KvStoreLevelDB) Get(key []byte) ([]byte, error) {
	return k.db.Get(key, nil)
}

func (k *KvStoreLevelDB) Del(key []byte) error {
	return k.db.Delete(key, nil)
}

//
// GetPrefixRangeKvs
// @Description: 按前缀范围查询
// @receiver k
// @param prefix
// @return []string
// @return []string
// @return error
//
func (k *KvStoreLevelDB) GetPrefixRangeKvs(prefix []byte) ([]string, []string, error) {
	keys := make([]string, 0)
	vals := make([]string, 0)
	iter := k.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		log.MainLogger.Debug().Msgf("iter key:%v,val:%v", iter.Key(), iter.Value())
		keys = append(keys, string(iter.Key()))
		vals = append(vals, string(iter.Value()))
	}
	iter.Release()
	return keys, vals, nil
}

//
// SeekPrefixFirst
// @Description: 查找符合第一个前缀的kv对
// @receiver k
// @param prefix
// @return []byte
// @return []byte
// @return error
//
func (k *KvStoreLevelDB) SeekPrefixFirst(prefix []byte) ([]byte, []byte, error) {
	iter := k.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	if iter.Next() {
		return iter.Key(), iter.Value(), nil
	}
	return nil, nil, errors.New("kv pair not found")
}

//
// SeekPrefixLast
// @Description: 查询最后一个符合前缀的kv对
// @receiver k
// @param prefix
// @return []byte
// @return []byte
// @return error
//
func (k *KvStoreLevelDB) SeekPrefixLast(prefix []byte) ([]byte, []byte, error) {
	iter := k.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()
	ok := iter.Last()
	var KeyBytes, ValBytes []byte
	if ok {
		KeyBytes = iter.Key()
		ValBytes = iter.Value()
	}
	return KeyBytes, ValBytes, nil
}

//
// DelPrefixKeys
// @Description: 删除含前缀的kv对
// @receiver k
// @param prefix
// @return error
//
func (k *KvStoreLevelDB) DelPrefixKeys(prefix []byte) error {
	iter := k.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		err := k.db.Delete(iter.Key(), nil)
		if err != nil {
			return err
		}
	}
	iter.Release()
	return nil
}

//
// SeekPrefixKeyIdMax
// @Description: 返回满足前缀的最大的id
// @receiver k
// @param prefix
// @return uint64
// @return error
//
func (k *KvStoreLevelDB) SeekPrefixKeyIdMax(prefix []byte) (uint64, error) {
	iter := k.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()
	var maxKeyId uint64
	maxKeyId = 0
	for iter.Next() {
		keyBytes := iter.Key()
		KeyId := binary.BigEndian.Uint64(keyBytes)
		if KeyId > maxKeyId {
			maxKeyId = KeyId
		}
	}
	return maxKeyId, nil
}

//
// DumpPrefixKey
// @Description: 保存满足前缀的kv map
// @receiver k
// @param prefix
// @return map[string]string
// @return error
//
func (k *KvStoreLevelDB) DumpPrefixKey(prefix string) (map[string]string, error) {
	kvs := make(map[string]string)
	iter := k.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		key := string(iter.Key())
		val := string(iter.Value())
		kvs[key] = val
	}
	iter.Release()
	return kvs, iter.Error()
}
