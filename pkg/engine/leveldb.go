package engine

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
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

func (k KvStoreLevelDB) Put(key []byte, val []byte) error {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) Get(key []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) Del(key []byte) error {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) GetPrefixRangeKvs(prefix []byte) ([]string, []string, error) {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) SeekPrefixFirst(prefix []byte) ([]byte, []byte, error) {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) SeekPrefixLast(prefix []byte) ([]byte, []byte, error) {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) DelPrefixKeys(prefix []byte) error {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) SeekPrefixKeyIdMax(prefix []byte) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (k KvStoreLevelDB) DumpPrefixKey(prefix string) (map[string]string, error) {
	//TODO implement me
	panic("implement me")
}
