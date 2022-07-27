package engine

import "rstorage/pkg/log"

type KvStore interface {
	Put(key []byte, val []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) error
	GetPrefixRangeKvs(prefix []byte) ([]string, []string, error)
	SeekPrefixFirst(prefix []byte) ([]byte, []byte, error)
	SeekPrefixLast(prefix []byte) ([]byte, []byte, error)
	DelPrefixKeys(prefix []byte) error
	SeekPrefixKeyIdMax(prefix []byte) (uint64, error)
	DumpPrefixKey(prefix string) (map[string]string, error)
}

func KvStoreFactory(engineName string, path string) KvStore {
	switch engineName {
	case "levelDB":
		db, err := BuildLevelDBKvStore(path)
		if err != nil {
			log.MainLogger.Error().Msgf("build leveldb error")
			panic(err)
		}
		return db
	}
	return nil
}
