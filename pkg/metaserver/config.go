package metaserver

import (
	"rstorage/pkg/common"
	"rstorage/pkg/engine"
)

//
// MetaConfig
// @Description: MetaServer config
//
type MetaConfig struct {
	Version      int
	Slots        [common.SLOT_NUM]int
	ServerGroups map[int][]string
}

//
// ConfigSTM
// @Description:
//
type ConfigSTM interface {
	Join(serverGroups map[int][]string) error
	Leave(groupIds []int) error
	Query(versionID int) (MetaConfig, error)
}

//
// PersistConfig
// @Description: PersistConfig
//
type PersistConfig struct {
	db               engine.KvStore
	currentVersionID int
}

//
// NewPersistConfig
// @Description: New PersistConfig
//
func NewPersistConfig(db engine.KvStore) *PersistConfig {
	//todo
	return nil
}

func (p PersistConfig) Join(serverGroups map[int][]string) error {
	//TODO implement me
	panic("implement me")
}

func (p PersistConfig) Leave(groupIds []int) error {
	//TODO implement me
	panic("implement me")
}

func (p PersistConfig) Query(versionID int) (MetaConfig, error) {
	//TODO implement me
	panic("implement me")
}
