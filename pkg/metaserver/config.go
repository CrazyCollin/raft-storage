package metaserver

import (
	"bytes"
	"encoding/json"
	"rstorage/pkg/common"
	"rstorage/pkg/engine"
	"rstorage/pkg/log"
	"strconv"
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

func DefaultMetaConfig() MetaConfig {
	return MetaConfig{
		ServerGroups: make(map[int][]string),
	}
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
	var buffer bytes.Buffer
	buffer.Write(common.CONFIG_META_PREFIX)
	buffer.Write([]byte(strconv.Itoa(0)))
	configSTM := &PersistConfig{
		db:               db,
		currentVersionID: 0,
	}
	//start reload config from db engine or create default config
	_, err := configSTM.db.Get(buffer.Bytes())
	if err != nil {
		//default config
		var err error
		defaultMetaConfig := DefaultMetaConfig()
		configBytes, err := json.Marshal(defaultMetaConfig)
		if err != nil {
			log.Log.Errorf("marshal default config error: %v\n", err)
			return nil
		}
		err = configSTM.db.Put(buffer.Bytes(), configBytes)
		if err != nil {
			log.Log.Errorf("put default config error: %v\n", err)
			return nil
		}
		err = configSTM.db.Put(common.CURR_CONFIG_VERSION_KEY, []byte(strconv.Itoa(configSTM.currentVersionID)))
		if err != nil {
			log.Log.Errorf("put default config version key error: %v\n", err)
			return nil
		}
		return configSTM
	}
	//reload config from db engine,renew config version id
	versionID, err := configSTM.db.Get(common.CURR_CONFIG_VERSION_KEY)
	if err != nil {
		log.Log.Errorf("get config version id error: %v\n", err)
		return nil
	}
	configSTM.currentVersionID, _ = strconv.Atoi(string(versionID))
	return configSTM
}

//
// Join
// @Description: join server groups in meta config
//
func (p *PersistConfig) Join(serverGroups map[int][]string) error {

	var buffer bytes.Buffer
	buffer.Write(common.CONFIG_META_PREFIX)
	buffer.Write([]byte(strconv.Itoa(p.currentVersionID)))
	lastMetaConfig := &MetaConfig{}
	lastMetaConfigBytes, err := p.db.Get(buffer.Bytes())
	if err != nil {
		log.Log.Errorf("get last config error: %v\n", err)
		return err
	}
	_ = json.Unmarshal(lastMetaConfigBytes, lastMetaConfig)
	//all map edited is in new map
	newMetaConfig := &MetaConfig{
		Version:      p.currentVersionID + 1,
		Slots:        lastMetaConfig.Slots,
		ServerGroups: copyMap(lastMetaConfig.ServerGroups),
	}
	for groupID, serverList := range serverGroups {
		//check if the group is not existed,if not existed,then put it into new config
		//operation always in new config
		if _, ok := lastMetaConfig.ServerGroups[groupID]; !ok {
			tmp := make([]string, len(serverList))
			copy(tmp, serverList)
			newMetaConfig.ServerGroups[groupID] = tmp
			continue
		}
	}

	//set new meta config slot info after server group info updated
	var groupIDList []int
	for groupID := range newMetaConfig.ServerGroups {
		groupIDList = append(groupIDList, groupID)
	}
	//configure slots
	configureSlots(newMetaConfig.Slots[:], groupIDList)

	//update current version id
	p.currentVersionID = newMetaConfig.Version
	err = p.db.Put(common.CURR_CONFIG_VERSION_KEY, []byte(strconv.Itoa(p.currentVersionID)))
	if err != nil {
		log.Log.Errorf("put current version id error: %v\n", err)
		return err
	}
	//update new meta config
	newMetaConfigBytes, _ := json.Marshal(newMetaConfig)

	buffer = bytes.Buffer{}
	buffer.Write(common.CONFIG_META_PREFIX)
	buffer.Write([]byte(strconv.Itoa(p.currentVersionID)))
	err = p.db.Put(buffer.Bytes(), newMetaConfigBytes)
	if err != nil {
		log.Log.Errorf("put new config in db error: %v\n", err)
		return err
	}
	return nil
}

//
// Leave
// @Description: leave server groups in meta config
//
func (p *PersistConfig) Leave(groupIds []int) error {
	var buffer bytes.Buffer
	buffer.Write(common.CONFIG_META_PREFIX)
	buffer.Write([]byte(strconv.Itoa(p.currentVersionID)))
	lastMetaConfig := &MetaConfig{}
	lastMetaConfigBytes, err := p.db.Get(buffer.Bytes())
	if err != nil {
		log.Log.Errorf("get last config error: %v\n", err)
		return err
	}
	_ = json.Unmarshal(lastMetaConfigBytes, lastMetaConfig)
	//all map edited is in new map
	newMetaConfig := &MetaConfig{
		Version:      p.currentVersionID + 1,
		Slots:        lastMetaConfig.Slots,
		ServerGroups: copyMap(lastMetaConfig.ServerGroups),
	}
	//delete server group in new config
	for _, groupID := range groupIds {
		delete(newMetaConfig.ServerGroups, groupID)
	}
	//set new meta config slot info after server group info updated
	var groupIDList []int
	for groupID := range newMetaConfig.ServerGroups {
		groupIDList = append(groupIDList, groupID)
	}
	//configure slots
	configureSlots(newMetaConfig.Slots[:], groupIDList)

	//update current version id
	p.currentVersionID = newMetaConfig.Version
	err = p.db.Put(common.CURR_CONFIG_VERSION_KEY, []byte(strconv.Itoa(p.currentVersionID)))
	if err != nil {
		log.Log.Errorf("put current version id error: %v\n", err)
		return err
	}
	//update new meta config
	newMetaConfigBytes, _ := json.Marshal(newMetaConfig)

	buffer = bytes.Buffer{}
	buffer.Write(common.CONFIG_META_PREFIX)
	buffer.Write([]byte(strconv.Itoa(p.currentVersionID)))
	err = p.db.Put(buffer.Bytes(), newMetaConfigBytes)
	if err != nil {
		log.Log.Errorf("put new config in db error: %v\n", err)
		return err
	}
	return nil
}

//
// Query
// @Description: query meta config by version id
//
func (p *PersistConfig) Query(versionID int) (MetaConfig, error) {

	var buffer bytes.Buffer
	var metaConfig MetaConfig
	buffer.Write(common.CONFIG_META_PREFIX)
	if versionID < 0 || versionID >= p.currentVersionID {
		//return latest config
		versionID = p.currentVersionID
		buffer.Write([]byte(strconv.Itoa(versionID)))
		metaConfigBytes, err := p.db.Get(buffer.Bytes())
		if err != nil {
			return DefaultMetaConfig(), err
		}
		_ = json.Unmarshal(metaConfigBytes, &metaConfig)
		return metaConfig, nil
	} else {
		//return specific config
		buffer.Write([]byte(strconv.Itoa(versionID)))
		metaConfigBytes, err := p.db.Get(buffer.Bytes())
		if err != nil {
			return DefaultMetaConfig(), err
		}
		_ = json.Unmarshal(metaConfigBytes, &metaConfig)
		return metaConfig, nil
	}
}

// todo figured out how to configure slots
func configureSlots(slotList []int, groupIDList []int) {
	index := 0
	for i := 0; i < common.SLOT_NUM; i++ {
		slotList[i] = groupIDList[index]
		index++
		//reset index
		if index == len(groupIDList) {
			index = 0
		}
	}
}

func copyMap(src map[int][]string) map[int][]string {
	dst := make(map[int][]string)
	for k, v := range src {
		tmp := make([]string, len(v))
		copy(tmp, v)
		dst[k] = tmp
	}
	return dst
}
