package log

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/opencord/voltha-lib-go/v2/pkg/db/kvstore"
	"github.com/opencord/voltha-lib-go/v2/pkg/db/model"
	 "github.com/opencord/voltha-lib-go/v2/pkg/log"
	"strings"
)

type ConfigType int

const (
	ConfigTypeLogLevel ConfigType = iota
	ConfigTypeKafka
)

func (c ConfigType) String() string {
	return [...]string{"loglevel", "kafka"}[c]
}

type ChangeEventType int

const (
	Put ChangeEventType = iota
	Delete
)

func (c ChangeEventType) EventString() string {
	return [...]string{"Put", "Delete"}[c]
}

type ConfigChangeEvent struct {
	ChangeType  ChangeEventType
	Key         string
	PackageName string
	Value       interface{}
}

type ConfigManager struct {
	backend             *model.Backend
	KvStoreConfigPrefix string
}

//Create New ConfigManager
func NewConfigManager(kvClient kvstore.Client, kvStoreType, kvStoreHost, kvStoreDataPrefix, kvStoreConfigPrefix string, kvStorePort, kvStoreTimeout int) *ConfigManager {
	// Setup the KV store
	var cm ConfigManager
	cm.KvStoreConfigPrefix = kvStoreConfigPrefix
	cm.backend = &model.Backend{
		Client:     kvClient,
		StoreType:  kvStoreType,
		Host:       kvStoreHost,
		Port:       kvStorePort,
		Timeout:    kvStoreTimeout,
		PathPrefix: kvStoreDataPrefix}
	return &cm
}

//Initialize the component config
func (cm *ConfigManager) InitComponentConfig(componentLabel string, configType ConfigType) (*ComponentConfig, error) {
	//construct ComponentConfig

	cConfig := &ComponentConfig{
		componentLabel:   componentLabel,
		configType:       configType,
		cManager:         cm,
		changeEventChan:  nil,
		kvStoreEventChan: nil,
	}

	return cConfig, nil
}

type ComponentConfig struct {
	cManager         *ConfigManager
	componentLabel   string
	configType       ConfigType
	changeEventChan  chan *ConfigChangeEvent
	kvStoreEventChan chan *kvstore.Event
}

func (c *ComponentConfig) makeConfigPath() string {
	//construct path
	cType := c.configType.String()
	return c.cManager.KvStoreConfigPrefix + c.componentLabel + "/" + cType
}

//MonitorForConfigChange watch on the keys
//If any changes happen then process the event create new ConfigChangeEvent and return
func (c *ComponentConfig) MonitorForConfigChange() (chan *ConfigChangeEvent, error) {
	//call makeConfigPath function to create path
	key := c.makeConfigPath()

	//call backend createwatch method
	c.kvStoreEventChan = make(chan *kvstore.Event)
	c.changeEventChan = make(chan *ConfigChangeEvent)

	c.kvStoreEventChan = c.cManager.backend.CreateWatchForSubKeys(key)

	go c.processKVStoreWatchEvents()

	return c.changeEventChan, nil
}

//processKVStoreWatchEvents process the kvStoreEventChan and create ConfigChangeEvent
func (c *ComponentConfig) processKVStoreWatchEvents() {
	//In a loop process incoming kvstore events and push changes to changeEventChan

	for watchResp := range c.kvStoreEventChan {
		key := fmt.Sprintf("%s", watchResp.Key)
		ky := strings.SplitAfter(key, c.cManager.KvStoreConfigPrefix)
		configKey := strings.Split(ky[1], "/"+c.configType.String())
		pname := strings.SplitAfter(configKey[1], "/")

		ChangeType := ChangeEventType(watchResp.EventType)
		configEvent := &ConfigChangeEvent{ChangeType, configKey[0], pname[1], watchResp.Value}
		c.changeEventChan <- configEvent
	}
}

func (c *ComponentConfig) Retreive() (map[string]interface{}, error) {

	//construct key using makeConfigPath
	key := c.makeConfigPath()

	//perform get operation on backend using constructed key
	data, err := c.cManager.backend.Get(key)
	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	if data.Value != nil {
		res[data.Key] = data.Value
		return res, nil
	}
	return nil, errors.New("data not found")
}

func (c *ComponentConfig) RetreiveAsString() (string, error) {
	//call  Retrieve method

	data, err := c.Retreive()
	if err != nil {
		return "", err
	}
	//convert interface value to string and return
	return fmt.Sprintf("%v", data), nil
}

func (c *ComponentConfig) RetreiveAll() (map[string]*kvstore.KVPair, error) {
	key := c.makeConfigPath()

	data, err := c.cManager.backend.List(key)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *ComponentConfig) Save(configKey string, configValue interface{}) error {
	//construct key using makeConfigPath
	key := c.makeConfigPath() + "/" + configKey

	configVal, err := json.Marshal(configValue)
	if err != nil {
		log.Error(err)
	}

	//save the data for update config
	err = c.cManager.backend.Put(key, configVal)
	if err != nil {
		return err
	}
	return nil
}

func (c *ComponentConfig) Delete(configKey string) error {
	//construct key using makeConfigPath
	key := c.makeConfigPath()

	//delete the config
	err := c.cManager.backend.Delete(key)
	if err != nil {
		return err
	}
	return nil
}
