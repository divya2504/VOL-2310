package logger

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/opencord/voltha-lib-go/v2/pkg/db/kvstore"
	"github.com/opencord/voltha-lib-go/v2/pkg/log"
	"os"
	"strings"
)

type ComponentLogController struct {
	ComponentName       string
	componentNameConfig *ComponentConfig
	globalConfig        *ComponentConfig
	configManager       *ConfigManager
	logHash             [16]byte
}

//create new component LogController
func NewComponentLogController(cm *ConfigManager) (*ComponentLogController, error) {
	//populate the ComponentLogController with env variables
	componentName := os.Getenv("COMPONENTNAME")
	hash := [16]byte{}

	cc := &ComponentLogController{
		ComponentName:       componentName,
		componentNameConfig: nil,
		globalConfig:        nil,
		configManager:       cm,
		logHash:             hash}

	return cc, nil
}

//ProcessLogConfigChange initialize component config and global config then process the log config changes
func ProcessLogConfigChange(cm *ConfigManager) {
	//populate the ComponentLogController using NewComponentLogController
	cc, err := NewComponentLogController(cm)
	if err != nil {
		log.Error("error", err)
	}

	ccGlobal, err := cm.InitComponentConfig("global", ConfigTypeLogLevel)
	if err != nil {
		log.Errorw("fail-to-initialize-gloabl-loglevel-config", log.Fields{"error": err})
	}
	cc.globalConfig = ccGlobal

	ccComponentName, err := cm.InitComponentConfig(cc.ComponentName, ConfigTypeLogLevel)
	if err != nil {
		log.Errorw("fail-to-inialize-component-loglevel-config", log.Fields{"error": err})
	}
	cc.componentNameConfig = ccComponentName

	//call ProcessConfigChange and check for any changes to config
	cc.processLogConfig()
}

//processLogConfig wait on componentn config and global config channel for any changes
//If any changes happen in kvstore then process the changes and update the log config
//then load and apply the config for the component
func (c *ComponentLogController) processLogConfig() {

	//call MonitorForConfigChange  for componentNameConfig
	//get ConfigChangeEvent Channel for componentName
	changeInComponentNameConfigEvent, _ := c.componentNameConfig.MonitorForConfigChange()

	//call MonitorForConfigChange  for global
	//get ConfigChangeEvent Channel for global
	changeInglobalConfigEvent, _ := c.globalConfig.MonitorForConfigChange()

	//process the events for componentName and  global config
	changeEvent := &ConfigChangeEvent{}
	configEvent := &ConfigChangeEvent{}
	for {
		select {
		case configEvent = <-changeInglobalConfigEvent:
		case configEvent = <-changeInComponentNameConfigEvent:

		}
		changeEvent = configEvent

		//if the eventType is Put call updateLogConfig for  componentName or global config
		if changeEvent.ChangeType.EventString() == "Put" {
			logLevel := c.updateLogConfig()

			//loadAndApplyLogConfig
			c.loadAndApplyLogConfig(logLevel)
		}

	}

	//if the eventType is Delete call propagateLogConfigForClear
	//need to discuss on loading after delete

}

//create logLevel from data retrieved from kvstore
func populateListData(data map[string]*kvstore.KVPair,configType string) map[string]string {
	loglevel := make(map[string]string)
	for _, val := range data {
		key := strings.SplitAfter(val.Key, configType + "/")
		loglevel[key[1]] = getLogLevel(val.Value)
	}
	return loglevel
}

func getLogLevel(val interface{}) string {
	level := fmt.Sprintf("%s", val)
	logLevel := strings.SplitAfter(level, "\\")
	loglevel := strings.Split(logLevel[0], "\"")
	return loglevel[1]
}

//get active loglevel from the zap logger
func getActiveLogLevel() (map[string]string, error) {
	loglevel := make(map[string]string)

	// now do the default log level
	loglevel["default"] = log.IntToString(log.GetDefaultLogLevel())

	// do the per-package log levels
	for _, packageName := range log.GetPackageNames() {
		level, err := log.GetPackageLogLevel(packageName)
		if err != nil {
			return nil, err
		}

		packagename := strings.ReplaceAll(packageName, "/", "#")
		loglevel[packagename] = log.IntToString(level)

	}

	return loglevel, nil
}

//get the global config from kvstore
func (c *ComponentLogController) getGlobalLogConfig() (map[string]string, error) {

	globalData, err := c.globalConfig.RetreiveAll()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	globalloglevel := populateListData(globalData,c.globalConfig.configType.String())

	return globalloglevel, nil
}

//get the component config  from kvstore
func (c *ComponentLogController) getComponentLogConfig() (map[string]string, error) {
	data, err := c.componentNameConfig.RetreiveAll()
	if err != nil {
		return nil, err
	}
	componentLogLevel := populateListData(data,c.componentNameConfig.configType.String())

	return componentLogLevel, nil
}

//updateLogConfig get the global and component loglevel from kvstore
//then create loglevel  for loading
func (c *ComponentLogController) updateLogConfig() map[string]string {
	globalLevel, err := c.getGlobalLogConfig()
	if err != nil {
		log.Error(err)
	}

	componentLevel, err := c.getComponentLogConfig()
	if err != nil {
		log.Error(err)
	}
	logLevel := createCurrentLogLevel(globalLevel, componentLevel)
	return logLevel
}

func (c *ComponentLogController) loadAndApplyLogConfig(logLevel map[string]string) {
	//load the current configuration for component name using Retreive method
	//create hash of loaded configuration using GenerateLogConfigHash
	//if there is previous hash stored, compare the hash to stored hash
	//if there is any change will call UpdateLogLevels

	//generate hash
	currentLogHash := GenerateLogConfigHash(logLevel)

	//will set the activeHash to true and update the logHash
	if c.logHash != currentLogHash {
		UpdateLogLevels(logLevel)
		c.logHash = currentLogHash
	}

}

//call getDefaultLogLevel to get active default log level
func getDefaultLogLevel(logLevel map[string]string) string {

	for key, level := range logLevel {
		if key == "default" {
			return level
		}
	}
	return ""
}

//create loglevel to set loglevel for the component
func createCurrentLogLevel(activeLogLevels, currentLogLevel map[string]string) map[string]string {

	level := getDefaultLogLevel(currentLogLevel)
	for activeKey, activeLevel := range activeLogLevels {
		exist := false
		for currentLogLevelKey, _ := range currentLogLevel {
			if activeKey == currentLogLevelKey {
				exist = true
				break
			}
		}
		if !exist {
			if level != "" {
				activeLevel = level
			}
			currentLogLevel[activeKey] = activeLevel
		}
	}
	return currentLogLevel
}

//updateLogLevels update the loglevels for the component
func UpdateLogLevels(logLevel map[string]string) {
	//it should retrieve active confguration from logger
	//it should compare with new entries one by one and apply if any changes

	activeLogLevels, _ := getActiveLogLevel()
	currentLogLevel := createCurrentLogLevel(activeLogLevels, logLevel)
	for key, level := range currentLogLevel {
		if key == "default" {
			log.SetDefaultLogLevel(log.StringToInt(level))
		} else {
			pname := strings.ReplaceAll(key, "#", "/")
			log.SetPackageLogLevel(pname, log.StringToInt(level))
		}
	}
}

//generate hash to check for any changes happened to current and active loglevel
func GenerateLogConfigHash(createHashLog map[string]string) [16]byte {
	//it will generate md5 hash of key value pairs appended into a single string
	//in order by key name
	createHashLogBytes := []byte{}
	levelData, _ := json.Marshal(createHashLog)
	createHashLogBytes = append(createHashLogBytes, levelData...)
	return md5.Sum(createHashLogBytes)
}
