package config

import (
	"gopkg.in/ini.v1"
	"os"
)

var cfg *ini.File

func LoadConfig() (cfg *ini.File, err error) {
	configPath := "./config.ini"
	// 检查是否有ENV配置
	envConfig := os.Getenv("FU_APP_CONFIG")
	if envConfig != "" {
		configPath = envConfig
	}
	cfg, err = ini.Load(configPath)
	return
}
