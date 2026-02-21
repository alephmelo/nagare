package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WorkerPools map[string]int `yaml:"worker_pools"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Return default config if file doesn't exist
			return &Config{
				WorkerPools: map[string]int{"default": 4},
			}, nil
		}
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	// Ensure default pool exists
	if len(cfg.WorkerPools) == 0 {
		cfg.WorkerPools = map[string]int{"default": 4}
	} else if _, ok := cfg.WorkerPools["default"]; !ok {
		cfg.WorkerPools["default"] = 4
	}

	return &cfg, nil
}
