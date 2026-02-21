package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// CORSConfig holds cross-origin resource sharing settings.
type CORSConfig struct {
	// AllowedOrigins is an explicit list of origins that may make cross-origin
	// requests.  If empty, the server falls back to the wildcard "*" and logs a
	// warning at startup.
	AllowedOrigins []string `yaml:"allowed_origins"`
}

type Config struct {
	WorkerPools map[string]int `yaml:"worker_pools"`
	CORS        CORSConfig     `yaml:"cors"`
	// APIKey is the shared secret that protects all /api/* routes (except
	// /api/webhooks/).  Empty means no authentication is enforced.
	APIKey string `yaml:"api_key"`
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
