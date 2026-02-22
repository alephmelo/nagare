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

// DockerProviderConfig holds settings for the Docker-based cloud provider.
// This provider is primarily used for local development and testing of the
// autoscaler without requiring real cloud credentials.
type DockerProviderConfig struct {
	// Image is the Docker image that contains the nagare binary.
	// The image must have nagare installed at /usr/local/bin/nagare or on PATH.
	// Example: "nagare:latest"
	Image string `yaml:"image"`

	// Network is the Docker network mode for spawned worker containers.
	// Use "host" so containers can reach the master on localhost (Linux only).
	// On macOS with Docker Desktop use "bridge" and set MasterAddr to
	// "http://host.docker.internal:8080".
	// Defaults to "host" when omitted.
	Network string `yaml:"network"`

	// MasterAddr overrides the master address passed to spawned worker
	// containers via --join.  When empty the address is derived from the
	// master's own listen address (e.g. http://localhost:8080), which works
	// on Linux with network=host but not on macOS with Docker Desktop.
	// Set to "http://host.docker.internal:8080" when using bridge networking
	// on macOS.
	MasterAddr string `yaml:"master_addr"`
}

// AWSProviderConfig holds settings for the AWS EC2-based cloud provider.
type AWSProviderConfig struct {
	// Region is the AWS region in which to launch instances (e.g. "us-east-1").
	Region string `yaml:"region"`

	// InstanceType is the default EC2 instance type (e.g. "t3.medium").
	InstanceType string `yaml:"instance_type"`

	// AMIID is the Amazon Machine Image to use.  The AMI must have the nagare
	// binary pre-installed.  When empty, the provider falls back to a
	// user-data bootstrap script that downloads the binary at boot time.
	AMIID string `yaml:"ami_id"`

	// KeyName is the EC2 key pair name for SSH access (optional).
	KeyName string `yaml:"key_name"`

	// SecurityGroup is the security group ID to attach to instances.
	SecurityGroup string `yaml:"security_group"`

	// SubnetID is the VPC subnet in which to launch instances.
	SubnetID string `yaml:"subnet_id"`

	// GPUInstanceType is the EC2 instance type used when a worker pool
	// requires GPU resources (e.g. "g4dn.xlarge").  Falls back to
	// InstanceType when unset.
	GPUInstanceType string `yaml:"gpu_instance_type"`

	// IAMInstanceProfile is the IAM instance profile name or ARN to attach.
	// Required if the nagare binary needs to interact with other AWS services.
	IAMInstanceProfile string `yaml:"iam_instance_profile"`

	// NagareDownloadURL is an HTTPS URL from which the user-data script
	// downloads the nagare binary when no pre-baked AMI is used.
	NagareDownloadURL string `yaml:"nagare_download_url"`
}

// AutoscalerConfig holds all settings governing automatic cloud worker
// provisioning.  When Enabled is false the autoscaler is a no-op and no
// cloud API calls are ever made.
type AutoscalerConfig struct {
	// Enabled controls whether the autoscaler is active.
	// Default: false (opt-in feature).
	Enabled bool `yaml:"enabled"`

	// Provider selects the cloud backend.  Supported values:
	//   "docker" — local Docker daemon (dev/test)
	//   "aws"    — AWS EC2
	Provider string `yaml:"provider"`

	// MaxCloudWorkers is a hard cap on the number of cloud-provisioned workers
	// that may be alive at any one time, across all pools.
	// Default: 5
	MaxCloudWorkers int `yaml:"max_cloud_workers"`

	// ScaleUpThreshold is the number of queued tasks in a pool that triggers
	// a new cloud worker to be provisioned for that pool.
	// Default: 3
	ScaleUpThreshold int `yaml:"scale_up_threshold"`

	// ScaleDownIdleMins is how many minutes a cloud worker must be idle
	// (no tasks executed) before it is eligible for termination.
	// Default: 10
	ScaleDownIdleMins int `yaml:"scale_down_idle_mins"`

	// CooldownSecs is the minimum number of seconds between successive
	// scale-up decisions for the same pool.  Prevents flapping when tasks
	// arrive in bursts.
	// Default: 60
	CooldownSecs int `yaml:"cooldown_secs"`

	// Docker holds Docker-specific provider settings.
	Docker DockerProviderConfig `yaml:"docker"`

	// AWS holds AWS EC2-specific provider settings.
	AWS AWSProviderConfig `yaml:"aws"`
}

// Config is the top-level configuration structure loaded from nagare.yaml.
type Config struct {
	WorkerPools map[string]int   `yaml:"worker_pools"`
	CORS        CORSConfig       `yaml:"cors"`
	Autoscaler  AutoscalerConfig `yaml:"autoscaler"`

	// APIKey is the shared secret that protects all /api/* routes (except
	// /api/webhooks/).  Empty means no authentication is enforced.
	APIKey string `yaml:"api_key"`
}

// LoadConfig reads and parses the YAML config at path.  If the file does not
// exist, sensible defaults are returned without error — this preserves
// backward compatibility for setups without a nagare.yaml.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return defaultConfig(), nil
		}
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	applyDefaults(&cfg)
	return &cfg, nil
}

// defaultConfig returns a Config populated with built-in defaults.
func defaultConfig() *Config {
	c := &Config{
		WorkerPools: map[string]int{"default": 4},
	}
	applyDefaults(c)
	return c
}

// applyDefaults fills in zero-value fields with sensible built-in defaults.
// Called both after YAML unmarshalling and when building the default config.
func applyDefaults(cfg *Config) {
	// Worker pools.
	if len(cfg.WorkerPools) == 0 {
		cfg.WorkerPools = map[string]int{"default": 4}
	} else if _, ok := cfg.WorkerPools["default"]; !ok {
		cfg.WorkerPools["default"] = 4
	}

	// Autoscaler thresholds — only apply when the section is present but
	// individual fields are left at their zero values.
	a := &cfg.Autoscaler
	if a.MaxCloudWorkers == 0 {
		a.MaxCloudWorkers = 5
	}
	if a.ScaleUpThreshold == 0 {
		a.ScaleUpThreshold = 3
	}
	if a.ScaleDownIdleMins == 0 {
		a.ScaleDownIdleMins = 10
	}
	if a.CooldownSecs == 0 {
		a.CooldownSecs = 60
	}
	if a.Docker.Network == "" {
		a.Docker.Network = "host"
	}
}
