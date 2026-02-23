package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "nagare-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return f.Name()
}

func TestLoadConfig_APIKey(t *testing.T) {
	path := writeYAML(t, `
worker_pools:
  default: 2
api_key: "super-secret-key"
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.APIKey != "super-secret-key" {
		t.Errorf("expected APIKey %q, got %q", "super-secret-key", cfg.APIKey)
	}
}

func TestLoadConfig_APIKeyEmpty(t *testing.T) {
	path := writeYAML(t, `
worker_pools:
  default: 2
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.APIKey != "" {
		t.Errorf("expected empty APIKey, got %q", cfg.APIKey)
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig with missing file should not error, got: %v", err)
	}
	if cfg.APIKey != "" {
		t.Errorf("expected empty APIKey for default config, got %q", cfg.APIKey)
	}
}

func TestLoadConfig_AutoscalerDefaults(t *testing.T) {
	// A config with no autoscaler section should produce sensible defaults.
	path := writeYAML(t, `worker_pools:
  default: 2
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Autoscaler.Enabled {
		t.Error("autoscaler should be disabled by default")
	}
	if cfg.Autoscaler.Provider != "" {
		t.Errorf("expected empty provider, got %q", cfg.Autoscaler.Provider)
	}
}

func TestLoadConfig_AutoscalerFull(t *testing.T) {
	path := writeYAML(t, `
worker_pools:
  default: 2
  gpu_workers: 1

autoscaler:
  enabled: true
  provider: docker
  max_cloud_workers: 10
  scale_up_threshold: 3
  scale_down_idle_mins: 5
  cooldown_secs: 60
  docker:
    image: "nagare:latest"
    network: "host"
  aws:
    region: us-east-1
    instance_type: t3.medium
    ami_id: ami-12345
    key_name: my-key
    security_group: sg-123
    subnet_id: subnet-456
    gpu_instance_type: g4dn.xlarge
    iam_instance_profile: nagare-worker-role
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	a := cfg.Autoscaler
	if !a.Enabled {
		t.Error("expected autoscaler enabled")
	}
	if a.Provider != "docker" {
		t.Errorf("expected provider %q, got %q", "docker", a.Provider)
	}
	if a.MaxCloudWorkers != 10 {
		t.Errorf("expected max_cloud_workers 10, got %d", a.MaxCloudWorkers)
	}
	if a.ScaleUpThreshold != 3 {
		t.Errorf("expected scale_up_threshold 3, got %d", a.ScaleUpThreshold)
	}
	if a.ScaleDownIdleMins != 5 {
		t.Errorf("expected scale_down_idle_mins 5, got %d", a.ScaleDownIdleMins)
	}
	if a.CooldownSecs != 60 {
		t.Errorf("expected cooldown_secs 60, got %d", a.CooldownSecs)
	}
	if a.Docker.Image != "nagare:latest" {
		t.Errorf("expected docker image %q, got %q", "nagare:latest", a.Docker.Image)
	}
	if a.Docker.Network != "host" {
		t.Errorf("expected docker network %q, got %q", "host", a.Docker.Network)
	}
	if a.AWS.Region != "us-east-1" {
		t.Errorf("expected AWS region %q, got %q", "us-east-1", a.AWS.Region)
	}
	if a.AWS.InstanceType != "t3.medium" {
		t.Errorf("expected instance type %q, got %q", "t3.medium", a.AWS.InstanceType)
	}
	if a.AWS.AMIID != "ami-12345" {
		t.Errorf("expected AMI ID %q, got %q", "ami-12345", a.AWS.AMIID)
	}
	if a.AWS.GPUInstanceType != "g4dn.xlarge" {
		t.Errorf("expected GPU instance type %q, got %q", "g4dn.xlarge", a.AWS.GPUInstanceType)
	}
	if a.AWS.IAMInstanceProfile != "nagare-worker-role" {
		t.Errorf("expected IAM profile %q, got %q", "nagare-worker-role", a.AWS.IAMInstanceProfile)
	}
}

func TestLoadConfig_AWSProfile(t *testing.T) {
	path := writeYAML(t, `
autoscaler:
  enabled: true
  provider: aws
  aws:
    region: us-east-1
    instance_type: t3.nano
    security_group: sg-123
    subnet_id: subnet-456
    profile: staging
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Autoscaler.AWS.Profile != "staging" {
		t.Errorf("expected AWS profile %q, got %q", "staging", cfg.Autoscaler.AWS.Profile)
	}
}

func TestLoadConfig_AWSProfileEmpty(t *testing.T) {
	// When profile is omitted it must be empty string (fall through to default chain).
	path := writeYAML(t, `
autoscaler:
  enabled: true
  provider: aws
  aws:
    region: us-east-1
    instance_type: t3.nano
    security_group: sg-123
    subnet_id: subnet-456
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Autoscaler.AWS.Profile != "" {
		t.Errorf("expected empty AWS profile, got %q", cfg.Autoscaler.AWS.Profile)
	}
}

func TestLoadConfig_AutoscalerDefaultThresholds(t *testing.T) {
	// When autoscaler is enabled but thresholds are omitted, defaults are applied.
	path := writeYAML(t, `
autoscaler:
  enabled: true
  provider: docker
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	a := cfg.Autoscaler
	if a.MaxCloudWorkers != 5 {
		t.Errorf("expected default max_cloud_workers 5, got %d", a.MaxCloudWorkers)
	}
	if a.ScaleUpThreshold != 3 {
		t.Errorf("expected default scale_up_threshold 3, got %d", a.ScaleUpThreshold)
	}
	if a.ScaleDownIdleMins != 10 {
		t.Errorf("expected default scale_down_idle_mins 10, got %d", a.ScaleDownIdleMins)
	}
	if a.CooldownSecs != 60 {
		t.Errorf("expected default cooldown_secs 60, got %d", a.CooldownSecs)
	}
}
