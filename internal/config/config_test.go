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
	f.Close()
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
