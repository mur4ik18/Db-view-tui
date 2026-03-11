package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Connection struct {
	Host     string `yaml:"host,omitempty"`
	Port     int    `yaml:"port,omitempty"`
	Database string `yaml:"database,omitempty"`
	User     string `yaml:"user,omitempty"`
	Password string `yaml:"password,omitempty"`
	SSLMode  string `yaml:"sslmode,omitempty"`
	URL      string `yaml:"url,omitempty"`
}

type Config struct {
	Connections map[string]Connection `yaml:"connections"`
	Default     string                `yaml:"default,omitempty"`
}

func Dir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".dbctl"
	}
	return filepath.Join(home, ".dbctl")
}

func Path() string {
	return filepath.Join(Dir(), "config.yaml")
}

func HistoryPath() string {
	return filepath.Join(Dir(), "history")
}

func EnsureDir() error {
	return os.MkdirAll(Dir(), 0o755)
}

func Load() (*Config, error) {
	if err := EnsureDir(); err != nil {
		return nil, err
	}

	cfg := &Config{
		Connections: map[string]Connection{},
	}

	data, err := os.ReadFile(Path())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return nil, err
	}

	if len(data) == 0 {
		return cfg, nil
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.Connections == nil {
		cfg.Connections = map[string]Connection{}
	}

	return cfg, nil
}

func Save(cfg *Config) error {
	if err := EnsureDir(); err != nil {
		return err
	}

	if cfg.Connections == nil {
		cfg.Connections = map[string]Connection{}
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	return os.WriteFile(Path(), data, 0o600)
}

func EnvPassword(name string) string {
	key := "DBCTL_PASS_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
	return os.Getenv(key)
}
