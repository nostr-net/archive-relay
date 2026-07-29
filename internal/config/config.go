// Package config loads the YAML configuration for the archive relay.
package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Relay      Relay      `yaml:"relay"`
	ClickHouse ClickHouse `yaml:"clickhouse"`
	SQLite     SQLite     `yaml:"sqlite"`
	Batch      Batch      `yaml:"batch"`
	Retention  Retention  `yaml:"retention"`
	// Classifier optionally overrides the built-in kind→tier map.
	// Keys are kind numbers, values are tier names: permanent|archive|social|transient|drop.
	Classifier map[int]string `yaml:"classifier"`

	// Policy caps the breadth of inbound REQ/COUNT filters so a hostile client
	// can't force huge scans. A zero field means "no limit" for that field.
	Policy Policy `yaml:"policy"`
}

type Relay struct {
	Addr       string `yaml:"addr"`       // e.g. ":3334"
	ServiceURL string `yaml:"serviceURL"` // canonical URL for NIP-42, optional
}

type ClickHouse struct {
	Addr     string `yaml:"addr"` // host:port, native protocol (default :9000)
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type SQLite struct {
	Path string `yaml:"path"` // e.g. "./control.db"
}

type Batch struct {
	MaxSize int           `yaml:"maxSize"` // flush after this many events
	MaxAge  time.Duration `yaml:"maxAge"`  // flush after this duration
}

// Retention sets the per-tier TTL delete intervals. Zero = keep forever.
type Retention struct {
	Permanent string `yaml:"permanent"` // e.g. "" (forever)
	Archive   string `yaml:"archive"`   // e.g. "10 YEAR"
	Social    string `yaml:"social"`    // e.g. "1 YEAR"
	Transient string `yaml:"transient"` // e.g. "30 DAY"
}

// Policy sets inbound filter-breadth limits enforced at the relay hooks. A zero
// field disables that specific limit. Loaded from the `policy` YAML block.
type Policy struct {
	MaxIDs     int `yaml:"maxIDs"`     // max IDs per filter
	MaxAuthors int `yaml:"maxAuthors"` // max authors per filter
	MaxKinds   int `yaml:"maxKinds"`   // max kinds per filter
	MaxTags    int `yaml:"maxTags"`    // max total tag values per filter
}

// Load reads and parses the config file, applying defaults.
func Load(path string) (*Config, error) {
	c := defaults()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func defaults() *Config {
	return &Config{
		Relay:      Relay{Addr: ":3334"},
		ClickHouse: ClickHouse{Addr: "localhost:9000", Database: "archive_relay", Username: "default"},
		SQLite:     SQLite{Path: "./control.db"},
		// Production-tuned: at firehose rates a 1s flush creates ~1 part/sec (the
		// ClickHouse small-inserts anti-pattern). 5s/5000 coalesces into few,
		// large parts. Dev/tests override these explicitly.
		Batch:     Batch{MaxSize: 5000, MaxAge: 5 * time.Second},
		Retention: Retention{Archive: "10 YEAR", Social: "1 YEAR", Transient: "30 DAY"},
		Policy:    Policy{MaxIDs: 1000, MaxAuthors: 1000, MaxKinds: 20, MaxTags: 256},
	}
}
