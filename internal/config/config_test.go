package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c := defaults()
	if c.Relay.Addr != ":3334" {
		t.Errorf("default relay.addr = %q, want :3334", c.Relay.Addr)
	}
	if c.ClickHouse.Addr != "localhost:9000" || c.ClickHouse.Database != "archive_relay" {
		t.Errorf("unexpected clickhouse defaults: %+v", c.ClickHouse)
	}
	if c.Batch.MaxSize != 5000 || c.Batch.MaxAge != 5*time.Second {
		t.Errorf("unexpected batch defaults: %+v", c.Batch)
	}
	// production breadth caps are populated by default (not zero/disabled)
	if c.Policy.MaxIDs != 1000 || c.Policy.MaxAuthors != 1000 ||
		c.Policy.MaxKinds != 20 || c.Policy.MaxTags != 256 {
		t.Errorf("unexpected policy defaults: %+v", c.Policy)
	}
}

func TestLoadEmptyPathReturnsDefaults(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Relay.Addr != ":3334" {
		t.Errorf("expected default addr, got %q", c.Relay.Addr)
	}
}

func TestLoadAppliesYAMLOverrides(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := []byte(`
relay: {addr: ":9999", serviceURL: "wss://relay.example.com"}
clickhouse: {addr: "ch:9000", database: "x", username: "u", password: "p"}
sqlite: {path: "/data/c.db"}
batch: {maxSize: 7, maxAge: 250ms}
retention: {permanent: "", archive: "5 YEAR", social: "6 MONTH", transient: "7 DAY"}
policy: {maxIDs: 5, maxAuthors: 6, maxKinds: 7, maxTags: 8}
classifier: {5: permanent}
`)
	if err := os.WriteFile(p, body, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Relay.Addr != ":9999" || c.Relay.ServiceURL != "wss://relay.example.com" {
		t.Errorf("relay override not applied: %+v", c.Relay)
	}
	if c.ClickHouse.Addr != "ch:9000" || c.ClickHouse.Database != "x" ||
		c.ClickHouse.Username != "u" || c.ClickHouse.Password != "p" {
		t.Errorf("clickhouse override not applied: %+v", c.ClickHouse)
	}
	if c.SQLite.Path != "/data/c.db" {
		t.Errorf("sqlite override not applied: %+v", c.SQLite)
	}
	if c.Batch.MaxSize != 7 || c.Batch.MaxAge != 250*time.Millisecond {
		t.Errorf("batch override not applied: %+v", c.Batch)
	}
	if c.Retention.Archive != "5 YEAR" || c.Retention.Social != "6 MONTH" ||
		c.Retention.Transient != "7 DAY" {
		t.Errorf("retention override not applied: %+v", c.Retention)
	}
	if c.Policy.MaxIDs != 5 || c.Policy.MaxAuthors != 6 ||
		c.Policy.MaxKinds != 7 || c.Policy.MaxTags != 8 {
		t.Errorf("policy override not applied: %+v", c.Policy)
	}
	if c.Classifier[5] != "permanent" {
		t.Errorf("classifier override not applied: %+v", c.Classifier)
	}
}

func TestLoadMissingFileErrors(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing config file")
	}
}
