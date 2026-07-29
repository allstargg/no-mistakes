package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestLoadGlobalEventLogRetentionDefaultAndOverride(t *testing.T) {
	cfg, err := LoadGlobal(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EventLogRetention != DefaultEventLogRetention {
		t.Fatalf("default event_log_retention = %v, want %v", cfg.EventLogRetention, DefaultEventLogRetention)
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("event_log_retention: 48h\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadGlobal(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EventLogRetention != 48*time.Hour {
		t.Fatalf("event_log_retention = %v, want 48h", cfg.EventLogRetention)
	}
}

func TestLoadGlobalEventLogRetentionRejectsInvalidOrUnboundedValues(t *testing.T) {
	for _, value := range []string{"nope", "0s", "-1h", "unlimited"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("event_log_retention: \""+value+"\"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadGlobal(path); err == nil {
				t.Fatalf("event_log_retention %q unexpectedly accepted", value)
			}
		})
	}
}

func TestDefaultConfigYAMLEventLogRetentionMatchesGoDefault(t *testing.T) {
	var raw globalConfigRaw
	if err := yaml.Unmarshal([]byte(defaultConfigYAML), &raw); err != nil {
		t.Fatal(err)
	}
	got, err := time.ParseDuration(raw.EventLogRetention)
	if err != nil {
		t.Fatalf("default event_log_retention %q: %v", raw.EventLogRetention, err)
	}
	if got != DefaultEventLogRetention {
		t.Fatalf("YAML event_log_retention = %v, Go default = %v", got, DefaultEventLogRetention)
	}
}
