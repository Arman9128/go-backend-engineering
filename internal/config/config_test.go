package config

import "testing"

func setTestEnv(t *testing.T) {
	t.Helper()

	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "task_user")
	t.Setenv("DB_PASSWORD", "test_password")
	t.Setenv("DB_NAME", "task_platform")
	t.Setenv("DB_SSLMODE", "disable")
}

func TestLoad(t *testing.T) {
	setTestEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.DBHost != "localhost" {
		t.Errorf("expected localhost, got %s", cfg.DBHost)
	}

	if cfg.DBPort != 5432 {
		t.Errorf("expected port 5432, got %d", cfg.DBPort)
	}

	if cfg.DBUser != "task_user" {
		t.Errorf("expected task_user, got %s", cfg.DBUser)
	}

	if cfg.DBName != "task_platform" {
		t.Errorf("expected task_platform, got %s", cfg.DBName)
	}
}

func TestLoadInvalidPort(t *testing.T) {
	setTestEnv(t)
	t.Setenv("DB_PORT", "invalid")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid DB_PORT")
	}
}

func TestLoadMissingHost(t *testing.T) {
	setTestEnv(t)
	t.Setenv("DB_HOST", "")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing DB_HOST")
	}
}

func TestLoadMissingPassword(t *testing.T) {
	setTestEnv(t)
	t.Setenv("DB_PASSWORD", "")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing DB_PASSWORD")
	}
}

func TestLoadDefaultHTTPAddr(t *testing.T) {
	setTestEnv(t)
	t.Setenv("HTTP_ADDR", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("expected :8080, got %s", cfg.HTTPAddr)
	}
}

func TestLoadCustomHTTPAddr(t *testing.T) {
	setTestEnv(t)
	t.Setenv("HTTP_ADDR", "127.0.0.1:9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.HTTPAddr != "127.0.0.1:9090" {
		t.Errorf("expected 127.0.0.1:9090, got %s", cfg.HTTPAddr)
	}
}
