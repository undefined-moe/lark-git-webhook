package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GitHubSecret            string
	GitLabSecret            string
	GitHubAuditOrg          string
	GitHubAuditToken        string
	GitHubAuditPollInterval time.Duration
	LarkAppID               string
	LarkAppSecret           string
	LarkChatID              string
	LarkVerificationToken   string
	ListenAddr              string
	AdminListenAddr         string
	WebhookPath             string
	GitLabWebhookPath       string
	LarkEventPath           string
	LarkCallbackPath        string
	DataDir                 string
	MaxMindCityDBPath       string
	MaxBodyBytes            int64
	WebhookMaxInFlight      int
	ArchiveMinFreeBytes     uint64
	QueueMaxItems           uint64
	QueueMaxBytes           uint64
	DedupeTTL               time.Duration
	DedupeMaxItems          uint64
	DeadMaxItems            uint64
	DeadMaxBytes            uint64
	BatchWindow             time.Duration
	BatchMaxEvents          int
	LarkMaxMessageBytes     int
	LarkHTTPTimeout         time.Duration
	RetryBaseDelay          time.Duration
	RetryMaxDelay           time.Duration
	MaxAttempts             int
}

func Load() (Config, error) {
	c := Config{
		GitHubSecret:            os.Getenv("GITHUB_WEBHOOK_SECRET"),
		GitLabSecret:            strings.TrimSpace(os.Getenv("GITLAB_WEBHOOK_SECRET")),
		GitHubAuditOrg:          strings.TrimSpace(os.Getenv("GITHUB_AUDIT_ORG")),
		GitHubAuditToken:        os.Getenv("GITHUB_AUDIT_TOKEN"),
		GitHubAuditPollInterval: time.Minute,
		LarkAppID:               os.Getenv("LARK_APP_ID"),
		LarkAppSecret:           os.Getenv("LARK_APP_SECRET"),
		LarkChatID:              os.Getenv("LARK_CHAT_ID"),
		LarkVerificationToken:   os.Getenv("LARK_VERIFICATION_TOKEN"),
		ListenAddr:              value("LISTEN_ADDR", "127.0.0.1:8080"),
		AdminListenAddr:         value("ADMIN_LISTEN_ADDR", "127.0.0.1:9090"),
		WebhookPath:             value("WEBHOOK_PATH", "/webhook"),
		GitLabWebhookPath:       value("GITLAB_WEBHOOK_PATH", "/gitlab/webhook"),
		LarkEventPath:           value("LARK_EVENT_PATH", "/webhook/lark/event"),
		LarkCallbackPath:        value("LARK_CALLBACK_PATH", "/webhook/lark/callback"),
		DataDir:                 value("DATA_DIR", "/data"),
		MaxMindCityDBPath:       strings.TrimSpace(os.Getenv("MAXMIND_CITY_DB_PATH")),
		MaxBodyBytes:            10 << 20,
		WebhookMaxInFlight:      8,
		ArchiveMinFreeBytes:     1 << 30,
		QueueMaxItems:           10000,
		QueueMaxBytes:           128 << 20,
		DedupeTTL:               168 * time.Hour,
		DedupeMaxItems:          100000,
		DeadMaxItems:            10000,
		DeadMaxBytes:            128 << 20,
		BatchWindow:             2 * time.Second,
		BatchMaxEvents:          10,
		LarkMaxMessageBytes:     15360,
		LarkHTTPTimeout:         10 * time.Second,
		RetryBaseDelay:          time.Second,
		RetryMaxDelay:           time.Minute,
		MaxAttempts:             10,
	}
	if c.GitHubSecret == "" || c.LarkAppID == "" || c.LarkAppSecret == "" || c.LarkChatID == "" || c.LarkVerificationToken == "" {
		return Config{}, errors.New("GITHUB_WEBHOOK_SECRET, LARK_APP_ID, LARK_APP_SECRET, LARK_CHAT_ID, and LARK_VERIFICATION_TOKEN are required")
	}
	encodedChatID, _ := json.Marshal(c.LarkChatID)
	if len(encodedChatID)-len(`""`) > 300 {
		return Config{}, errors.New("LARK_CHAT_ID JSON-encoded value exceeds the 300-byte Lark message-size reserve")
	}
	var err error
	if c.GitHubAuditPollInterval, err = durationEnv("GITHUB_AUDIT_POLL_INTERVAL", c.GitHubAuditPollInterval); err != nil {
		return Config{}, err
	}
	if c.MaxBodyBytes, err = int64Env("MAX_BODY_BYTES", c.MaxBodyBytes); err != nil {
		return Config{}, err
	}
	if c.WebhookMaxInFlight, err = intEnv("WEBHOOK_MAX_IN_FLIGHT", c.WebhookMaxInFlight); err != nil {
		return Config{}, err
	}
	if c.ArchiveMinFreeBytes, err = uint64Env("ARCHIVE_MIN_FREE_BYTES", c.ArchiveMinFreeBytes); err != nil {
		return Config{}, err
	}
	if c.QueueMaxItems, err = uint64Env("QUEUE_MAX_ITEMS", c.QueueMaxItems); err != nil {
		return Config{}, err
	}
	if c.QueueMaxBytes, err = uint64Env("QUEUE_MAX_BYTES", c.QueueMaxBytes); err != nil {
		return Config{}, err
	}
	if c.DedupeTTL, err = durationEnv("DEDUPE_TTL", c.DedupeTTL); err != nil {
		return Config{}, err
	}
	if c.DedupeMaxItems, err = uint64Env("DEDUPE_MAX_ITEMS", c.DedupeMaxItems); err != nil {
		return Config{}, err
	}
	if c.DeadMaxItems, err = uint64Env("DEAD_MAX_ITEMS", c.DeadMaxItems); err != nil {
		return Config{}, err
	}
	if c.DeadMaxBytes, err = uint64Env("DEAD_MAX_BYTES", c.DeadMaxBytes); err != nil {
		return Config{}, err
	}
	if c.BatchWindow, err = durationEnv("BATCH_WINDOW", c.BatchWindow); err != nil {
		return Config{}, err
	}
	if c.BatchMaxEvents, err = intEnv("BATCH_MAX_EVENTS", c.BatchMaxEvents); err != nil {
		return Config{}, err
	}
	if c.LarkMaxMessageBytes, err = intEnv("LARK_MAX_MESSAGE_BYTES", c.LarkMaxMessageBytes); err != nil {
		return Config{}, err
	}
	if c.LarkHTTPTimeout, err = durationEnv("LARK_HTTP_TIMEOUT", c.LarkHTTPTimeout); err != nil {
		return Config{}, err
	}
	if c.RetryBaseDelay, err = durationEnv("RETRY_BASE_DELAY", c.RetryBaseDelay); err != nil {
		return Config{}, err
	}
	if c.RetryMaxDelay, err = durationEnv("RETRY_MAX_DELAY", c.RetryMaxDelay); err != nil {
		return Config{}, err
	}
	if c.MaxAttempts, err = intEnv("MAX_ATTEMPTS", c.MaxAttempts); err != nil {
		return Config{}, err
	}
	if c.MaxBodyBytes <= 0 || c.WebhookMaxInFlight <= 0 || c.ArchiveMinFreeBytes == 0 || c.QueueMaxItems == 0 || c.QueueMaxBytes == 0 || c.DedupeTTL <= 0 || c.DedupeMaxItems == 0 || c.DeadMaxItems == 0 || c.DeadMaxBytes == 0 || c.BatchWindow < 0 || c.BatchMaxEvents <= 0 || c.LarkMaxMessageBytes < 512 || c.LarkHTTPTimeout <= 0 || c.RetryBaseDelay <= 0 || c.RetryMaxDelay < c.RetryBaseDelay || c.MaxAttempts <= 0 {
		return Config{}, errors.New("configuration limits must be valid, archive free-space reserve and webhook concurrency must be positive, Lark message size must be at least 512 bytes, and retry maximum must not be lower than retry base")
	}
	if (c.GitHubAuditOrg == "") != (c.GitHubAuditToken == "") {
		return Config{}, errors.New("GITHUB_AUDIT_ORG and GITHUB_AUDIT_TOKEN must be provided together")
	}
	if c.GitHubAuditOrg != "" && (!validGitHubOrg(c.GitHubAuditOrg) || c.GitHubAuditPollInterval < 10*time.Second) {
		return Config{}, errors.New("GITHUB_AUDIT_ORG must be a valid GitHub organization and GITHUB_AUDIT_POLL_INTERVAL must be at least 10s")
	}
	if !loopbackAddress(c.AdminListenAddr) {
		return Config{}, errors.New("ADMIN_LISTEN_ADDR must use localhost or a loopback IP")
	}
	if !validWebhookPath(c.WebhookPath) || !validWebhookPath(c.GitLabWebhookPath) || !validWebhookPath(c.LarkEventPath) || !validWebhookPath(c.LarkCallbackPath) {
		return Config{}, errors.New("WEBHOOK_PATH, GITLAB_WEBHOOK_PATH, LARK_EVENT_PATH, and LARK_CALLBACK_PATH must be clean non-root absolute paths without query or fragment")
	}
	if c.WebhookPath == c.GitLabWebhookPath || c.WebhookPath == c.LarkEventPath || c.WebhookPath == c.LarkCallbackPath || c.GitLabWebhookPath == c.LarkEventPath || c.GitLabWebhookPath == c.LarkCallbackPath || c.LarkEventPath == c.LarkCallbackPath {
		return Config{}, errors.New("WEBHOOK_PATH, GITLAB_WEBHOOK_PATH, LARK_EVENT_PATH, and LARK_CALLBACK_PATH must be distinct")
	}
	if err := os.MkdirAll(c.DataDir, 0o750); err != nil {
		return Config{}, fmt.Errorf("create data directory: %w", err)
	}
	probe := filepath.Join(c.DataDir, ".write-probe")
	if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		return Config{}, fmt.Errorf("data directory is not writable: %w", err)
	}
	if err := os.Remove(probe); err != nil {
		return Config{}, fmt.Errorf("remove data directory probe: %w", err)
	}
	return c, nil
}

func validGitHubOrg(org string) bool {
	if len(org) == 0 || len(org) > 39 || org[0] == '-' || org[len(org)-1] == '-' {
		return false
	}
	for _, r := range org {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return false
	}
	return true
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validWebhookPath(value string) bool {
	if value == "/" || strings.HasSuffix(value, "/") || !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "?#{}") || pathpkg.Clean(value) != value {
		return false
	}
	_, err := url.ParseRequestURI(value)
	return err == nil
}

func value(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}
func intEnv(name string, fallback int) (int, error) {
	v, err := int64Env(name, int64(fallback))
	return int(v), err
}
func int64Env(name string, fallback int64) (int64, error) {
	if os.Getenv(name) == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}
func uint64Env(name string, fallback uint64) (uint64, error) {
	if os.Getenv(name) == "" {
		return fallback, nil
	}
	v, err := strconv.ParseUint(os.Getenv(name), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}
func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	if os.Getenv(name) == "" {
		return fallback, nil
	}
	v, err := time.ParseDuration(os.Getenv(name))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}
