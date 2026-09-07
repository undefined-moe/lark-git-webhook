package config

import (
	"strings"
	"testing"
	"time"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GITHUB_WEBHOOK_SECRET", "github")
	t.Setenv("LARK_APP_ID", "cli_test")
	t.Setenv("LARK_APP_SECRET", "lark-secret")
	t.Setenv("LARK_CHAT_ID", "oc_test")
	t.Setenv("LARK_VERIFICATION_TOKEN", "verification-token")
	t.Setenv("DATA_DIR", t.TempDir())
}

func TestDefaultListenAddressIsLoopback(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("ADMIN_LISTEN_ADDR", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "127.0.0.1:8080" || cfg.AdminListenAddr != "127.0.0.1:9090" {
		t.Fatalf("listen addresses webhook=%q admin=%q", cfg.ListenAddr, cfg.AdminListenAddr)
	}
}

func TestMaxMindCityDBPathIsOptional(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxMindCityDBPath != "" {
		t.Fatalf("default MaxMind path=%q", cfg.MaxMindCityDBPath)
	}
	t.Setenv("MAXMIND_CITY_DB_PATH", " /private/GeoLite2-City.mmdb ")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxMindCityDBPath != "/private/GeoLite2-City.mmdb" {
		t.Fatalf("MaxMind path=%q", cfg.MaxMindCityDBPath)
	}
}

func TestGitHubAuditConfiguration(t *testing.T) {
	for _, tc := range []struct {
		org, token, interval string
		want                 bool
	}{
		{"", "", "", true}, {"example-org", "test-token", "10s", true},
		{"example-org", "", "", false}, {"", "test-token", "", false},
		{"bad_org", "test-token", "1m", false}, {"-bad", "test-token", "1m", false}, {"example-org", "test-token", "9s", false},
	} {
		setRequiredEnv(t)
		t.Setenv("GITHUB_AUDIT_ORG", tc.org)
		t.Setenv("GITHUB_AUDIT_TOKEN", tc.token)
		t.Setenv("GITHUB_AUDIT_POLL_INTERVAL", tc.interval)
		cfg, err := Load()
		if tc.want && err != nil {
			t.Fatal(err)
		}
		if !tc.want && err == nil {
			t.Fatalf("accepted audit config %#v", tc)
		}
		if tc.want && tc.org != "" && cfg.GitHubAuditPollInterval != 10*time.Second {
			t.Fatalf("interval=%s", cfg.GitHubAuditPollInterval)
		}
	}
}

func TestLarkAppConfigurationIsRequired(t *testing.T) {
	setRequiredEnv(t)
	for _, name := range []string{"GITHUB_WEBHOOK_SECRET", "LARK_APP_ID", "LARK_APP_SECRET", "LARK_CHAT_ID", "LARK_VERIFICATION_TOKEN"} {
		t.Setenv(name, "")
		if _, err := Load(); err == nil {
			t.Fatalf("accepted empty %s", name)
		}
		setRequiredEnv(t)
	}
}

func TestLarkChatIDFitsMessageSizeReserve(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "plain boundary", value: strings.Repeat("x", 300), want: true},
		{name: "escaped boundary", value: strings.Repeat("\\", 150), want: true},
		{name: "plain over budget", value: strings.Repeat("x", 301)},
		{name: "escape-heavy over budget", value: strings.Repeat("\\", 151)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("LARK_CHAT_ID", tc.value)
			_, err := Load()
			if tc.want && err != nil {
				t.Fatal(err)
			}
			if !tc.want && (err == nil || !strings.Contains(err.Error(), "LARK_CHAT_ID JSON-encoded value exceeds")) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestArchiveReserveAndWebhookConcurrencyDefaultsAndValidation(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ArchiveMinFreeBytes != 1<<30 || cfg.WebhookMaxInFlight != 8 {
		t.Fatalf("archive reserve/concurrency = %d/%d", cfg.ArchiveMinFreeBytes, cfg.WebhookMaxInFlight)
	}
	for _, setting := range []struct{ name, value string }{{"ARCHIVE_MIN_FREE_BYTES", "0"}, {"WEBHOOK_MAX_IN_FLIGHT", "0"}} {
		t.Setenv(setting.name, setting.value)
		if _, err := Load(); err == nil {
			t.Fatalf("accepted %s=%s", setting.name, setting.value)
		}
		t.Setenv(setting.name, "")
	}
}

func TestAdminListenAddressMustBeLoopback(t *testing.T) {
	for _, address := range []string{"0.0.0.0:9090", "192.0.2.1:9090", "example.com:9090"} {
		if loopbackAddress(address) {
			t.Fatalf("accepted non-loopback %q", address)
		}
	}
	for _, address := range []string{"localhost:9090", "127.0.0.1:9090", "[::1]:9090"} {
		if !loopbackAddress(address) {
			t.Fatalf("rejected loopback %q", address)
		}
	}
}

func TestWebhookPathValidation(t *testing.T) {
	for _, value := range []string{"/webhook", "/webhook/github"} {
		if !validWebhookPath(value) {
			t.Fatalf("rejected valid path %q", value)
		}
	}
	for _, value := range []string{"", "/", "webhook", "/webhook/", "/webhook/../admin", "/webhook?x=1", "/webhook#x", "/webhook/{event}", "/webhook/{", "/webhook/%"} {
		if validWebhookPath(value) {
			t.Fatalf("accepted invalid path %q", value)
		}
	}
}

func TestLarkCallbackPathsDefaultAndCannotCollide(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LarkEventPath != "/webhook/lark/event" || cfg.LarkCallbackPath != "/webhook/lark/callback" {
		t.Fatalf("Lark paths = %q, %q", cfg.LarkEventPath, cfg.LarkCallbackPath)
	}
	for _, paths := range []struct{ webhook, event, callback string }{
		{"/", "/webhook/lark/event", "/webhook/lark/callback"},
		{"/webhook", "/", "/webhook/lark/callback"},
		{"/webhook", "/webhook/lark/event", "/"},
		{"/same", "/same", "/webhook/lark/callback"},
		{"/webhook", "/same", "/same"},
	} {
		t.Setenv("WEBHOOK_PATH", paths.webhook)
		t.Setenv("LARK_EVENT_PATH", paths.event)
		t.Setenv("LARK_CALLBACK_PATH", paths.callback)
		if _, err := Load(); err == nil {
			t.Fatalf("accepted paths %#v", paths)
		}
	}
}
