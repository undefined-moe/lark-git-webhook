package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/config"
	"github.com/undefined-moe/lark-git-webhook/internal/forwarder"
	"github.com/undefined-moe/lark-git-webhook/internal/github"
	"github.com/undefined-moe/lark-git-webhook/internal/gitlabwebhook"
	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/larkcallback"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
	"github.com/undefined-moe/lark-git-webhook/internal/webhook"
)

func TestWebhookMuxIsolatesConfiguredPublicRoutes(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		WebhookPath:           "/webhook/github",
		GitLabWebhookPath:     "/webhook/gitlab",
		LarkEventPath:         "/webhook/lark/event",
		LarkCallbackPath:      "/webhook/lark/callback",
		GitHubSecret:          "secret",
		GitLabSecret:          "gitlab-secret",
		LarkVerificationToken: "verification-token",
		LarkAppID:             "cli_test",
		MaxBodyBytes:          1024,
		DedupeTTL:             time.Hour,
		WebhookMaxInFlight:    8,
	}
	mux := webhookMux(
		webhook.New(s, a, cfg.GitHubSecret, cfg.LarkChatID, cfg.MaxBodyBytes, cfg.DedupeTTL, cfg.WebhookMaxInFlight, &metrics.Metrics{}),
		gitlabwebhook.New(s, a, cfg.GitLabSecret, cfg.LarkChatID, cfg.MaxBodyBytes, cfg.DedupeTTL, cfg.WebhookMaxInFlight, &metrics.Metrics{}),
		larkcallback.New(s, a, cfg.LarkVerificationToken, cfg.LarkAppID, cfg.MaxBodyBytes, cfg.DedupeTTL, cfg.WebhookMaxInFlight, github.New(), nil),
		cfg,
	)
	for _, path := range []string{"/webhook", "/webhook/lark", "/gitlab/webhook"} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
	github := httptest.NewRecorder()
	mux.ServeHTTP(github, httptest.NewRequest(http.MethodPost, cfg.WebhookPath, strings.NewReader(`{}`)))
	if github.Code != http.StatusUnauthorized {
		t.Fatalf("GitHub route status=%d", github.Code)
	}
	gitlab := httptest.NewRecorder()
	mux.ServeHTTP(gitlab, httptest.NewRequest(http.MethodPost, cfg.GitLabWebhookPath, strings.NewReader(`{}`)))
	if gitlab.Code != http.StatusUnauthorized {
		t.Fatalf("GitLab route status=%d", gitlab.Code)
	}
	gitlabAuthed := httptest.NewRequest(http.MethodPost, cfg.GitLabWebhookPath, strings.NewReader(`{"object_kind":"push"}`))
	gitlabAuthed.Header.Set("X-Gitlab-Token", cfg.GitLabSecret)
	authed := httptest.NewRecorder()
	mux.ServeHTTP(authed, gitlabAuthed)
	if authed.Code != http.StatusBadRequest {
		t.Fatalf("GitLab authed route without event status=%d", authed.Code)
	}
	for _, path := range []string{cfg.LarkEventPath, cfg.LarkCallbackPath} {
		lark := httptest.NewRecorder()
		mux.ServeHTTP(lark, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		if lark.Code != http.StatusBadRequest {
			t.Fatalf("Lark route %s status=%d", path, lark.Code)
		}
	}
}

func TestRecoverPendingArchiveDeliveries(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := s.Admit("archive-only", "push", []byte(`{"id":1}`), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := recoverPending(a, s, logger); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Admit("queued-no-marker", "push", []byte(`{"id":2}`), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Store("push", "queued-no-marker", []byte(`{"id":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := recoverPending(a, s, logger); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 2 {
		t.Fatalf("items=%d err=%v", len(items), err)
	}
	pending, err := s.PendingArchives(10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
}

func TestOperationalEndpoints(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := &metrics.Metrics{}
	m.Received()
	worker := forwarder.NewWorker(s, func(context.Context, lark.Message) error { return nil }, m, slog.New(slog.NewTextHandler(io.Discard, nil)), 0, 10, 15360, time.Second, time.Minute, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)
	deadline := time.Now().Add(time.Second)
	for !worker.Alive(time.Now()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	mux := adminMux(s, a, worker, m, config.Config{QueueMaxItems: 10, QueueMaxBytes: 1024, DeadMaxItems: 10, DeadMaxBytes: 1024, ArchiveMinFreeBytes: 1})
	for _, path := range []string{"/livez", "/readyz"} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
	lowSpaceMux := adminMux(s, a, worker, m, config.Config{QueueMaxItems: 10, QueueMaxBytes: 1 << 20, DeadMaxItems: 10, DeadMaxBytes: 1 << 20, ArchiveMinFreeBytes: ^uint64(0)})
	lowSpace := httptest.NewRecorder()
	lowSpaceMux.ServeHTTP(lowSpace, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if lowSpace.Code != http.StatusServiceUnavailable {
		t.Fatalf("low-space readiness status=%d", lowSpace.Code)
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "lark_git_webhook_dead_letter_items") {
		t.Fatalf("metrics=%d %s", response.Code, response.Body.String())
	}
}
