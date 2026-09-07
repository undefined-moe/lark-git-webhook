package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/audit"
	"github.com/undefined-moe/lark-git-webhook/internal/config"
	"github.com/undefined-moe/lark-git-webhook/internal/forwarder"
	"github.com/undefined-moe/lark-git-webhook/internal/geoip"
	"github.com/undefined-moe/lark-git-webhook/internal/github"
	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/larkcallback"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
	"github.com/undefined-moe/lark-git-webhook/internal/webhook"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	a, err := archive.Open(filepath.Join(cfg.DataDir, "archive"), cfg.ArchiveMinFreeBytes)
	if err != nil {
		logger.Error("open archive", "error", err)
		os.Exit(1)
	}
	s, err := store.Open(filepath.Join(cfg.DataDir, "lark-git-webhook.db"), cfg.QueueMaxItems, cfg.QueueMaxBytes, cfg.DedupeMaxItems, cfg.DeadMaxItems, cfg.DeadMaxBytes)
	if err != nil {
		logger.Error("open queue store", "error", err)
		os.Exit(1)
	}
	if len(os.Args) > 1 {
		defer s.Close()
		if err := handleCommand(s, os.Args[1:]); err != nil {
			logger.Error("command failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if err := recoverPending(a, s, logger); err != nil {
		logger.Error("recover pending archive deliveries", "error", err)
		_ = s.Close()
		os.Exit(1)
	}
	m := &metrics.Metrics{}
	client := lark.New(cfg.LarkAppID, cfg.LarkAppSecret, cfg.LarkChatID, cfg.LarkHTTPTimeout)
	githubClient := github.New()
	var cityDB *geoip.Reader
	if cfg.MaxMindCityDBPath != "" {
		cityDB, err = geoip.Open(cfg.MaxMindCityDBPath)
		if err != nil {
			logger.Warn("MaxMind City database unavailable; continuing without MaxMind geolocation")
		} else {
			defer cityDB.Close()
		}
	}
	worker := forwarder.NewWorker(s, client.Send, m, logger, cfg.BatchWindow, cfg.BatchMaxEvents, cfg.LarkMaxMessageBytes, cfg.RetryBaseDelay, cfg.RetryMaxDelay, cfg.MaxAttempts)
	worker.SetLarkClient(client)
	if cityDB != nil {
		worker.SetGeoLookup(func(ip string) string { return cityDB.Lookup(ip).String() })
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	workerDone := make(chan struct{})
	go func() { worker.Run(ctx); close(workerDone) }()
	cleanupDone := make(chan struct{})
	go func() { cleanupDedupe(ctx, s, logger); close(cleanupDone) }()
	recoveryDone := make(chan struct{})
	go func() { recoverArchives(ctx, a, s, logger); close(recoveryDone) }()
	var auditDone <-chan struct{}
	if cfg.GitHubAuditOrg != "" {
		done := make(chan struct{})
		auditDone = done
		poller := audit.NewPoller(s, a, audit.NewClient(cfg.GitHubAuditToken), cfg.GitHubAuditOrg, cfg.GitHubAuditPollInterval, cfg.DedupeTTL, logger)
		go func() { poller.Run(ctx); close(done) }()
	}
	webhookHandler := webhook.New(s, a, cfg.GitHubSecret, cfg.LarkChatID, cfg.MaxBodyBytes, cfg.DedupeTTL, cfg.WebhookMaxInFlight, m)
	larkCallbackReceiver := larkcallback.New(s, a, cfg.LarkVerificationToken, cfg.LarkAppID, cfg.MaxBodyBytes, cfg.DedupeTTL, cfg.WebhookMaxInFlight, githubClient, client)
	webhookServer := newServer(cfg.ListenAddr, webhookMux(webhookHandler, larkCallbackReceiver, cfg))
	adminServer := newServer(cfg.AdminListenAddr, adminMux(s, a, worker, m, cfg))
	errCh := make(chan error, 2)
	go func() { errCh <- webhookServer.ListenAndServe() }()
	go func() { errCh <- adminServer.ListenAndServe() }()
	logger.Info("servers started", "webhook_listen_addr", cfg.ListenAddr, "admin_listen_addr", cfg.AdminListenAddr)
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := webhookServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown webhook server", "error", err)
		if closeErr := webhookServer.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			logger.Error("force close webhook server", "error", closeErr)
		}
	}
	if err := adminServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown admin server", "error", err)
		if closeErr := adminServer.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			logger.Error("force close admin server", "error", closeErr)
		}
	}
	webhookHandler.Wait()
	larkCallbackReceiver.Wait()
	cancel()
	<-workerDone
	<-cleanupDone
	<-recoveryDone
	if auditDone != nil {
		<-auditDone
	}
	if err := s.Close(); err != nil {
		logger.Error("close queue store", "error", err)
	}
	fmt.Fprintln(os.Stderr, "server stopped")
}

func newServer(address string, handler http.Handler) *http.Server {
	return &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
}

func handleCommand(s *store.Store, args []string) error {
	if len(args) != 2 || args[0] != "requeue-dead" {
		return errors.New("usage: lark-git-webhook requeue-dead <sequence>")
	}
	sequence, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid dead-letter sequence: %w", err)
	}
	if err := s.RequeueDeadLetter(sequence, time.Now()); err != nil {
		return fmt.Errorf("requeue dead letter %d: %w", sequence, err)
	}
	return nil
}

func cleanupDedupe(ctx context.Context, s *store.Store, logger *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := s.CleanupDedupe(now, 1000); err != nil {
				logger.Error("cleanup dedupe records", "error", err)
			}
		}
	}
}

func recoverPending(a *archive.Archive, s *store.Store, logger *slog.Logger) error {
	pending, err := s.PendingArchives(100)
	if err != nil {
		return err
	}
	for _, item := range pending {
		if _, err := a.Store(item.Event, item.DeliveryID, item.RawJSON); err != nil {
			return fmt.Errorf("archive pending delivery %s: %w", item.DeliveryID, err)
		}
		if err := s.FinalizeArchive(item.DeliveryID); err != nil {
			return fmt.Errorf("finalize pending delivery %s: %w", item.DeliveryID, err)
		}
		logger.Info("recovered pending archived delivery", "event", item.Event)
	}
	return nil
}

func recoverArchives(ctx context.Context, a *archive.Archive, s *store.Store, logger *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if err := recoverPending(a, s, logger); err != nil {
			logger.Error("recover pending archive deliveries", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func webhookMux(handler *webhook.Handler, larkReceiver *larkcallback.Receiver, cfg config.Config) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(cfg.WebhookPath, handler)
	mux.Handle(cfg.LarkEventPath, larkReceiver.EventHandler())
	mux.Handle(cfg.LarkCallbackPath, larkReceiver.CallbackHandler())
	return mux
}

func adminMux(s *store.Store, a *archive.Archive, worker *forwarder.Worker, m *metrics.Metrics, cfg config.Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		stats, err := s.Stats()
		free, freeErr := a.FreeBytes()
		pending, pendingErr := s.PendingArchives(1)
		if err != nil || freeErr != nil || pendingErr != nil || len(pending) > 0 || free < cfg.ArchiveMinFreeBytes || s.Writable() != nil || !worker.Alive(time.Now()) || stats.Items >= cfg.QueueMaxItems || stats.Bytes >= cfg.QueueMaxBytes || stats.DeadLetters >= cfg.DeadMaxItems || stats.DeadBytes >= cfg.DeadMaxBytes {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", m.Handler(func() metrics.Snapshot {
		stats, _ := s.Stats()
		age, _ := s.OldestAge(time.Now())
		free, _ := a.FreeBytes()
		return metrics.Snapshot{QueueItems: stats.Items, QueueBytes: stats.Bytes, DeadLetters: stats.DeadLetters, DeadBytes: stats.DeadBytes, DBBytes: s.FileSize(), OldestAge: age, WorkerLive: worker.Alive(time.Now()), ArchiveFreeBytes: free}
	}))
	return mux
}
