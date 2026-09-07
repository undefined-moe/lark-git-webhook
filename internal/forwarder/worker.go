package forwarder

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

type SendFunc func(context.Context, lark.Message) error

type workflowClient interface {
	CreateInteractive(context.Context, any, string) (string, error)
	CreateInteractiveToOpenID(context.Context, string, any, string) (string, error)
	UpdateInteractive(context.Context, string, any) error
	AddReaction(context.Context, string, string) (string, error)
	DeleteReaction(context.Context, string, string) error
}

type chatSender interface {
	SendToChat(context.Context, string, lark.Message) error
}

type chatWorkflowClient interface {
	CreateInteractiveToChat(context.Context, string, any, string) (string, error)
}

type batchResult uint8

const (
	continueRound batchResult = iota
	stopRound
	stopWorker
)

type Worker struct {
	store           *store.Store
	send            SendFunc
	lark            workflowClient
	metrics         *metrics.Metrics
	logger          *slog.Logger
	batchWindow     time.Duration
	batchMaxEvents  int
	maxMessageBytes int
	retryBase       time.Duration
	retryMax        time.Duration
	maxAttempts     int
	geoLookup       GeoLookup
	running         atomic.Bool
}

func NewWorker(s *store.Store, send SendFunc, m *metrics.Metrics, logger *slog.Logger, batchWindow time.Duration, batchMaxEvents, maxMessageBytes int, retryBase, retryMax time.Duration, maxAttempts int) *Worker {
	return &Worker{store: s, send: send, metrics: m, logger: logger, batchWindow: batchWindow, batchMaxEvents: batchMaxEvents, maxMessageBytes: maxMessageBytes, retryBase: retryBase, retryMax: retryMax, maxAttempts: maxAttempts}
}

func (w *Worker) SetLarkClient(client workflowClient) { w.lark = client }

// SetGeoLookup enables optional offline MaxMind geolocation for audit formatting.
func (w *Worker) SetGeoLookup(lookup GeoLookup) { w.geoLookup = lookup }

func (w *Worker) Run(ctx context.Context) {
	if !w.running.CompareAndSwap(false, true) {
		w.logger.Warn("worker already running")
		return
	}
	defer w.running.Store(false)
	release, ok := w.store.AcquireConsumer()
	if !ok {
		w.logger.Warn("another worker already consumes this store")
		return
	}
	defer release()
	for {
		items, err := w.store.Due(time.Now(), w.batchMaxEvents)
		if err != nil {
			w.logger.Error("read due queue items", "error", err)
			if !waitContext(ctx, time.Second) {
				return
			}
			continue
		}
		if len(items) == 0 {
			if !waitContext(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		if w.batchWindow > 0 && !waitContext(ctx, w.batchWindow) {
			return
		}
		items, err = w.store.Due(time.Now(), w.batchMaxEvents)
		if err != nil {
			w.logger.Error("read batched queue items", "error", err)
			continue
		}
		ordinary := make([]store.Item, 0, len(items))
		for _, item := range items {
			switch item.Event {
			case "workflow_run", "check_run":
				switch w.sendWorkflows(ctx, item) {
				case stopWorker:
					return
				case stopRound:
					goto nextRound
				}
			case "lark_event":
				switch w.sendOnboarding(ctx, item) {
				case stopWorker:
					return
				case stopRound:
					goto nextRound
				}
			default:
				ordinary = append(ordinary, item)
			}
		}
		switch w.sendOrdinary(ctx, ordinary) {
		case stopWorker:
			return
		case stopRound:
			goto nextRound
		}
	nextRound:
	}
}

func (w *Worker) Alive(_ time.Time) bool { return w.running.Load() }

func (w *Worker) sendBatch(ctx context.Context, items []store.Item) batchResult {
	return w.sendBatchToChat(ctx, "", items)
}

func (w *Worker) sendBatchToChat(ctx context.Context, chatID string, items []store.Item) batchResult {
	for {
		wait, err := w.store.ReserveRequest(time.Now())
		if err != nil {
			w.logger.Error("reserve rate-limit slot", "error", err)
			if waitContext(ctx, time.Second) {
				return continueRound
			}
			return stopWorker
		}
		if wait <= 0 {
			break
		}
		w.metrics.RateWait(wait)
		if !waitContext(ctx, wait) {
			return stopWorker
		}
	}
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, formatForLimit(item, w.maxMessageBytes, w.geoLookup))
	}
	err := w.sendToChat(ctx, chatID, MakeMessage(lines))
	if err == nil {
		if err := w.store.CompleteChatDelivery(items, chatID); err != nil {
			w.logger.Error("record delivered chat target", "error", err)
			return stopWorker
		}
		w.metrics.LarkSuccess()
		for _, item := range items {
			w.metrics.EventDelivered(item.Event)
		}
		return continueRound
	}
	w.metrics.LarkFailure()
	retryErr := new(lark.RetryError)
	isRetry := errors.As(err, &retryErr)
	delay := w.backoff(items)
	retryAfter := time.Duration(0)
	permanent := isRetry && retryErr.Permanent
	if isRetry {
		retryAfter = retryErr.RetryAfter
		if retryAfter > delay {
			delay = retryAfter
		}
	}
	result, updateErr := w.store.HandleFailure(items, time.Now(), time.Now().Add(delay), err.Error(), retryAfter, w.maxAttempts, permanent)
	if updateErr != nil {
		w.logger.Error("persist failed delivery state", "error", updateErr)
		return stopWorker
	}
	if result.Retried > 0 {
		w.metrics.Retry()
	}
	if result.DeadLetterFull {
		w.logger.Error("dead-letter capacity full; retained failed items in active queue", "items", len(items))
	}
	if retryAfter > 0 {
		w.logger.Warn("Lark rate limited; scheduled retry", "items", len(items), "delay", delay)
		return stopRound
	}
	w.logger.Warn("Lark delivery failed", "items", len(items), "retried", result.Retried, "dead_lettered", result.DeadLettered, "error", err)
	return continueRound
}
func (w *Worker) sendToChat(ctx context.Context, chatID string, message lark.Message) error {
	if chatID != "" {
		if client, ok := w.lark.(chatSender); ok {
			return client.SendToChat(ctx, chatID, message)
		}
	}
	if client, ok := w.lark.(chatSender); ok && w.send == nil {
		return client.SendToChat(ctx, chatID, message)
	}
	return w.send(ctx, message)
}

func (w *Worker) sendOrdinary(ctx context.Context, items []store.Item) batchResult {
	if len(items) == 0 {
		return continueRound
	}
	byChat := make(map[string][]store.Item)
	for _, item := range items {
		for _, chatID := range itemChatIDs(item) {
			byChat[chatID] = append(byChat[chatID], item)
		}
	}
	chatIDs := make([]string, 0, len(byChat))
	for chatID := range byChat {
		chatIDs = append(chatIDs, chatID)
	}
	sort.Strings(chatIDs)
	for _, chatID := range chatIDs {
		for _, batch := range splitWithLookup(byChat[chatID], w.maxMessageBytes, w.geoLookup) {
			if result := w.sendBatchToChat(ctx, chatID, batch); result != continueRound {
				return result
			}
		}
	}
	return continueRound
}

func itemChatIDs(item store.Item) []string {
	if len(item.ChatIDs) == 0 {
		return []string{""}
	}
	seen := make(map[string]bool, len(item.ChatIDs))
	chatIDs := make([]string, 0, len(item.ChatIDs))
	for _, chatID := range item.ChatIDs {
		if !seen[chatID] {
			chatIDs, seen[chatID] = append(chatIDs, chatID), true
		}
	}
	sort.Strings(chatIDs)
	return chatIDs
}

func (w *Worker) backoff(items []store.Item) time.Duration {
	attempt := 0
	for _, item := range items {
		if item.Attempts > attempt {
			attempt = item.Attempts
		}
	}
	delay := w.retryBase
	for i := 0; i < attempt && delay < w.retryMax; i++ {
		delay *= 2
		if delay > w.retryMax {
			delay = w.retryMax
		}
	}
	jitter := time.Duration(rand.Float64() * float64(delay) * 0.2)
	if delay+jitter > w.retryMax {
		return w.retryMax
	}
	return delay + jitter
}

func split(items []store.Item, maxBytes int) [][]store.Item {
	return splitWithLookup(items, maxBytes, nil)
}

func splitWithLookup(items []store.Item, maxBytes int, lookup GeoLookup) [][]store.Item {
	batches := make([][]store.Item, 0, len(items))
	for _, item := range items {
		if len(batches) == 0 {
			batches = append(batches, []store.Item{item})
			continue
		}
		candidate := append(append([]store.Item(nil), batches[len(batches)-1]...), item)
		if messageSizeWithLookup(candidate, maxBytes, lookup)+300 > maxBytes {
			batches = append(batches, []store.Item{item})
		} else {
			batches[len(batches)-1] = candidate
		}
	}
	return batches
}
func messageSize(items []store.Item, maxBytes int) int {
	return messageSizeWithLookup(items, maxBytes, nil)
}

func messageSizeWithLookup(items []store.Item, maxBytes int, lookup GeoLookup) int {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, formatForLimit(item, maxBytes, lookup))
	}
	return serializedMessageSize(lines)
}
func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
