package forwarder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/onboarding"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func (w *Worker) sendOnboarding(ctx context.Context, item store.Item) batchResult {
	if w.lark == nil {
		return w.onboardingFailure(item, errors.New("Lark app client is not configured"))
	}
	var envelope struct {
		Header struct {
			EventType string `json:"event_type"`
		} `json:"header"`
	}
	if err := json.Unmarshal(item.RawJSON, &envelope); err != nil {
		return w.onboardingFailure(item, fmt.Errorf("parse Lark event header: %w", err))
	}
	if envelope.Header.EventType != onboarding.P2PChatEnteredEventType {
		return w.onboardingFailure(item, errors.New("unsupported Lark queued event"))
	}
	entered, err := onboarding.ParseEnteredEvent(item.RawJSON)
	if err != nil || entered.OpenID == "" || entered.ChatID == "" {
		if err == nil {
			err = errors.New("Lark onboarding event missing operator or chat")
		}
		return w.onboardingFailure(item, fmt.Errorf("parse Lark onboarding event: %w", err))
	}

	state, err := w.store.Onboarding(entered.OpenID)
	if err == nil && (state.FormMessageID != "" || state.GitHubLogin != "") {
		return w.deleteOnboarding(item)
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return w.onboardingFailure(item, fmt.Errorf("read onboarding state: %w", err))
	}
	if result := w.reserveOnboardingRequest(ctx, item); result != continueRound {
		return result
	}
	messageID, err := w.lark.CreateInteractiveToOpenID(ctx, entered.OpenID, onboarding.FormCard(), onboardingUUID(entered.OpenID))
	if err != nil {
		return w.onboardingFailure(item, err)
	}
	if err := w.store.SetOnboardingForm(entered.OpenID, messageID); err != nil {
		w.logger.Error("persist sent onboarding form", "error", err)
		return stopWorker
	}
	w.metrics.LarkSuccess()
	return w.deleteOnboarding(item)
}

func (w *Worker) deleteOnboarding(item store.Item) batchResult {
	if err := w.store.Delete([]store.Item{item}); err != nil {
		w.logger.Error("delete delivered onboarding item", "error", err)
		return stopWorker
	}
	w.metrics.EventDelivered(item.Event)
	return continueRound
}

func (w *Worker) reserveOnboardingRequest(ctx context.Context, item store.Item) batchResult {
	for {
		wait, err := w.store.ReserveRequest(time.Now())
		if err != nil {
			return w.onboardingFailure(item, fmt.Errorf("reserve rate-limit slot: %w", err))
		}
		if wait <= 0 {
			return continueRound
		}
		w.metrics.RateWait(wait)
		if !waitContext(ctx, wait) {
			return stopWorker
		}
	}
}

func (w *Worker) onboardingFailure(item store.Item, err error) batchResult {
	w.metrics.LarkFailure()
	retryErr := new(lark.RetryError)
	isRetry := errors.As(err, &retryErr)
	delay := w.backoff([]store.Item{item})
	retryAfter := time.Duration(0)
	permanent := isRetry && retryErr.Permanent
	if isRetry {
		retryAfter = retryErr.RetryAfter
		if retryAfter > delay {
			delay = retryAfter
		}
	}
	result, updateErr := w.store.HandleFailure([]store.Item{item}, time.Now(), time.Now().Add(delay), err.Error(), retryAfter, w.maxAttempts, permanent)
	if updateErr != nil {
		w.logger.Error("persist failed onboarding delivery state", "error", updateErr)
		return stopWorker
	}
	if result.Retried > 0 {
		w.metrics.Retry()
	}
	if retryAfter > 0 {
		w.logger.Warn("Lark rate limited; scheduled onboarding retry", "delay", delay)
		return stopRound
	}
	w.logger.Warn("Lark onboarding delivery failed", "retried", result.Retried, "dead_lettered", result.DeadLettered, "error", err)
	return continueRound
}

func onboardingUUID(openID string) string {
	digest := sha256.Sum256([]byte(openID))
	return "lark-onboarding-" + hex.EncodeToString(digest[:16])
}
