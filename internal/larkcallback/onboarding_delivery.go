package larkcallback

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/github"
	"github.com/undefined-moe/lark-git-webhook/internal/onboarding"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func (r *Receiver) processOnboardingDelivery(ctx context.Context, delivery deliveryRequest, body []byte, callback onboarding.Callback) callbackOutcome {
	deliveryID := qualifiedDeliveryID(callbackKind, delivery.Header.EventID)
	state, found, err := r.store.LookupDelivery(deliveryID, callbackKind, body)
	if err != nil {
		return storeFailureOutcome(err)
	}
	if found {
		exists, err := r.archive.VerifyExistingContext(ctx, callbackKind, deliveryID, body)
		if err != nil {
			if ctx.Err() != nil {
				return temporaryOutcome()
			}
			return archiveFailureOutcome(err)
		}
		if exists {
			if !state.Archived {
				if err := r.store.FinalizeArchive(deliveryID); err != nil {
					return storeFailureOutcome(err)
				}
			}
			if ctx.Err() != nil {
				return temporaryOutcome()
			}
			return r.processOnboardingCallbackOutcome(ctx, deliveryID, callback)
		}
	}

	reservation, err := r.archive.ReserveContext(ctx, archive.AdmissionRequired(uint64(len(body))))
	if err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		return archiveFailureOutcome(err)
	}
	defer reservation.Release()
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	if _, err := r.store.AdmitArchiveOnly(deliveryID, callbackKind, body); err != nil {
		return storeFailureOutcome(err)
	}
	if _, err := reservation.Store(callbackKind, deliveryID, body); err != nil {
		return archiveFailureOutcome(err)
	}
	if err := r.store.FinalizeArchive(deliveryID); err != nil {
		return storeFailureOutcome(err)
	}
	reservation.Release()
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	return r.processOnboardingCallbackOutcome(ctx, deliveryID, callback)
}

func (r *Receiver) processArchiveOnlyCallbackDelivery(ctx context.Context, delivery deliveryRequest, body []byte) callbackOutcome {
	deliveryID := qualifiedDeliveryID(callbackKind, delivery.Header.EventID)
	state, found, err := r.store.LookupDelivery(deliveryID, callbackKind, body)
	if err != nil {
		return storeFailureOutcome(err)
	}
	if found {
		exists, err := r.archive.VerifyExistingContext(ctx, callbackKind, deliveryID, body)
		if err != nil {
			if ctx.Err() != nil {
				return temporaryOutcome()
			}
			return archiveFailureOutcome(err)
		}
		if exists {
			if !state.Archived {
				if err := r.store.FinalizeArchive(deliveryID); err != nil {
					return storeFailureOutcome(err)
				}
			}
			if ctx.Err() != nil {
				return temporaryOutcome()
			}
			return acceptedOutcome()
		}
	}

	reservation, err := r.archive.ReserveContext(ctx, archive.AdmissionRequired(uint64(len(body))))
	if err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		return archiveFailureOutcome(err)
	}
	defer reservation.Release()
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	if _, err := r.store.AdmitArchiveOnly(deliveryID, callbackKind, body); err != nil {
		return storeFailureOutcome(err)
	}
	if _, err := reservation.Store(callbackKind, deliveryID, body); err != nil {
		return archiveFailureOutcome(err)
	}
	if err := r.store.FinalizeArchive(deliveryID); err != nil {
		return storeFailureOutcome(err)
	}
	reservation.Release()
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	return acceptedOutcome()
}

func (r *Receiver) processOnboardingCallbackOutcome(ctx context.Context, deliveryID string, callback onboarding.Callback) callbackOutcome {
	result, claimed, err := r.store.ClaimOnboardingCallbackResultContext(ctx, deliveryID, time.Now(), callbackBudget)
	if err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		return storeFailureOutcome(err)
	}
	if !claimed {
		if result.ToastType != "" && result.Content != "" {
			return r.replayOnboardingOutcome(callback, result)
		}
		result, found, err := r.waitOnboardingCallbackResult(ctx, deliveryID)
		if err != nil {
			if ctx.Err() != nil {
				return temporaryOutcome()
			}
			return storeFailureOutcome(err)
		}
		if found {
			return r.replayOnboardingOutcome(callback, result)
		}
		return temporaryOutcome()
	}
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	if callback.OpenID == "" || callback.OpenMessageID == "" {
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", genericBindToast)
	}
	state, err := r.store.Onboarding(callback.OpenID)
	if err != nil || state.FormMessageID == "" || state.FormMessageID != callback.OpenMessageID {
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", genericBindToast)
	}
	login, valid := onboarding.ValidateGitHubLogin(callback.GitHubID)
	if !valid {
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", "GitHub 用户名格式无效")
	}
	if state.GitHubLogin != "" && strings.EqualFold(state.GitHubLogin, login) {
		return r.persistOnboardingSuccessOutcome(ctx, deliveryID, state.GitHubLogin)
	}
	if r.github == nil {
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", temporaryToast)
	}
	if _, err := r.store.ReserveOnboardingValidationContext(ctx, callback.OpenID, callback.OpenMessageID, time.Now()); err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		if errors.Is(err, store.ErrOnboardingValidationCooldown) {
			return r.persistOnboardingOutcome(ctx, deliveryID, "error", rateLimitToast)
		}
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", genericBindToast)
	}
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	lookupDeadline := time.Now().Add(githubLookupBudget)
	if parentDeadline, ok := ctx.Deadline(); ok {
		persistenceDeadline := parentDeadline.Add(-callbackResultPersistenceBudget)
		if !persistenceDeadline.After(time.Now()) {
			return r.persistOnboardingOutcome(ctx, deliveryID, "error", temporaryToast)
		}
		if persistenceDeadline.Before(lookupDeadline) {
			lookupDeadline = persistenceDeadline
		}
	}
	lookupCtx, cancel := context.WithDeadline(ctx, lookupDeadline)
	canonical, err := r.github.ValidateUser(lookupCtx, login)
	lookupErr := lookupCtx.Err()
	cancel()
	if lookupErr != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", temporaryToast)
	}
	if err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		var notFound *github.NotFoundError
		if errors.As(err, &notFound) {
			return r.persistOnboardingOutcome(ctx, deliveryID, "error", "GitHub 用户名不存在")
		}
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", temporaryToast)
	}
	if ctx.Err() != nil || !strings.EqualFold(canonical, login) {
		return temporaryOutcome()
	}
	if err := r.store.SetOnboardingGitHubLoginContext(ctx, callback.OpenID, callback.OpenMessageID, canonical); err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		return r.persistOnboardingOutcome(ctx, deliveryID, "error", genericBindToast)
	}
	return r.persistOnboardingSuccessOutcome(ctx, deliveryID, canonical)
}

func (r *Receiver) replayOnboardingOutcome(callback onboarding.Callback, result store.OnboardingCallbackResult) callbackOutcome {
	if result.ToastType != "success" {
		return toastOutcome(result.ToastType, result.Content)
	}
	state, err := r.store.Onboarding(callback.OpenID)
	if err != nil {
		return storeFailureOutcome(err)
	}
	if state.FormMessageID != callback.OpenMessageID || state.GitHubLogin == "" {
		return toastOutcome("error", genericBindToast)
	}
	return successCardOutcome(state.GitHubLogin)
}

func (r *Receiver) persistOnboardingSuccessOutcome(ctx context.Context, deliveryID, login string) callbackOutcome {
	outcome := r.persistOnboardingOutcome(ctx, deliveryID, "success", "GitHub 用户名已绑定")
	if outcome.status != http.StatusOK {
		return outcome
	}
	return successCardOutcome(login)
}

func (r *Receiver) persistOnboardingOutcome(ctx context.Context, deliveryID, toastType, content string) callbackOutcome {
	if ctx.Err() != nil {
		return temporaryOutcome()
	}
	if err := r.store.SetOnboardingCallbackResultContext(ctx, deliveryID, store.OnboardingCallbackResult{ToastType: toastType, Content: content}); err != nil {
		if ctx.Err() != nil {
			return temporaryOutcome()
		}
		return storeFailureOutcome(err)
	}
	return toastOutcome(toastType, content)
}

func temporaryOutcome() callbackOutcome {
	return toastOutcome("error", temporaryToast)
}

func toastOutcome(toastType, content string) callbackOutcome {
	return callbackOutcome{status: http.StatusOK, toastType: toastType, content: content}
}

func successCardOutcome(login string) callbackOutcome {
	return callbackOutcome{
		status:    http.StatusOK,
		toastType: "success",
		content:   "GitHub 用户名已绑定",
		card:      onboarding.BoundCard(login),
	}
}

func acceptedOutcome() callbackOutcome { return callbackOutcome{status: http.StatusOK, accepted: true} }

func archiveFailureOutcome(err error) callbackOutcome {
	if errors.Is(err, archive.ErrConflict) {
		return callbackOutcome{status: http.StatusConflict, message: "delivery conflicts with permanent archive"}
	}
	return callbackOutcome{status: http.StatusServiceUnavailable, message: "archive unavailable"}
}

func storeFailureOutcome(err error) callbackOutcome {
	if errors.Is(err, store.ErrDeliveryConflict) {
		return callbackOutcome{status: http.StatusConflict, message: "delivery conflicts with permanent state"}
	}
	return callbackOutcome{status: http.StatusServiceUnavailable, message: "store unavailable"}
}

func writeCallbackOutcome(w http.ResponseWriter, outcome callbackOutcome) {
	if outcome.status == http.StatusOK {
		if outcome.accepted {
			writeAccepted(w)
			return
		}
		writeCallbackSuccess(w, outcome.toastType, outcome.content, outcome.card)
		return
	}
	http.Error(w, outcome.message, outcome.status)
}
