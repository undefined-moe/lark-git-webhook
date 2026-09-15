// Package larkcallback receives unencrypted Lark event and callback deliveries.
package larkcallback

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/onboarding"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

const (
	eventKind                       = "lark_event"
	callbackKind                    = "lark_callback"
	callbackBudget                  = 2500 * time.Millisecond
	githubLookupBudget              = 1500 * time.Millisecond
	callbackResultPersistenceBudget = 750 * time.Millisecond
	temporaryToast                  = "验证服务暂时不可用，请重试"
	rateLimitToast                  = "验证请求过于频繁，请稍后重试"
	genericBindToast                = "无法绑定 GitHub 用户名，请重试"
)

// GitHubLookup is the minimal GitHub API surface used for onboarding callbacks.
type GitHubLookup interface {
	ValidateUser(context.Context, string) (string, error)
}

// ChatSender is the Lark message surface used for subscription confirmations.
type ChatSender interface {
	SendToChat(context.Context, string, lark.Message) error
}

type callbackOutcome struct {
	status    int
	accepted  bool
	toastType string
	content   string
	card      map[string]any
	message   string
}

type Receiver struct {
	store             *store.Store
	archive           *archive.Archive
	github            GitHubLookup
	chatSender        ChatSender
	addSubscription   func(string, string) error
	claimSubscription func(string) (bool, error)
	token             []byte
	appID             string
	maxBodyBytes      int64
	dedupeTTL         time.Duration
	inFlight          chan struct{}
	wg                sync.WaitGroup
}

func New(s *store.Store, a *archive.Archive, verificationToken, appID string, maxBodyBytes int64, dedupeTTL time.Duration, maxInFlight int, githubClient GitHubLookup, chatSender ChatSender) *Receiver {
	return &Receiver{
		store:             s,
		archive:           a,
		github:            githubClient,
		chatSender:        chatSender,
		addSubscription:   s.AddRepositorySubscription,
		claimSubscription: s.ClaimSubscriptionReply,
		token:             []byte(verificationToken),
		appID:             appID,
		maxBodyBytes:      maxBodyBytes,
		dedupeTTL:         dedupeTTL,
		inFlight:          make(chan struct{}, maxInFlight),
	}
}

func (r *Receiver) EventHandler() http.Handler { return http.HandlerFunc(r.Event) }

func (r *Receiver) CallbackHandler() http.Handler { return http.HandlerFunc(r.Callback) }

func (r *Receiver) Event(w http.ResponseWriter, request *http.Request) {
	r.serve(w, request, eventKind)
}

func (r *Receiver) Callback(w http.ResponseWriter, request *http.Request) {
	r.serve(w, request, callbackKind)
}

func (r *Receiver) Wait() { r.wg.Wait() }

func (r *Receiver) serve(w http.ResponseWriter, request *http.Request, kind string) {
	r.wg.Add(1)
	async := false
	defer func() {
		if !async {
			r.wg.Done()
		}
	}()
	callbackCtx := request.Context()
	var cancel context.CancelFunc
	if kind == callbackKind {
		callbackCtx, cancel = context.WithTimeout(callbackCtx, callbackBudget)
		defer func() {
			if !async {
				cancel()
			}
		}()
		deadline := time.Now().Add(callbackBudget)
		controller := http.NewResponseController(w)
		_ = controller.SetReadDeadline(deadline)
		_ = controller.SetWriteDeadline(deadline)
	}
	if request.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	inFlight := false
	select {
	case r.inFlight <- struct{}{}:
		inFlight = true
		defer func() {
			if inFlight && !async {
				<-r.inFlight
			}
		}()
	default:
		http.Error(w, "Lark callback capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, r.maxBodyBytes+1))
	if err != nil {
		if kind == callbackKind && callbackCtx.Err() != nil {
			writeErrorToast(w, temporaryToast)
			return
		}
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}
	if kind == callbackKind && callbackCtx.Err() != nil {
		writeErrorToast(w, temporaryToast)
		return
	}
	if int64(len(body)) > r.maxBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil || top == nil {
		http.Error(w, "invalid Lark JSON", http.StatusBadRequest)
		return
	}
	if _, encrypted := top["encrypt"]; encrypted {
		http.Error(w, "encrypted Lark payloads are unsupported", http.StatusBadRequest)
		return
	}

	var verification verificationRequest
	if err := json.Unmarshal(body, &verification); err != nil {
		http.Error(w, "invalid Lark JSON", http.StatusBadRequest)
		return
	}
	if verification.Type == "url_verification" {
		if verification.Challenge == "" {
			http.Error(w, "missing Lark challenge", http.StatusBadRequest)
			return
		}
		if !r.validToken(verification.Token) {
			http.Error(w, "invalid Lark verification token", http.StatusUnauthorized)
			return
		}
		writeChallenge(w, verification.Challenge)
		return
	}

	var delivery deliveryRequest
	if err := json.Unmarshal(body, &delivery); err != nil {
		http.Error(w, "invalid Lark JSON", http.StatusBadRequest)
		return
	}
	if delivery.Schema != "2.0" || delivery.Header.EventID == "" || delivery.Header.EventType == "" || delivery.Header.Token == "" || delivery.Header.AppID == "" {
		http.Error(w, "invalid Lark schema 2.0 delivery", http.StatusBadRequest)
		return
	}
	if !r.validToken(delivery.Header.Token) {
		http.Error(w, "invalid Lark verification token", http.StatusUnauthorized)
		return
	}
	if !hmac.Equal([]byte(delivery.Header.AppID), []byte(r.appID)) {
		http.Error(w, "invalid Lark app ID", http.StatusUnauthorized)
		return
	}

	if kind == callbackKind {
		callback, onboardingCallback := r.onboardingCallback(kind, delivery, body)
		outcomes := make(chan callbackOutcome, 1)
		async = true
		go func() {
			defer r.wg.Done()
			defer func() { <-r.inFlight }()
			if onboardingCallback {
				outcomes <- r.processOnboardingDelivery(callbackCtx, delivery, body, callback)
				return
			}
			outcomes <- r.processArchiveOnlyCallbackDelivery(callbackCtx, delivery, body)
		}()
		select {
		case outcome := <-outcomes:
			if callbackCtx.Err() != nil {
				writeErrorToast(w, temporaryToast)
				return
			}
			writeCallbackOutcome(w, outcome)
			cancel()
		case <-callbackCtx.Done():
			writeErrorToast(w, temporaryToast)
		}
		return
	}
	queueOnboarding := delivery.Header.EventType == onboarding.P2PChatEnteredEventType && validEnteredEvent(body)
	subscription, subscribe := groupSubscription(body, delivery.Header.EventType)
	deliveryID := qualifiedDeliveryID(kind, delivery.Header.EventID)
	state, found, err := r.store.LookupDelivery(deliveryID, kind, body)
	if err != nil {
		r.writeStoreError(w, err)
		return
	}
	if found {
		exists, err := r.archive.VerifyExisting(kind, deliveryID, body)
		if err != nil {
			r.writeArchiveError(w, err)
			return
		}
		if exists {
			if !state.Archived {
				if err := r.store.FinalizeArchive(deliveryID); err != nil {
					http.Error(w, "store unavailable", http.StatusServiceUnavailable)
					return
				}
			}
			if subscribe {
				if err := r.subscribeAndReply(request.Context(), deliveryID, subscription); err != nil {
					http.Error(w, "store unavailable", http.StatusServiceUnavailable)
					return
				}
			}
			writeAccepted(w)
			return
		}
	}

	reservation, err := r.archive.Reserve(archive.AdmissionRequired(uint64(len(body))))
	if err != nil {
		http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
		return
	}
	defer reservation.Release()
	if queueOnboarding {
		_, err = r.store.Admit(deliveryID, eventKind, body, time.Now(), r.dedupeTTL)
	} else {
		_, err = r.store.AdmitArchiveOnly(deliveryID, kind, body)
	}
	if err != nil {
		r.writeStoreError(w, err)
		return
	}
	if _, err := reservation.Store(kind, deliveryID, body); err != nil {
		r.writeArchiveError(w, err)
		return
	}
	if err := r.store.FinalizeArchive(deliveryID); err != nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	if subscribe {
		if err := r.subscribeAndReply(request.Context(), deliveryID, subscription); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	reservation.Release()
	writeAccepted(w)
}

func (r *Receiver) subscribeAndReply(ctx context.Context, deliveryID string, subscription repositorySubscription) error {
	if err := r.addSubscription(subscription.ChatID, subscription.Repository); err != nil {
		return err
	}
	claimed, err := r.claimSubscription(deliveryID)
	if err != nil || !claimed || r.chatSender == nil {
		return err
	}
	message := lark.Message{MsgType: "post", Content: lark.Content{Post: lark.Post{
		"zh_cn": {Content: [][]lark.Text{{{Tag: "text", Text: "已订阅 GitHub 仓库：" + subscription.Repository}}}},
	}}}
	if err := r.chatSender.SendToChat(ctx, subscription.ChatID, message); err != nil {
		slog.Warn("send subscription confirmation", "delivery_id", deliveryID, "chat_id", subscription.ChatID, "repository", subscription.Repository, "error", err)
	}
	return nil
}

type repositorySubscription struct {
	ChatID     string
	Repository string
}

func groupSubscription(body []byte, eventType string) (repositorySubscription, bool) {
	if eventType != "im.message.receive_v1" {
		return repositorySubscription{}, false
	}
	var delivery struct {
		Event struct {
			Message struct {
				ChatID      string `json:"chat_id"`
				ChatType    string `json:"chat_type"`
				MessageType string `json:"message_type"`
				Content     string `json:"content"`
				Mentions    []struct {
					Key string `json:"key"`
				} `json:"mentions"`
			} `json:"message"`
		} `json:"event"`
	}
	if json.Unmarshal(body, &delivery) != nil || delivery.Event.Message.ChatType != "group" || delivery.Event.Message.MessageType != "text" || delivery.Event.Message.ChatID == "" {
		return repositorySubscription{}, false
	}
	var content struct {
		Text string `json:"text"`
	}
	if json.Unmarshal([]byte(delivery.Event.Message.Content), &content) != nil {
		return repositorySubscription{}, false
	}
	text := content.Text
	if !strings.HasPrefix(text, "/github-add ") {
		mentions := delivery.Event.Message.Mentions
		if len(mentions) != 1 || mentions[0].Key == "" || !strings.HasPrefix(text, mentions[0].Key+" ") {
			return repositorySubscription{}, false
		}
		text = strings.TrimPrefix(text, mentions[0].Key+" ")
	}
	if !strings.HasPrefix(text, "/github-add ") {
		return repositorySubscription{}, false
	}
	parts := strings.Split(strings.TrimPrefix(text, "/github-add "), "/")
	if len(parts) != 2 || !validRepositoryOwner(parts[0]) || !validRepositoryName(parts[1]) {
		return repositorySubscription{}, false
	}
	return repositorySubscription{ChatID: delivery.Event.Message.ChatID, Repository: strings.ToLower(parts[0] + "/" + parts[1])}, true
}

// validRepositoryOwner accepts GitHub logins (letters, digits, hyphens) and
// GitLab group/subgroup names, which additionally allow underscores.
func validRepositoryOwner(value string) bool {
	if len(value) == 0 || len(value) > 100 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func validRepositoryName(value string) bool {
	if len(value) == 0 || len(value) > 100 || value[0] == '.' || value[0] == '-' || value[len(value)-1] == '.' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validEnteredEvent(body []byte) bool {
	event, err := onboarding.ParseEnteredEvent(body)
	return err == nil && event.OpenID != "" && event.ChatID != ""
}

func (r *Receiver) onboardingCallback(kind string, delivery deliveryRequest, body []byte) (onboarding.Callback, bool) {
	if kind != callbackKind || delivery.Header.EventType != onboarding.CardActionTriggerEvent {
		return onboarding.Callback{}, false
	}
	callback, err := onboarding.ParseCallback(body)
	return callback, err == nil && callback.IsOnboarding()
}

func (r *Receiver) waitOnboardingCallbackResult(ctx context.Context, deliveryID string) (store.OnboardingCallbackResult, bool, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, found, err := r.store.OnboardingCallbackResult(deliveryID)
		if err != nil || (found && result.ToastType != "" && result.Content != "") {
			return result, found, err
		}
		select {
		case <-ctx.Done():
			return store.OnboardingCallbackResult{}, false, ctx.Err()
		case <-ticker.C:
		}
	}
}

type verificationRequest struct {
	Type      string `json:"type"`
	Token     string `json:"token"`
	Challenge string `json:"challenge"`
}

type deliveryRequest struct {
	Schema string `json:"schema"`
	Header struct {
		EventID   string `json:"event_id"`
		EventType string `json:"event_type"`
		Token     string `json:"token"`
		AppID     string `json:"app_id"`
	} `json:"header"`
}

func (r *Receiver) validToken(token string) bool { return hmac.Equal(r.token, []byte(token)) }

func qualifiedDeliveryID(kind, eventID string) string {
	digest := sha256.Sum256([]byte(eventID))
	return kind + ":" + hex.EncodeToString(digest[:])
}

func (r *Receiver) writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrDeliveryConflict) {
		http.Error(w, "delivery conflicts with permanent state", http.StatusConflict)
		return
	}
	http.Error(w, "store unavailable", http.StatusServiceUnavailable)
}

func (r *Receiver) writeArchiveError(w http.ResponseWriter, err error) {
	if errors.Is(err, archive.ErrConflict) {
		http.Error(w, "delivery conflicts with permanent archive", http.StatusConflict)
		return
	}
	http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
}

func writeChallenge(w http.ResponseWriter, challenge string) {
	encoded, _ := json.Marshal(challenge)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{ "challenge": %s }`, encoded)
}

func writeAccepted(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{}"))
}

func writeSuccessToast(w http.ResponseWriter, content string) { writeToast(w, "success", content) }
func writeErrorToast(w http.ResponseWriter, content string)   { writeToast(w, "error", content) }

func writeCallbackSuccess(w http.ResponseWriter, toastType, content string, card map[string]any) {
	if card == nil {
		writeToast(w, toastType, content)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"toast": map[string]string{"type": toastType, "content": content},
		"card":  map[string]any{"type": "raw", "data": card},
	})
}

func writeToast(w http.ResponseWriter, toastType, content string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"toast": map[string]string{"type": toastType, "content": content}})
}
