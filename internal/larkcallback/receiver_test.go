package larkcallback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/github"
	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

const (
	testToken = "verification-token"
	testAppID = "cli_test"
)

func TestReceiverRejectsMethodSizeEncryptionAndInvalidAuthentication(t *testing.T) {
	r, _ := testReceiver(t, 1024)

	method := httptest.NewRecorder()
	r.Event(method, httptest.NewRequest(http.MethodGet, "/webhook/lark/event", nil))
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("method status=%d allow=%q", method.Code, method.Header().Get("Allow"))
	}

	tooLarge := httptest.NewRecorder()
	r.Event(tooLarge, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", strings.NewReader(strings.Repeat("x", 1025))))
	if tooLarge.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large status=%d", tooLarge.Code)
	}

	for _, test := range []struct {
		name string
		body []byte
		want int
	}{
		{"encrypt", []byte(`{"encrypt":"not-supported"}`), http.StatusBadRequest},
		{"token", delivery("event-1", "im.message.receive_v1", "wrong-token", testAppID), http.StatusUnauthorized},
		{"app ID", delivery("event-1", "im.message.receive_v1", testToken, "wrong-app"), http.StatusUnauthorized},
		{"schema", []byte(`{"schema":"1.0","header":{"event_id":"event-1","event_type":"im.message.receive_v1","token":"verification-token","app_id":"cli_test"}}`), http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(test.body)))
			if response.Code != test.want {
				t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
			}
		})
	}
}

func TestReceiverReturnsChallengesWithoutArchiving(t *testing.T) {
	r, s := testReceiver(t, 1024)
	body := []byte(`{"type":"url_verification","token":"verification-token","challenge":"challenge-value"}`)
	for _, handler := range []func(http.ResponseWriter, *http.Request){r.Event, r.Callback} {
		response := httptest.NewRecorder()
		handler(response, httptest.NewRequest(http.MethodPost, "/webhook/lark", bytes.NewReader(body)))
		if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" || response.Body.String() != `{ "challenge": "challenge-value" }` {
			t.Fatalf("challenge response status=%d content-type=%q body=%q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
		}
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 0 || stats.Bytes != 0 {
		t.Fatalf("challenge state=%+v err=%v", stats, err)
	}
}

func TestReceiverArchivesEventAndCallbackWithSourceQualifiedIDs(t *testing.T) {
	r, s, a := testReceiverWithArchive(t, 1024, 0)
	for _, test := range []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		kind    string
		body    []byte
	}{
		{"event", r.Event, eventKind, delivery("delivery-1", "im.message.receive_v1", testToken, testAppID)},
		{"callback", r.Callback, callbackKind, delivery("delivery-1", "card.action.trigger", testToken, testAppID)},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			test.handler(response, httptest.NewRequest(http.MethodPost, "/webhook/lark", bytes.NewReader(test.body)))
			if response.Code != http.StatusOK || response.Body.String() != "{}" {
				t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
			}
			id := qualifiedDeliveryID(test.kind, "delivery-1")
			exists, err := a.VerifyExisting(test.kind, id, test.body)
			if err != nil || !exists {
				t.Fatalf("archive exists=%v err=%v", exists, err)
			}
		})
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("forwardable items=%+v err=%v", items, err)
	}
}

func TestReceiverDuplicateAndConflict(t *testing.T) {
	r, _ := testReceiver(t, 1024)
	body := delivery("delivery-1", "im.message.receive_v1", testToken, testAppID)
	for i := 0; i < 2; i++ {
		response := httptest.NewRecorder()
		r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
		if response.Code != http.StatusOK || response.Body.String() != "{}" {
			t.Fatalf("request %d status=%d body=%q", i, response.Code, response.Body.String())
		}
	}
	conflict := httptest.NewRecorder()
	r.Event(conflict, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(delivery("delivery-1", "im.message.receive_v1", testToken, testAppID+"-different"))))
	if conflict.Code != http.StatusUnauthorized {
		t.Fatalf("app mismatch status=%d", conflict.Code)
	}
	conflict = httptest.NewRecorder()
	r.Event(conflict, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(delivery("delivery-1", "im.message.updated_v1", testToken, testAppID))))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("delivery conflict status=%d", conflict.Code)
	}
}

func TestReceiverReturnsServiceUnavailableWhenArchiveCannotAccept(t *testing.T) {
	r, _, _ := testReceiverWithArchive(t, 1024, ^uint64(0))
	response := httptest.NewRecorder()
	r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(delivery("delivery-1", "im.message.receive_v1", testToken, testAppID))))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("archive failure status=%d", response.Code)
	}
}

func TestReceiverReturnsServiceUnavailableWhenStoreIsClosed(t *testing.T) {
	r, s := testReceiver(t, 1024)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(delivery("delivery-1", "im.message.receive_v1", testToken, testAppID))))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("store failure status=%d", response.Code)
	}
}

func TestReceiverSharesCapacityAcrossRoutes(t *testing.T) {
	r, _ := testReceiver(t, 1024)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Event(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/webhook/lark/event", blockingReader{started: started, release: release}))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("event route did not acquire capacity")
	}
	callback := httptest.NewRecorder()
	r.Callback(callback, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", strings.NewReader(`{}`)))
	if callback.Code != http.StatusServiceUnavailable {
		t.Fatalf("callback capacity status=%d", callback.Code)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event route did not finish")
	}
	r.Wait()
}

func TestReceiverQueuesOnlyValidOnboardingEnteredEvents(t *testing.T) {
	r, s, a := testReceiverWithArchive(t, 4096, 0)
	valid := larkDelivery("entered", "im.chat.access_event.bot_p2p_chat_entered_v1", `{"operator_id":{"open_id":"ou_1"},"chat_id":"oc_1"}`)
	response := httptest.NewRecorder()
	r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(valid)))
	if response.Code != http.StatusOK || response.Body.String() != "{}" {
		t.Fatalf("valid response=%d %q", response.Code, response.Body.String())
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 1 || items[0].Event != eventKind {
		t.Fatalf("queued items=%+v err=%v", items, err)
	}
	if exists, err := a.VerifyExisting(eventKind, qualifiedDeliveryID(eventKind, "entered"), valid); err != nil || !exists {
		t.Fatalf("archive exists=%v err=%v", exists, err)
	}

	for _, body := range [][]byte{
		larkDelivery("missing-chat", "im.chat.access_event.bot_p2p_chat_entered_v1", `{"operator_id":{"open_id":"ou_2"}}`),
		larkDelivery("unrelated", "im.message.receive_v1", `{"operator_id":{"open_id":"ou_3"},"chat_id":"oc_3"}`),
	} {
		response := httptest.NewRecorder()
		r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("archive-only response=%d", response.Code)
		}
	}
	items, err = s.Due(time.Now(), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("archive-only deliveries were queued: items=%+v err=%v", items, err)
	}
}

func TestGroupSubscriptionParsesOnlyExactGroupTextCommand(t *testing.T) {
	valid := larkDelivery("subscription", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add Acme/Repo\"}"}}`)
	subscription, ok := groupSubscription(valid, "im.message.receive_v1")
	if !ok || subscription.ChatID != "oc_group" || subscription.Repository != "acme/repo" {
		t.Fatalf("subscription=%+v ok=%v", subscription, ok)
	}

	mentioned := larkDelivery("mentioned-subscription", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 /github-add Acme/Repo\"}","mentions":[{"key":"@_user_1","id":{"open_id":"ou_bot"}}]}}`)
	subscription, ok = groupSubscription(mentioned, "im.message.receive_v1")
	if !ok || subscription.ChatID != "oc_group" || subscription.Repository != "acme/repo" {
		t.Fatalf("mentioned subscription=%+v ok=%v", subscription, ok)
	}

	for _, body := range [][]byte{
		larkDelivery("private", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"p2p","message_type":"text","content":"{\"text\":\"/github-add acme/repo\"}"}}`),
		larkDelivery("extra", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add acme/repo now\"}"}}`),
		larkDelivery("invalid", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add acme/repo/extra\"}"}}`),
		larkDelivery("plain-text-mention", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 /github-add acme/repo\"}"}}`),
		larkDelivery("mismatched-mention", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 /github-add acme/repo\"}","mentions":[{"key":"@_user_2"}]}}`),
		larkDelivery("multiple-mentions", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 /github-add acme/repo\"}","mentions":[{"key":"@_user_1"},{"key":"@_user_2"}]}}`),
		larkDelivery("mention-extra-text", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 please /github-add acme/repo\"}","mentions":[{"key":"@_user_1"}]}}`),
		larkDelivery("invalid-mentioned-repository", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 /github-add -acme/repo\"}","mentions":[{"key":"@_user_1"}]}}`),
	} {
		if _, ok := groupSubscription(body, "im.message.receive_v1"); ok {
			t.Fatalf("invalid command accepted: %s", body)
		}
	}
}

func TestReceiverArchivesAndReplaysGroupSubscription(t *testing.T) {
	r, s, a := testReceiverWithArchive(t, 4096, 0)
	body := larkDelivery("subscription", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add Acme/Repo\"}"}}`)
	response := httptest.NewRecorder()
	r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
	if response.Code != http.StatusOK || response.Body.String() != "{}" {
		t.Fatalf("response=%d %q", response.Code, response.Body.String())
	}
	subscribers, err := s.RepositorySubscribers("acme/repo")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" {
		t.Fatalf("subscribers=%v err=%v", subscribers, err)
	}
	if exists, err := a.VerifyExisting(eventKind, qualifiedDeliveryID(eventKind, "subscription"), body); err != nil || !exists {
		t.Fatalf("archive exists=%v err=%v", exists, err)
	}
}

func TestReceiverSendsOneSubscriptionConfirmation(t *testing.T) {
	r, s, _ := testReceiverWithArchive(t, 4096, 0)
	sender := &fakeChatSender{}
	r.chatSender = sender
	body := larkDelivery("mentioned-subscription", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"@_user_1 /github-add HFT-Team/Strategy_Console\"}","mentions":[{"key":"@_user_1","id":{"open_id":"ou_bot"}}]}}`)
	for range 2 {
		response := httptest.NewRecorder()
		r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
		if response.Code != http.StatusOK || response.Body.String() != "{}" {
			t.Fatalf("response=%d %q", response.Code, response.Body.String())
		}
	}
	if len(sender.messages) != 1 {
		t.Fatalf("sent messages=%d", len(sender.messages))
	}
	message := sender.messages[0]
	if message.chatID != "oc_group" || message.message.MsgType != "post" || message.message.Content.Post["zh_cn"].Content[0][0].Text != "已订阅 GitHub 仓库：hft-team/strategy_console" {
		t.Fatalf("message=%+v", message)
	}
	subscribers, err := s.RepositorySubscribers("hft-team/strategy_console")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" {
		t.Fatalf("subscribers=%v err=%v", subscribers, err)
	}
}

func TestReceiverDoesNotSendInvalidSubscriptionConfirmation(t *testing.T) {
	r, _, _ := testReceiverWithArchive(t, 4096, 0)
	sender := &fakeChatSender{}
	r.chatSender = sender
	body := larkDelivery("invalid-subscription", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add acme/repo extra\"}"}}`)
	response := httptest.NewRecorder()
	r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
	if response.Code != http.StatusOK || len(sender.messages) != 0 {
		t.Fatalf("response=%d sent=%d", response.Code, len(sender.messages))
	}
}

func TestReceiverRetriesSubscriptionAfterPostArchiveStoreFailure(t *testing.T) {
	r, s, a := testReceiverWithArchive(t, 4096, 0)
	sender := &fakeChatSender{}
	r.chatSender = sender
	body := larkDelivery("subscription-retry", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add Acme/Repo\"}"}}`)
	calls := 0
	r.addSubscription = func(chatID, repository string) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("subscription store unavailable")
		}
		return s.AddRepositorySubscription(chatID, repository)
	}
	first := httptest.NewRecorder()
	r.Event(first, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first status=%d body=%q", first.Code, first.Body.String())
	}
	if exists, err := a.VerifyExisting(eventKind, qualifiedDeliveryID(eventKind, "subscription-retry"), body); err != nil || !exists {
		t.Fatalf("archive exists=%v err=%v", exists, err)
	}
	second := httptest.NewRecorder()
	r.Event(second, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
	if second.Code != http.StatusOK || second.Body.String() != "{}" {
		t.Fatalf("second status=%d body=%q", second.Code, second.Body.String())
	}
	third := httptest.NewRecorder()
	r.Event(third, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
	if third.Code != http.StatusOK || len(sender.messages) != 1 {
		t.Fatalf("third status=%d sent=%d", third.Code, len(sender.messages))
	}
	subscribers, err := s.RepositorySubscribers("acme/repo")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" || calls != 3 {
		t.Fatalf("subscribers=%v calls=%d err=%v", subscribers, calls, err)
	}
}

func TestReceiverRetriesSubscriptionConfirmationAfterClaimFailure(t *testing.T) {
	r, s, _ := testReceiverWithArchive(t, 4096, 0)
	sender := &fakeChatSender{}
	r.chatSender = sender
	body := larkDelivery("subscription-claim-retry", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add Acme/Repo\"}"}}`)
	claims := 0
	r.claimSubscription = func(deliveryID string) (bool, error) {
		claims++
		if claims == 1 {
			return false, fmt.Errorf("claim store unavailable")
		}
		return s.ClaimSubscriptionReply(deliveryID)
	}
	first := httptest.NewRecorder()
	r.Event(first, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
	if first.Code != http.StatusServiceUnavailable || sender.count() != 0 {
		t.Fatalf("first status=%d sent=%d", first.Code, sender.count())
	}
	subscribers, err := s.RepositorySubscribers("acme/repo")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" {
		t.Fatalf("subscribers=%v err=%v", subscribers, err)
	}
	for range 2 {
		response := httptest.NewRecorder()
		r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("retry status=%d", response.Code)
		}
	}
	if claims != 3 || sender.count() != 1 {
		t.Fatalf("claims=%d sent=%d", claims, sender.count())
	}
}

func TestReceiverConcurrentSubscriptionSendsOneConfirmation(t *testing.T) {
	r, s, _ := testReceiverWithArchive(t, 4096, 0)
	r.inFlight = make(chan struct{}, 2)
	sender := &fakeChatSender{}
	r.chatSender = sender
	body := larkDelivery("concurrent-subscription", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add Acme/Repo\"}"}}`)
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	statuses := make(chan int, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			response := httptest.NewRecorder()
			r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
			statuses <- response.Code
		}()
	}
	for range 2 {
		<-ready
	}
	close(start)
	wg.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("status=%d", status)
		}
	}
	subscribers, err := s.RepositorySubscribers("acme/repo")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" {
		t.Fatalf("subscribers=%v err=%v", subscribers, err)
	}
	if sender.count() != 1 {
		t.Fatalf("sent=%d", sender.count())
	}
}

func TestReceiverKeepsSubscriptionWhenConfirmationSendFails(t *testing.T) {
	r, s, _ := testReceiverWithArchive(t, 4096, 0)
	sender := &fakeChatSender{err: fmt.Errorf("Lark unavailable")}
	r.chatSender = sender
	body := larkDelivery("subscription-send-failure", "im.message.receive_v1", `{"message":{"chat_id":"oc_group","chat_type":"group","message_type":"text","content":"{\"text\":\"/github-add Acme/Repo\"}"}}`)
	for range 2 {
		response := httptest.NewRecorder()
		r.Event(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/event", bytes.NewReader(body)))
		if response.Code != http.StatusOK || response.Body.String() != "{}" {
			t.Fatalf("response=%d %q", response.Code, response.Body.String())
		}
	}
	if len(sender.messages) != 1 {
		t.Fatalf("sent messages=%d", len(sender.messages))
	}
	subscribers, err := s.RepositorySubscribers("acme/repo")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" {
		t.Fatalf("subscribers=%v err=%v", subscribers, err)
	}
}

func TestReceiverOnboardingCallbackArchivesBeforeDuplicateProcessing(t *testing.T) {
	lookup := &fakeGitHubLookup{login: "OctoCat"}
	r, s, a := testReceiverWithLookup(t, lookup)
	if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
		t.Fatal(err)
	}
	body := onboardingCallbackDelivery("callback-1", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat")
	for i := 0; i < 2; i++ {
		response := httptest.NewRecorder()
		r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("callback %d response=%d %q", i, response.Code, response.Body.String())
		}
		var payload struct {
			Toast struct {
				Type string `json:"type"`
			} `json:"toast"`
			Card struct {
				Type string `json:"type"`
				Data struct {
					Header struct {
						Title struct {
							Tag     string `json:"tag"`
							Content string `json:"content"`
						} `json:"title"`
					} `json:"header"`
					Body struct {
						Elements []struct {
							Tag  string `json:"tag"`
							Text struct {
								Tag     string `json:"tag"`
								Content string `json:"content"`
							} `json:"text"`
						} `json:"elements"`
					} `json:"body"`
				} `json:"data"`
			} `json:"card"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Toast.Type != "success" || payload.Card.Type != "raw" || payload.Card.Data.Header.Title.Tag != "plain_text" || payload.Card.Data.Header.Title.Content != "已绑定 GitHub 账号" || len(payload.Card.Data.Body.Elements) != 1 || payload.Card.Data.Body.Elements[0].Tag != "div" || payload.Card.Data.Body.Elements[0].Text.Tag != "plain_text" || payload.Card.Data.Body.Elements[0].Text.Content != "GitHub 用户名：OctoCat" {
			t.Fatalf("callback %d payload=%s", i, response.Body.String())
		}
	}
	if lookup.calls != 1 {
		t.Fatalf("duplicate callback lookups=%d", lookup.calls)
	}
	state, err := s.Onboarding("ou_1")
	if err != nil || state.GitHubLogin != "OctoCat" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if exists, err := a.VerifyExisting(callbackKind, qualifiedDeliveryID(callbackKind, "callback-1"), body); err != nil || !exists {
		t.Fatalf("callback archive exists=%v err=%v", exists, err)
	}
}

func TestReceiverOnboardingCallbackValidationAndOwnership(t *testing.T) {
	tests := []struct {
		name       string
		actionName string
		action     string
		openID     string
		messageID  string
		login      string
		lookup     *fakeGitHubLookup
		wantToast  string
		wantCalls  int
		wantLogin  string
		wantCard   bool
	}{
		{name: "unrelated action", actionName: "other", action: "other", openID: "ou_1", messageID: "om_1", login: "octocat", lookup: &fakeGitHubLookup{login: "OctoCat"}, wantCalls: 0},
		{name: "wrong message", actionName: "submit_github_id", action: "submit_github_id", openID: "ou_1", messageID: "om_other", login: "octocat", lookup: &fakeGitHubLookup{login: "OctoCat"}, wantToast: "无法绑定", wantCalls: 0},
		{name: "invalid login", actionName: "submit_github_id", action: "submit_github_id", openID: "ou_1", messageID: "om_1", login: "octo_cat", lookup: &fakeGitHubLookup{login: "OctoCat"}, wantToast: "格式无效", wantCalls: 0},
		{name: "not found", actionName: "submit_github_id", action: "submit_github_id", openID: "ou_1", messageID: "om_1", login: "missing", lookup: &fakeGitHubLookup{err: &github.NotFoundError{}}, wantToast: "不存在", wantCalls: 1},
		{name: "temporary failure", actionName: "submit_github_id", action: "submit_github_id", openID: "ou_1", messageID: "om_1", login: "octocat", lookup: &fakeGitHubLookup{err: &github.TemporaryError{Err: fmt.Errorf("unavailable")}}, wantToast: "暂时不可用", wantCalls: 1},
		{name: "canonical mismatch", actionName: "submit_github_id", action: "submit_github_id", openID: "ou_1", messageID: "om_1", login: "octocat", lookup: &fakeGitHubLookup{login: "different"}, wantToast: "暂时不可用", wantCalls: 1},
		{name: "success", actionName: "submit_github_id", action: "submit_github_id", openID: "ou_1", messageID: "om_1", login: "octocat", lookup: &fakeGitHubLookup{login: "OctoCat"}, wantToast: "已绑定", wantCalls: 1, wantLogin: "OctoCat", wantCard: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, s, _ := testReceiverWithLookup(t, test.lookup)
			if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
				t.Fatal(err)
			}
			body := onboardingCallbackDelivery(test.name, test.actionName, test.action, test.openID, test.messageID, test.login)
			response := httptest.NewRecorder()
			r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
			if response.Code != http.StatusOK {
				t.Fatalf("response=%d %q", response.Code, response.Body.String())
			}
			if test.wantToast == "" {
				if response.Body.String() != "{}" {
					t.Fatalf("unrelated response=%q", response.Body.String())
				}
			} else if !strings.Contains(response.Body.String(), test.wantToast) {
				t.Fatalf("toast=%q want %q", response.Body.String(), test.wantToast)
			}
			if hasCard := strings.Contains(response.Body.String(), `"card"`); hasCard != test.wantCard {
				t.Fatalf("card=%v want %v response=%q", hasCard, test.wantCard, response.Body.String())
			}
			if test.lookup.calls != test.wantCalls {
				t.Fatalf("lookups=%d want=%d", test.lookup.calls, test.wantCalls)
			}
			state, err := s.Onboarding("ou_1")
			if err != nil || state.GitHubLogin != test.wantLogin {
				t.Fatalf("state=%+v err=%v", state, err)
			}
		})
	}
}

func TestReceiverConcurrentDuplicateCallbackUsesOneLookup(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	lookup := &fakeGitHubLookup{login: "OctoCat", started: started, release: release}
	r, s, _ := testReceiverWithLookup(t, lookup)
	if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
		t.Fatal(err)
	}
	body := onboardingCallbackDelivery("concurrent", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat")
	responses := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			response := httptest.NewRecorder()
			r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
			responses <- response
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("GitHub lookup did not start")
	}
	time.Sleep(25 * time.Millisecond)
	if lookup.calls != 1 {
		t.Fatalf("concurrent lookup calls=%d", lookup.calls)
	}
	close(release)
	for range 2 {
		select {
		case response := <-responses:
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"type":"success"`) {
				t.Fatalf("response=%d %q", response.Code, response.Body.String())
			}
		case <-time.After(time.Second):
			t.Fatal("duplicate callback did not complete")
		}
	}
}

func TestReceiverReturnsBeforeContextIgnoringGitHubLookupFinishes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	lookup := &fakeGitHubLookup{login: "OctoCat", started: started, release: release}
	r, s, _ := testReceiverWithLookup(t, lookup)
	if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
		t.Fatal(err)
	}
	body := onboardingCallbackDelivery("late-github", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat")
	responses := make(chan *httptest.ResponseRecorder, 1)
	startedAt := time.Now()
	go func() {
		response := httptest.NewRecorder()
		r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
		responses <- response
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("GitHub lookup did not start")
	}
	select {
	case response := <-responses:
		if elapsed := time.Since(startedAt); elapsed >= 3*time.Second {
			t.Fatalf("callback exceeded Lark budget: %v", elapsed)
		}
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), temporaryToast) {
			t.Fatalf("response=%d %q", response.Code, response.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("callback remained blocked by GitHub lookup")
	}
	state, err := s.Onboarding("ou_1")
	if err != nil || state.GitHubLogin != "" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	close(release)
	r.Wait()
	state, err = s.Onboarding("ou_1")
	if err != nil || state.GitHubLogin != "" {
		t.Fatalf("late lookup changed state=%+v err=%v", state, err)
	}
}

func TestReceiverCachesTemporaryOutcomeAfterGitHubLookupTimeout(t *testing.T) {
	lookup := &fakeGitHubLookup{waitForContext: true}
	r, s, _ := testReceiverWithLookup(t, lookup)
	if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
		t.Fatal(err)
	}
	body := onboardingCallbackDelivery("lookup-timeout", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat")
	ctx, cancel := context.WithTimeout(context.Background(), 1900*time.Millisecond)
	first := httptest.NewRecorder()
	r.Callback(first, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)).WithContext(ctx))
	cancel()
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), temporaryToast) || strings.Contains(first.Body.String(), `"card"`) {
		t.Fatalf("first response=%d %q", first.Code, first.Body.String())
	}
	result, found, err := s.OnboardingCallbackResult(qualifiedDeliveryID(callbackKind, "lookup-timeout"))
	if err != nil || !found || result.ToastType != "error" || result.Content != temporaryToast {
		t.Fatalf("result=%+v found=%v err=%v", result, found, err)
	}

	replay := httptest.NewRecorder()
	r.Callback(replay, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), temporaryToast) || strings.Contains(replay.Body.String(), `"card"`) {
		t.Fatalf("replay response=%d %q", replay.Code, replay.Body.String())
	}
	if lookup.calls != 1 {
		t.Fatalf("lookup calls=%d", lookup.calls)
	}
	state, err := s.Onboarding("ou_1")
	if err != nil || state.GitHubLogin != "" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
}

func TestReceiverUnrelatedCallbackDeadlineWhileArchiveLocked(t *testing.T) {
	r, _, a := testReceiverWithLookup(t, nil)
	reservation, err := a.Reserve(0)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	body := onboardingCallbackDelivery("unrelated-deadline", "other", "other", "ou_1", "om_1", "octocat")
	response := httptest.NewRecorder()
	started := time.Now()
	r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("callback exceeded Lark budget: %v", elapsed)
	}
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), temporaryToast) {
		t.Fatalf("response=%d %q", response.Code, response.Body.String())
	}
	reservation.Release()
	r.Wait()
}

func TestReceiverOnboardingCallbackDeadlineWhileArchiveLocked(t *testing.T) {
	lookup := &fakeGitHubLookup{login: "OctoCat"}
	r, s, a := testReceiverWithLookup(t, lookup)
	if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
		t.Fatal(err)
	}
	reservation, err := a.Reserve(0)
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	body := onboardingCallbackDelivery("deadline", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat")
	response := httptest.NewRecorder()
	started := time.Now()
	r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
	if elapsed := time.Since(started); elapsed >= 3*time.Second {
		t.Fatalf("callback exceeded Lark budget: %v", elapsed)
	}
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), temporaryToast) {
		t.Fatalf("response=%d %q", response.Code, response.Body.String())
	}
	state, err := s.Onboarding("ou_1")
	if err != nil || state.GitHubLogin != "" || lookup.calls != 0 {
		t.Fatalf("state=%+v lookups=%d err=%v", state, lookup.calls, err)
	}
	reservation.Release()
	r.Wait()
}

func TestReceiverReturnsServiceUnavailableWhenCallbackOutcomeCannotPersist(t *testing.T) {
	r, s, _ := testReceiverWithLookup(t, nil)
	lookup := &fakeGitHubLookup{login: "OctoCat", after: func() { _ = s.Close() }}
	r.github = lookup
	if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(onboardingCallbackDelivery("store-failure", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat"))))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response=%d %q", response.Code, response.Body.String())
	}
}

func TestReceiverCachesCallbackResultsAndRateLimitsValidation(t *testing.T) {
	t.Run("temporary result replays and new delivery is cooled down", func(t *testing.T) {
		lookup := &fakeGitHubLookup{err: &github.TemporaryError{Err: fmt.Errorf("unavailable")}}
		r, s, _ := testReceiverWithLookup(t, lookup)
		if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
			t.Fatal(err)
		}
		first := onboardingCallbackDelivery("temporary", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat")
		for range 2 {
			response := httptest.NewRecorder()
			r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(first)))
			if !strings.Contains(response.Body.String(), temporaryToast) {
				t.Fatalf("temporary response=%q", response.Body.String())
			}
		}
		if lookup.calls != 1 {
			t.Fatalf("temporary lookup calls=%d", lookup.calls)
		}
		cooldown := httptest.NewRecorder()
		r.Callback(cooldown, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(onboardingCallbackDelivery("cooldown", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat"))))
		if !strings.Contains(cooldown.Body.String(), rateLimitToast) || lookup.calls != 1 {
			t.Fatalf("cooldown=%q calls=%d", cooldown.Body.String(), lookup.calls)
		}
	})
	t.Run("not found result replays", func(t *testing.T) {
		lookup := &fakeGitHubLookup{err: &github.NotFoundError{}}
		r, s, _ := testReceiverWithLookup(t, lookup)
		if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
			t.Fatal(err)
		}
		body := onboardingCallbackDelivery("not-found", "submit_github_id", "submit_github_id", "ou_1", "om_1", "missing")
		for range 2 {
			response := httptest.NewRecorder()
			r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(body)))
			if !strings.Contains(response.Body.String(), "不存在") {
				t.Fatalf("not-found response=%q", response.Body.String())
			}
		}
		if lookup.calls != 1 {
			t.Fatalf("not-found lookup calls=%d", lookup.calls)
		}
	})
	t.Run("same bound login skips lookup", func(t *testing.T) {
		lookup := &fakeGitHubLookup{login: "unexpected"}
		r, s, _ := testReceiverWithLookup(t, lookup)
		if err := s.SetOnboardingForm("ou_1", "om_1"); err != nil {
			t.Fatal(err)
		}
		if err := s.SetOnboardingGitHubLogin("ou_1", "om_1", "OctoCat"); err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		r.Callback(response, httptest.NewRequest(http.MethodPost, "/webhook/lark/callback", bytes.NewReader(onboardingCallbackDelivery("same", "submit_github_id", "submit_github_id", "ou_1", "om_1", "octocat"))))
		if !strings.Contains(response.Body.String(), `"type":"success"`) || !strings.Contains(response.Body.String(), `"type":"raw"`) || !strings.Contains(response.Body.String(), "GitHub 用户名：OctoCat") || lookup.calls != 0 {
			t.Fatalf("response=%q calls=%d", response.Body.String(), lookup.calls)
		}
	})
}

func testReceiver(t *testing.T, maxBodyBytes int64) (*Receiver, *store.Store) {
	t.Helper()
	r, s, _ := testReceiverWithArchive(t, maxBodyBytes, 0)
	return r, s
}

func testReceiverWithArchive(t *testing.T, maxBodyBytes int64, minFreeBytes uint64) (*Receiver, *store.Store, *archive.Archive) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), minFreeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return New(s, a, testToken, testAppID, maxBodyBytes, time.Hour, 1, nil, nil), s, a
}

func testReceiverWithLookup(t *testing.T, lookup GitHubLookup) (*Receiver, *store.Store, *archive.Archive) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	return New(s, a, testToken, testAppID, 4096, time.Hour, 8, lookup, nil), s, a
}

type sentChatMessage struct {
	chatID  string
	message lark.Message
}

type fakeChatSender struct {
	mu       sync.Mutex
	messages []sentChatMessage
	err      error
}

func (f *fakeChatSender) SendToChat(_ context.Context, chatID string, message lark.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, sentChatMessage{chatID: chatID, message: message})
	return f.err
}

func (f *fakeChatSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

type fakeGitHubLookup struct {
	login          string
	err            error
	calls          int
	started        chan<- struct{}
	release        <-chan struct{}
	waitForContext bool
	after          func()
}

func (f *fakeGitHubLookup) ValidateUser(ctx context.Context, _ string) (string, error) {
	f.calls++
	if f.started != nil {
		close(f.started)
	}
	if f.waitForContext {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if f.release != nil {
		<-f.release
	}
	if f.after != nil {
		f.after()
	}
	return f.login, f.err
}

func larkDelivery(eventID, eventType, event string) []byte {
	return []byte(fmt.Sprintf(`{"schema":"2.0","header":{"event_id":%q,"event_type":%q,"token":%q,"app_id":%q},"event":%s}`, eventID, eventType, testToken, testAppID, event))
}

func onboardingCallbackDelivery(eventID, name, action, openID, messageID, login string) []byte {
	return larkDelivery(eventID, "card.action.trigger", fmt.Sprintf(`{"operator":{"open_id":%q},"action":{"name":%q,"value":{"action":%q},"form_value":{"github_id":%q}},"context":{"open_message_id":%q}}`, openID, name, action, login, messageID))
}

func delivery(eventID, eventType, token, appID string) []byte {
	return []byte(fmt.Sprintf(`{"schema":"2.0","header":{"event_id":%q,"event_type":%q,"token":%q,"app_id":%q}}`, eventID, eventType, token, appID))
}

type blockingReader struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r blockingReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.release
	return 0, io.EOF
}
