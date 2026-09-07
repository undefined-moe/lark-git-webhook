package forwarder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
)

type fakeOnboardingClient struct {
	messageID string
	err       error
	openIDs   []string
	uuids     []string
	onCreate  func()
}

func (f *fakeOnboardingClient) CreateInteractive(context.Context, any, string) (string, error) {
	return "", errors.New("unexpected workflow card")
}
func (f *fakeOnboardingClient) CreateInteractiveToOpenID(_ context.Context, openID string, _ any, uuid string) (string, error) {
	f.openIDs = append(f.openIDs, openID)
	f.uuids = append(f.uuids, uuid)
	if f.onCreate != nil {
		f.onCreate()
	}
	return f.messageID, f.err
}
func (f *fakeOnboardingClient) UpdateInteractive(context.Context, string, any) error {
	return errors.New("unexpected workflow card update")
}
func (f *fakeOnboardingClient) AddReaction(context.Context, string, string) (string, error) {
	return "", errors.New("unexpected workflow reaction")
}
func (f *fakeOnboardingClient) DeleteReaction(context.Context, string, string) error {
	return errors.New("unexpected workflow reaction delete")
}

func TestOnboardingSendsOneFormAndNeverFallsBackToBatch(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	client := &fakeOnboardingClient{messageID: "om_1"}
	var ordinarySends atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.onCreate = cancel
	w := NewWorker(s, func(context.Context, lark.Message) error {
		ordinarySends.Add(1)
		return nil
	}, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	w.SetLarkClient(client)
	if _, err := s.Enqueue("entered-1", "lark_event", enteredEvent("ou_1", "oc_1"), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	if ordinarySends.Load() != 0 || len(client.openIDs) != 1 || client.openIDs[0] != "ou_1" {
		t.Fatalf("ordinary=%d openIDs=%v", ordinarySends.Load(), client.openIDs)
	}
	if client.uuids[0] != onboardingUUID("ou_1") || client.uuids[0] == onboardingUUID("ou_2") {
		t.Fatalf("UUID is not deterministic and user-specific: %q", client.uuids[0])
	}
	state, err := s.Onboarding("ou_1")
	if err != nil || state.FormMessageID != "om_1" {
		t.Fatalf("state=%+v err=%v", state, err)
	}

	if _, err := s.Enqueue("entered-2", "lark_event", enteredEvent("ou_1", "oc_1"), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	item, err := s.Due(time.Now(), 1)
	if err != nil || len(item) != 1 {
		t.Fatalf("item=%+v err=%v", item, err)
	}
	if result := w.sendOnboarding(context.Background(), item[0]); result != continueRound {
		t.Fatalf("result=%v", result)
	}
	if len(client.openIDs) != 1 {
		t.Fatalf("form resent %d times", len(client.openIDs))
	}
}

func TestOnboardingRetriesLarkFailures(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	client := &fakeOnboardingClient{err: &lark.RetryError{Err: errors.New("temporary")}}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	w.SetLarkClient(client)
	now := time.Now()
	if _, err := s.Enqueue("entered", "lark_event", enteredEvent("ou_1", "oc_1"), now, time.Hour); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if result := w.sendOnboarding(context.Background(), items[0]); result != continueRound {
		t.Fatalf("result=%v", result)
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 1 || len(client.openIDs) != 1 {
		t.Fatalf("stats=%+v calls=%d err=%v", stats, len(client.openIDs), err)
	}
	retry, err := s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(retry) != 1 || retry[0].Attempts != 1 {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
}

func enteredEvent(openID, chatID string) []byte {
	return []byte(`{"schema":"2.0","header":{"event_type":"im.chat.access_event.bot_p2p_chat_entered_v1"},"event":{"operator_id":{"open_id":"` + openID + `"},"chat_id":"` + chatID + `"}}`)
}

var _ workflowClient = (*fakeOnboardingClient)(nil)
