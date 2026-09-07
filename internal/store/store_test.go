package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestEnqueuePersistsAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	now := time.Now().UTC()
	s := openTestStore(t, path, 10, 1024, 10)
	duplicate, err := s.Enqueue("delivery-1", "push", []byte(`{"ok":true}`), now, time.Hour)
	if err != nil || duplicate {
		t.Fatalf("first enqueue duplicate=%v err=%v", duplicate, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1024, 10)
	defer s.Close()
	duplicate, err = s.Enqueue("delivery-1", "push", []byte(`{"ok":true}`), now.Add(time.Minute), time.Hour)
	if err != nil || !duplicate {
		t.Fatalf("duplicate enqueue duplicate=%v err=%v", duplicate, err)
	}
	items, err := s.Due(now.Add(time.Minute), 10)
	if err != nil || len(items) != 1 || items[0].DeliveryID != "delivery-1" {
		t.Fatalf("items=%v err=%v", items, err)
	}
}

func TestRepositorySubscriptionPersistsIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1<<20, 10)
	for range 2 {
		if err := s.AddRepositorySubscription("oc_group", "acme/repo"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1<<20, 10)
	defer s.Close()
	subscribers, err := s.RepositorySubscribers("acme/repo")
	if err != nil || len(subscribers) != 1 || subscribers[0] != "oc_group" {
		t.Fatalf("subscribers=%v err=%v", subscribers, err)
	}
}

func TestClaimSubscriptionReplyPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1<<20, 10)
	if _, err := s.AdmitArchiveOnly("delivery", "lark_event", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimSubscriptionReply("delivery")
	if err != nil || !claimed {
		t.Fatalf("first claim claimed=%v err=%v", claimed, err)
	}
	claimed, err = s.ClaimSubscriptionReply("delivery")
	if err != nil || claimed {
		t.Fatalf("duplicate claim claimed=%v err=%v", claimed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1<<20, 10)
	defer s.Close()
	claimed, err = s.ClaimSubscriptionReply("delivery")
	if err != nil || claimed {
		t.Fatalf("reopened claim claimed=%v err=%v", claimed, err)
	}
}

func TestClaimSubscriptionReplySupportsLegacyDeliveryState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1<<20, 10)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(deliveryStateBucket).Put([]byte("legacy-delivery"), []byte(`{"event":"lark_event","status":"archive_only","archived":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1<<20, 10)
	claimed, err := s.ClaimSubscriptionReply("legacy-delivery")
	if err != nil || !claimed {
		t.Fatalf("first claim claimed=%v err=%v", claimed, err)
	}
	claimed, err = s.ClaimSubscriptionReply("legacy-delivery")
	if err != nil || claimed {
		t.Fatalf("duplicate claim claimed=%v err=%v", claimed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1<<20, 10)
	defer s.Close()
	claimed, err = s.ClaimSubscriptionReply("legacy-delivery")
	if err != nil || claimed {
		t.Fatalf("reopened claim claimed=%v err=%v", claimed, err)
	}
}

func TestAdmissionPersistsIdentityAndArchiveState(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 1)
	defer s.Close()
	now := time.Now()
	if duplicate, err := s.Admit("delivery", "push", []byte(`{"id":1}`), now, time.Hour); err != nil || duplicate {
		t.Fatalf("admit duplicate=%v err=%v", duplicate, err)
	}
	pending, err := s.PendingArchives(10)
	if err != nil || len(pending) != 1 || pending[0].DeliveryID != "delivery" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if err := s.FinalizeArchive("delivery"); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 10)
	if err != nil || len(items) != 1 || !items[0].Archived {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if err := s.Delete(items); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDedupe(now.Add(2*time.Hour), 100); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := s.Admit("delivery", "push", []byte(`{"id":1}`), now.Add(2*time.Hour), time.Hour); err != nil || !duplicate {
		t.Fatalf("permanent duplicate=%v err=%v", duplicate, err)
	}
	if _, err := s.Admit("delivery", "issues", []byte(`{"id":1}`), now.Add(2*time.Hour), time.Hour); !errors.Is(err, ErrDeliveryConflict) {
		t.Fatalf("conflict=%v", err)
	}
}

func TestFinalizeArchiveKeepsStatsExactAfterDelete(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10)
	defer s.Close()
	now := time.Now()
	for i := 0; i < 3; i++ {
		deliveryID := fmt.Sprintf("delivery-%d", i)
		if _, err := s.Admit(deliveryID, "push", []byte(`{"id":1}`), now, time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := s.FinalizeArchive(deliveryID); err != nil {
			t.Fatal(err)
		}
		items, err := s.Due(now, 10)
		if err != nil || len(items) != 1 {
			t.Fatalf("items=%v err=%v", items, err)
		}
		if err := s.Delete(items); err != nil {
			t.Fatal(err)
		}
		stats, err := s.Stats()
		if err != nil || stats.Items != 0 || stats.Bytes != 0 {
			t.Fatalf("iteration %d stats=%+v err=%v", i, stats, err)
		}
	}
}

func TestDedupeExpiryAndBoundedEviction(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1024, 2)
	defer s.Close()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, id := range []string{"first", "second", "third"} {
		if _, err := s.Enqueue(id, "push", []byte(`{}`), base.Add(time.Duration(i)*time.Second), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	duplicate, err := s.Enqueue("first", "push", []byte(`{}`), base.Add(3*time.Second), time.Hour)
	if err != nil || duplicate {
		t.Fatalf("earliest dedupe record was not evicted: duplicate=%v err=%v", duplicate, err)
	}
	if err := s.CleanupDedupe(base.Add(2*time.Hour), 100); err != nil {
		t.Fatal(err)
	}
	duplicate, err = s.Enqueue("second", "push", []byte(`{}`), base.Add(2*time.Hour), time.Hour)
	if err != nil || duplicate {
		t.Fatalf("expired record survived cleanup: duplicate=%v err=%v", duplicate, err)
	}
}

func TestCapacityDoesNotPartiallyWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 1, 1000, 10)
	defer func() { _ = s.Close() }()
	now := time.Now()
	if _, err := s.Enqueue("one", "push", []byte(`1234567890`), now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue("two", "push", []byte(`1`), now, time.Hour); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full, got %v", err)
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 1 || stats.Bytes == 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 9, 10)
	if _, err := s.Enqueue("three", "push", []byte(`1`), now, time.Hour); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected lowered byte limit to reject enqueue, got %v", err)
	}
}

func TestRateLimitAndCooldownSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := openTestStore(t, path, 10, 1024, 10)
	for i := 0; i < 5; i++ {
		if wait, err := s.ReserveRequest(base); err != nil || wait != 0 {
			t.Fatalf("reserve %d: wait=%v err=%v", i, wait, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1024, 10)
	if wait, err := s.ReserveRequest(base); err != nil || wait <= 0 {
		t.Fatalf("expected persisted 5/s limit, got %v, %v", wait, err)
	}
	if _, err := s.HandleFailure(nil, base, base, "", 10*time.Second, 1, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1024, 10)
	defer s.Close()
	if wait, err := s.ReserveRequest(base); err != nil || wait != 10*time.Second {
		t.Fatalf("expected persisted cooldown, got %v, %v", wait, err)
	}
}

func TestDeadLetterRequeue(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1024, 10)
	defer s.Close()
	now := time.Now()
	if _, err := s.Enqueue("delivery-1", "push", []byte(`{}`), now, time.Hour); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.HandleFailure(items, now, now, "permanent failure", 0, 1, true); err != nil {
		t.Fatal(err)
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 0 || stats.DeadLetters != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if err := s.RequeueDeadLetter(items[0].Sequence, now); err != nil {
		t.Fatal(err)
	}
	stats, err = s.Stats()
	if err != nil || stats.Items != 1 || stats.DeadLetters != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestHandleFailureAtomicallySplitsMixedAttempts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1024, 10)
	now := time.Now()
	if _, err := s.Enqueue("one", "push", []byte(`{}`), now, time.Hour); err != nil {
		t.Fatal(err)
	}
	first, _ := s.Due(now, 1)
	if _, err := s.HandleFailure(first, now, now.Add(time.Minute), "retry", 0, 2, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue("two", "push", []byte(`{}`), now, time.Hour); err != nil {
		t.Fatal(err)
	}
	items, _ := s.Due(now.Add(time.Minute), 10)
	retryAt := now.Add(2 * time.Minute)
	result, err := s.HandleFailure(items, now, retryAt, "mixed", 3*time.Second, 2, false)
	if err != nil || result.DeadLettered != 1 || result.Retried != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1024, 10)
	defer s.Close()
	stats, err := s.Stats()
	if err != nil || stats.Items != 1 || stats.DeadLetters != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	retried, err := s.Due(retryAt, 10)
	if err != nil || len(retried) != 1 || retried[0].Attempts != 1 || retried[0].LastError != "mixed" || !retried[0].NextAttempt.Equal(retryAt.UTC()) {
		t.Fatalf("retried=%+v err=%v", retried, err)
	}
	if wait, err := s.ReserveRequest(now); err != nil || wait <= 0 {
		t.Fatalf("cooldown was not atomically persisted: wait=%v err=%v", wait, err)
	}
}

func TestRateLimitHundredPerMinuteSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	base := time.Now().UTC().Truncate(time.Second)
	s := openTestStore(t, path, 10, 1024, 10)
	for second := 0; second < 20; second++ {
		at := base.Add(time.Duration(second) * time.Second)
		for i := 0; i < 5; i++ {
			if wait, err := s.ReserveRequest(at); err != nil || wait != 0 {
				t.Fatalf("reserve %d/%d: %v %v", second, i, wait, err)
			}
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1024, 10)
	defer s.Close()
	if wait, err := s.ReserveRequest(base.Add(20 * time.Second)); err != nil || wait <= 0 {
		t.Fatalf("101st request did not wait: wait=%v err=%v", wait, err)
	}
}

func TestOpenRebuildsAndConvergesDedupe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	now := time.Now().UTC()
	s := openTestStore(t, path, 10, 1024, 3)
	for i, id := range []string{"old", "middle", "new"} {
		if _, err := s.Enqueue(id, "push", []byte(`{}`), now.Add(time.Duration(i)*time.Second), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1024, 1)
	defer s.Close()
	duplicate, err := s.Enqueue("new", "push", []byte(`{}`), now.Add(4*time.Second), time.Hour)
	if err != nil || !duplicate {
		t.Fatalf("latest record not rebuilt: duplicate=%v err=%v", duplicate, err)
	}
	duplicate, err = s.Enqueue("old", "push", []byte(`{}`), now.Add(4*time.Second), time.Hour)
	if err != nil || duplicate {
		t.Fatalf("old record was not evicted: duplicate=%v err=%v", duplicate, err)
	}
}

func TestDeadLetterFullRetainsActiveItem(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now()
	for _, id := range []string{"one", "two"} {
		if _, err := s.Enqueue(id, "push", []byte(`{}`), now, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	items, _ := s.Due(now, 10)
	if _, err := s.HandleFailure(items[:1], now, now.Add(time.Minute), "permanent", 0, 1, true); err != nil {
		t.Fatal(err)
	}
	result, err := s.HandleFailure(items[1:], now, now.Add(time.Minute), "permanent", 0, 1, true)
	if err != nil || !result.DeadLetterFull || result.Retried != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	stats, _ := s.Stats()
	if stats.Items != 1 || stats.DeadLetters != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestWorkflowStatePersistsAndRejectsOlderUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1<<20, 10)
	newer := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	state, err := s.UpsertWorkflow(WorkflowUpdate{Repository: "acme/repo", SuiteID: "42", WorkflowName: "CI", Status: "completed", Conclusion: "success", UpdatedAt: newer})
	if err != nil || state.Key == "" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if err := s.SetWorkflowMessage(state.Key, "om_1"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetWorkflowReaction(state.Key, "reaction_1", "DONE"); err != nil {
		t.Fatal(err)
	}
	_, err = s.UpsertWorkflow(WorkflowUpdate{Repository: "acme/repo", SuiteID: "42", Status: "in_progress", UpdatedAt: newer.Add(-time.Minute), Check: &WorkflowCheck{ID: "check-1", Name: "unit", Status: "completed", Conclusion: "success", UpdatedAt: newer}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1<<20, 10)
	defer s.Close()
	persisted, err := s.Workflow(state.Key)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "completed" || persisted.Conclusion != "success" || persisted.MessageID != "om_1" || persisted.ReactionID != "reaction_1" || persisted.Checks["check-1"].Name != "unit" {
		t.Fatalf("persisted=%+v", persisted)
	}
}

func TestArchiveOnlyAdmissionDoesNotQueue(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10)
	defer s.Close()
	if duplicate, err := s.AdmitArchiveOnly("delivery", "workflow_job", []byte(`{"id":1}`)); err != nil || duplicate {
		t.Fatalf("admit duplicate=%v err=%v", duplicate, err)
	}
	if err := s.FinalizeArchive("delivery"); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if duplicate, err := s.AdmitArchiveOnly("delivery", "workflow_job", []byte(`{"id":1}`)); err != nil || !duplicate {
		t.Fatalf("duplicate=%v err=%v", duplicate, err)
	}
}

func openTestStore(t *testing.T, path string, maxItems, maxBytes, dedupeMaxItems uint64) *Store {
	t.Helper()
	if maxBytes == 1024 {
		maxBytes = 1 << 20
	}
	s, err := Open(path, maxItems, maxBytes, dedupeMaxItems, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWorkflowRejectsLateNonterminalEventsWithoutAuthority(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10)
	defer s.Close()
	at := time.Now().UTC()
	state, err := s.UpsertWorkflow(WorkflowUpdate{RepositoryID: "1", Repository: "acme/repo", SuiteID: "2", RunAttempt: 1, Status: "completed", Conclusion: "success", UpdatedAt: at, Check: &WorkflowCheck{ID: "check", Status: "completed", Conclusion: "success", CompletedAt: at}})
	if err != nil {
		t.Fatal(err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: "1", Repository: "acme/repo", SuiteID: "2", Status: "in_progress", Check: &WorkflowCheck{ID: "check", Status: "in_progress"}})
	if err != nil || state.Status != "completed" || state.Conclusion != "success" || state.Checks["check"].Status != "completed" {
		t.Fatalf("late state=%+v err=%v", state, err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: "1", Repository: "acme/repo", SuiteID: "2", RunAttempt: 2, Status: "queued"})
	if err != nil || state.Status != "queued" || state.Conclusion != "" || len(state.Checks) != 0 {
		t.Fatalf("rerun state=%+v err=%v", state, err)
	}
}

func TestOnboardingStatePersistsAndEnforcesFormOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1<<20, 10)
	if _, err := s.Onboarding("ou_123"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing onboarding state error=%v", err)
	}
	if err := s.SetOnboardingForm("ou_123", "om_123"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOnboardingForm("ou_123", "om_123"); err != nil {
		t.Fatalf("same form ID was not idempotent: %v", err)
	}
	if err := s.SetOnboardingForm("ou_123", "om_456"); !errors.Is(err, ErrOnboardingFormConflict) {
		t.Fatalf("form overwrite error=%v", err)
	}
	if err := s.SetOnboardingGitHubLogin("ou_123", "om_456", "octocat"); !errors.Is(err, ErrOnboardingMessageMismatch) {
		t.Fatalf("wrong message ownership error=%v", err)
	}
	if err := s.SetOnboardingGitHubLogin("ou_other", "om_123", "octocat"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other user ownership error=%v", err)
	}
	if err := s.SetOnboardingGitHubLogin("ou_123", "om_123", "octocat"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOnboardingGitHubLogin("ou_123", "om_123", "hubot"); err != nil {
		t.Fatalf("valid resubmission error=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTestStore(t, path, 10, 1<<20, 10)
	defer s.Close()
	state, err := s.Onboarding("ou_123")
	if err != nil {
		t.Fatal(err)
	}
	if state.OpenID != "ou_123" || state.FormMessageID != "om_123" || state.GitHubLogin != "hubot" {
		t.Fatalf("state=%+v", state)
	}
}

func TestOnboardingValidationReservationAndCallbackResultPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s := openTestStore(t, path, 10, 1<<20, 10)
	if err := s.SetOnboardingForm("ou_123", "om_123"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	reserved, err := s.ReserveOnboardingValidation("ou_123", "om_123", now)
	if err != nil || !reserved {
		t.Fatalf("initial reservation=%v err=%v", reserved, err)
	}
	if _, err := s.ReserveOnboardingValidation("ou_123", "om_123", now.Add(59*time.Second)); !errors.Is(err, ErrOnboardingValidationCooldown) {
		t.Fatalf("cooldown error=%v", err)
	}
	if reserved, err := s.ReserveOnboardingValidation("ou_123", "om_123", now.Add(time.Minute)); err != nil || !reserved {
		t.Fatalf("post-cooldown reservation=%v err=%v", reserved, err)
	}
	if pending, claimed, err := s.ClaimOnboardingCallbackResult("lark_callback:delivery", now, 2500*time.Millisecond); err != nil || !claimed || pending.PendingAt.IsZero() {
		t.Fatalf("first callback claim=%+v claimed=%v err=%v", pending, claimed, err)
	}
	if pending, claimed, err := s.ClaimOnboardingCallbackResult("lark_callback:delivery", now.Add(time.Second), 2500*time.Millisecond); err != nil || claimed || pending.PendingAt.IsZero() {
		t.Fatalf("pending callback claim=%+v claimed=%v err=%v", pending, claimed, err)
	}
	result := OnboardingCallbackResult{ToastType: "success", Content: "GitHub 用户名已绑定"}
	if err := s.SetOnboardingCallbackResult("lark_callback:delivery", result); err != nil {
		t.Fatal(err)
	}
	if cached, claimed, err := s.ClaimOnboardingCallbackResult("lark_callback:delivery", now.Add(time.Second), 2500*time.Millisecond); err != nil || claimed || cached.ToastType != "success" {
		t.Fatalf("cached callback claim=%+v claimed=%v err=%v", cached, claimed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, 10, 1<<20, 10)
	defer s.Close()
	state, err := s.Onboarding("ou_123")
	if err != nil || state.LastValidationAt.IsZero() {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	got, found, err := s.OnboardingCallbackResult("lark_callback:delivery")
	if err != nil || !found || got != result {
		t.Fatalf("result=%+v found=%v err=%v", got, found, err)
	}
}

func TestOnboardingContextMutationAbortsAfterWriteLockWait(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10)
	defer s.Close()
	if err := s.SetOnboardingForm("ou_123", "om_123"); err != nil {
		t.Fatal(err)
	}
	locked := make(chan struct{})
	release := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- s.db.Update(func(*bolt.Tx) error {
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- s.SetOnboardingGitHubLoginContext(ctx, "ou_123", "om_123", "OctoCat")
	}()
	<-ctx.Done()
	close(release)
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late mutation error=%v", err)
	}
	state, err := s.Onboarding("ou_123")
	if err != nil || state.GitHubLogin != "" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
}

func TestWorkflowAttemptCutoffRejectsDelayedChecks(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10)
	defer s.Close()
	firstStart := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	secondStart := firstStart.Add(time.Hour)
	base := WorkflowUpdate{RepositoryID: "1", Repository: "acme/repo", SuiteID: "2"}
	// Check deliveries may precede the first workflow_run delivery and remain
	// cacheable for attempt one.
	state, err := s.UpsertWorkflow(WorkflowUpdate{RepositoryID: base.RepositoryID, Repository: base.Repository, SuiteID: base.SuiteID, Check: &WorkflowCheck{ID: "old", Status: "completed", Conclusion: "success", CompletedAt: firstStart.Add(time.Minute)}})
	if err != nil || len(state.Checks) != 1 {
		t.Fatalf("first check state=%+v err=%v", state, err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: base.RepositoryID, Repository: base.Repository, SuiteID: base.SuiteID, RunAttempt: 1, RunStartedAt: firstStart, Status: "completed", Conclusion: "success", UpdatedAt: firstStart.Add(2 * time.Minute)})
	if err != nil || len(state.Checks) != 1 {
		t.Fatalf("attempt one state=%+v err=%v", state, err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: base.RepositoryID, Repository: base.Repository, SuiteID: base.SuiteID, Check: &WorkflowCheck{ID: "queued", Status: "queued"}})
	if err != nil || state.Checks["queued"].Status != "queued" {
		t.Fatalf("attempt one untimed check state=%+v err=%v", state, err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: base.RepositoryID, Repository: base.Repository, SuiteID: base.SuiteID, RunAttempt: 2, RunStartedAt: secondStart, Status: "in_progress", UpdatedAt: secondStart})
	if err != nil || len(state.Checks) != 0 || !state.AttemptStartedAt.Equal(secondStart) {
		t.Fatalf("attempt two state=%+v err=%v", state, err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: base.RepositoryID, Repository: base.Repository, SuiteID: base.SuiteID, Check: &WorkflowCheck{ID: "old", Status: "completed", Conclusion: "success", CompletedAt: firstStart.Add(time.Minute)}})
	if err != nil || len(state.Checks) != 0 {
		t.Fatalf("delayed check polluted state=%+v err=%v", state, err)
	}
	state, err = s.UpsertWorkflow(WorkflowUpdate{RepositoryID: base.RepositoryID, Repository: base.Repository, SuiteID: base.SuiteID, Check: &WorkflowCheck{ID: "new", Status: "in_progress", StartedAt: secondStart.Add(time.Minute)}})
	if err != nil || len(state.Checks) != 1 || state.Checks["new"].Status != "in_progress" {
		t.Fatalf("current check state=%+v err=%v", state, err)
	}
}
