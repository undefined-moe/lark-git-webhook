package forwarder

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

type fakeWorkflowClient struct {
	calls       []string
	cards       []map[string]any
	afterCreate func()
}

func (f *fakeWorkflowClient) CreateInteractive(_ context.Context, card any, _ string) (string, error) {
	f.calls = append(f.calls, "create")
	f.cards = append(f.cards, card.(map[string]any))
	if f.afterCreate != nil {
		f.afterCreate()
	}
	return "om_1", nil
}
func (f *fakeWorkflowClient) CreateInteractiveToOpenID(_ context.Context, _ string, _ any, _ string) (string, error) {
	return "", errors.New("unexpected onboarding send")
}
func (f *fakeWorkflowClient) UpdateInteractive(_ context.Context, _ string, card any) error {
	f.calls = append(f.calls, "update")
	f.cards = append(f.cards, card.(map[string]any))
	return nil
}
func (f *fakeWorkflowClient) AddReaction(_ context.Context, _ string, emoji string) (string, error) {
	f.calls = append(f.calls, "add:"+emoji)
	return "reaction_" + emoji, nil
}
func (f *fakeWorkflowClient) DeleteReaction(_ context.Context, _ string, _ string) error {
	f.calls = append(f.calls, "delete")
	return nil
}

func TestWorkflowStateIsIsolatedByChat(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := &targetClient{}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	now := time.Now()
	payload := []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"workflow_run":{"id":10,"check_suite_id":99,"name":"CI","status":"queued"}}`)
	if _, err := s.AdmitToChats("workflow", "workflow_run", payload, []string{"oc_default", "oc_subscribed"}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive("workflow"); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 || w.sendWorkflows(context.Background(), items[0]) != continueRound {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	for chatID := range map[string]bool{"oc_default": true, "oc_subscribed": true} {
		state, err := s.Workflow(chatID + "\x001\x0099")
		if err != nil || state.ChatID != chatID || state.MessageID != "om_"+chatID {
			t.Fatalf("chat=%s state=%+v err=%v", chatID, state, err)
		}
	}
}

func TestWorkflowRetriesOnlyFailedChatTarget(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now()
	payload := []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"workflow_run":{"id":10,"check_suite_id":99,"name":"CI","status":"queued"}}`)
	if _, err := s.AdmitToChats("workflow", "workflow_run", payload, []string{"oc_default", "oc_subscribed"}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive("workflow"); err != nil {
		t.Fatal(err)
	}
	client := &targetClient{failCreateOnce: map[string]bool{"oc_subscribed": true}}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	w.SetLarkClient(client)
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 || w.sendWorkflows(context.Background(), items[0]) != continueRound {
		t.Fatalf("initial items=%+v err=%v", items, err)
	}
	items, err = s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(items) != 1 || strings.Join(items[0].ChatIDs, ",") != "oc_subscribed" {
		t.Fatalf("retry items=%+v err=%v", items, err)
	}
	if w.sendWorkflows(context.Background(), items[0]) != continueRound {
		t.Fatal("retry result")
	}
	if got := joinCalls(client.chatIDs); got != "oc_default,oc_subscribed,oc_subscribed" {
		t.Fatalf("chat IDs=%s", got)
	}
	items, err = s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("remaining items=%+v err=%v", items, err)
	}
	state, err := s.Workflow("oc_default\x001\x0099")
	if err != nil || state.MessageID != "om_oc_default" {
		t.Fatalf("default state=%+v err=%v", state, err)
	}
	state, err = s.Workflow("oc_subscribed\x001\x0099")
	if err != nil || state.MessageID != "om_oc_subscribed" {
		t.Fatalf("subscribed state=%+v err=%v", state, err)
	}
}

func TestCheckRunRetriesOnlyFailedChatTarget(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now()
	payload := []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"check_run":{"id":10,"name":"unit","status":"queued","check_suite":{"id":99}}}`)
	if _, err := s.AdmitToChats("check", "check_run", payload, []string{"oc_default", "oc_subscribed"}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive("check"); err != nil {
		t.Fatal(err)
	}
	client := &targetClient{failCreateOnce: map[string]bool{"oc_subscribed": true}}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	w.SetLarkClient(client)
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 || w.sendWorkflows(context.Background(), items[0]) != continueRound {
		t.Fatalf("initial items=%+v err=%v", items, err)
	}
	items, err = s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(items) != 1 || strings.Join(items[0].ChatIDs, ",") != "oc_subscribed" {
		t.Fatalf("retry items=%+v err=%v", items, err)
	}
	if w.sendWorkflows(context.Background(), items[0]) != continueRound {
		t.Fatal("retry result")
	}
	if got := joinCalls(client.chatIDs); got != "oc_default,oc_subscribed,oc_subscribed" {
		t.Fatalf("chat IDs=%s", got)
	}
	items, err = s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("remaining items=%+v err=%v", items, err)
	}
	defaultState, err := s.Workflow("oc_default\x001\x0099")
	if err != nil || defaultState.ChatID != "oc_default" || defaultState.MessageID != "om_oc_default" || defaultState.Checks["10"].Name != "unit" {
		t.Fatalf("default state=%+v err=%v", defaultState, err)
	}
	subscribedState, err := s.Workflow("oc_subscribed\x001\x0099")
	if err != nil || subscribedState.ChatID != "oc_subscribed" || subscribedState.MessageID != "om_oc_subscribed" || subscribedState.Checks["10"].Name != "unit" || subscribedState.Key == defaultState.Key {
		t.Fatalf("subscribed state=%+v err=%v", subscribedState, err)
	}
}

func TestWorkflowRunCreatesCardThenReactionAndRerunReplacesIt(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := &fakeWorkflowClient{}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	now := time.Now().UTC()
	first := []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"workflow_run":{"id":10,"check_suite_id":99,"name":"CI","html_url":"https://github.com/acme/repo/actions/runs/10","status":"completed","conclusion":"success","updated_at":"2026-08-01T12:00:00Z"}}`)
	if _, err := s.Enqueue("first", "workflow_run", first, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if result := w.sendWorkflow(context.Background(), items[0]); result != continueRound {
		t.Fatalf("result=%v", result)
	}
	if got := joinCalls(client.calls); got != "create,add:DONE" {
		t.Fatalf("first calls=%s", got)
	}
	second := []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"workflow_run":{"id":10,"check_suite_id":99,"name":"CI","html_url":"https://github.com/acme/repo/actions/runs/10","status":"completed","conclusion":"failure","updated_at":"2026-08-01T13:00:00Z"}}`)
	if _, err := s.Enqueue("second", "workflow_run", second, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	items, err = s.Due(now, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if result := w.sendWorkflow(context.Background(), items[0]); result != continueRound {
		t.Fatalf("result=%v", result)
	}
	if got := joinCalls(client.calls); got != "create,add:DONE,update,delete,add:ERROR" {
		t.Fatalf("rerun calls=%s", got)
	}
	state, err := s.Workflow("1\x0099")
	if err != nil || state.MessageID != "om_1" || state.ReactionType != "ERROR" || state.ReactionID != "reaction_ERROR" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
}

func TestCheckRunUsesSuiteCardAndReactionMappingIsFixed(t *testing.T) {
	item := store.Item{Event: "check_run", RawJSON: []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"check_run":{"id":4,"name":"unit","status":"completed","conclusion":"success","details_url":"https://ci.example.test/4","updated_at":"2026-08-01T12:00:00Z","check_suite":{"id":99}}}`)}
	update, err := workflowUpdate(item)
	if err != nil || update.SuiteID != "99" || update.Check == nil || update.Check.ID != "4" {
		t.Fatalf("update=%+v err=%v", update, err)
	}
	for conclusion, want := range map[string]string{
		"success": "DONE", "failure": "ERROR", "timed_out": "ERROR", "action_required": "ERROR",
		"cancelled": "CrossMark", "skipped": "CrossMark", "neutral": "CrossMark", "stale": "CrossMark",
	} {
		if got := workflowReaction(conclusion); got != want {
			t.Fatalf("%s reaction=%q want=%q", conclusion, got, want)
		}
	}
	if workflowReaction("unknown") != "" {
		t.Fatal("unknown conclusion received a reaction")
	}
}

func TestWorkflowStopsWhenPostSendStateCannotPersist(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeWorkflowClient{afterCreate: func() { _ = s.Close() }}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	item := store.Item{Event: "workflow_run", RawJSON: []byte(`{"repository":{"id":1,"full_name":"acme/repo"},"workflow_run":{"id":10,"check_suite_id":99,"name":"CI","status":"queued"}}`)}
	if result := w.sendWorkflow(context.Background(), item); result != stopWorker {
		t.Fatalf("result=%v", result)
	}
}

func TestWorkflowCardEscapesUntrustedMarkdownAndOnlyLinksGitHub(t *testing.T) {
	state := store.WorkflowState{
		WorkflowName: "CI [trusted]",
		Status:       "queued](https://evil.example/)",
		URL:          "https://github.com/acme/repo?x=)[evil](https://evil.example)",
		Checks: map[string]store.WorkflowCheck{
			"1": {Name: "unit](https://evil.example/)", Status: "ok_*", DetailsURL: "https://github.com/acme/repo?x=)[evil](https://evil.example)"},
			"2": {Name: "lint", Status: "success", DetailsURL: "https://github.com/acme/repo/runs/2"},
		},
	}
	card := makeWorkflowCard(state)
	content := card["elements"].([]any)[0].(map[string]any)["text"].(map[string]string)["content"]
	for _, unwanted := range []string{"](" + "https://evil.example", "[unit](https://evil.example/check)"} {
		if strings.Contains(content, unwanted) {
			t.Fatalf("untrusted Markdown/link was emitted: %q", content)
		}
	}
	if !strings.Contains(content, `queued\]\(https://evil.example/\)`) || !strings.Contains(content, `unit\]\(https://evil.example/\)`) {
		t.Fatalf("untrusted delimiters were not escaped: %q", content)
	}
	if !strings.Contains(content, `[lint](https://github.com/acme/repo/runs/2): success`) {
		t.Fatalf("valid GitHub check link missing: %q", content)
	}
	for value, want := range map[string]bool{
		"https://github.com/acme/repo":              true,
		"https://github.com.evil.example/acme/repo": false,
		"http://github.com/acme/repo":               false,
		"https://github.com":                        false,
		"https://github.com/acme/repo?x=)[evil]":    false,
		"https://github.com/acme\\repo":             false,
	} {
		if got := workflowGitHubURL(value); got != want {
			t.Fatalf("workflowGitHubURL(%q)=%v want=%v", value, got, want)
		}
	}
}

func joinCalls(calls []string) string {
	result := ""
	for i, call := range calls {
		if i > 0 {
			result += ","
		}
		result += call
	}
	return result
}

func TestWorkflowUsesImmutableRepositoryIDAndRerunClearsChecks(t *testing.T) {
	item := store.Item{Event: "workflow_run", RawJSON: []byte(`{"repository":{"id":42,"full_name":"renamed/repo"},"workflow_run":{"id":10,"check_suite_id":99,"run_attempt":2,"run_started_at":"2026-08-01T12:00:00Z","head_branch":"main","head_sha":"abc","status":"queued"}}`)}
	update, err := workflowUpdate(item)
	if err != nil || update.RepositoryID != "42" || update.SuiteID != "99" || update.RunAttempt != 2 || update.HeadSHA != "abc" || update.RunStartedAt.IsZero() {
		t.Fatalf("update=%+v err=%v", update, err)
	}
	if _, err := workflowUpdate(store.Item{Event: "workflow_run", RawJSON: []byte(`{"repository":{"id":42,"full_name":"x/y"},"workflow_run":{"id":10}}`)}); err == nil {
		t.Fatal("missing suite ID accepted")
	}
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now()
	state, err := s.UpsertWorkflow(store.WorkflowUpdate{RepositoryID: "42", Repository: "old/name", SuiteID: "99", RunAttempt: 1, Status: "completed", Conclusion: "success", UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpsertWorkflow(store.WorkflowUpdate{RepositoryID: "42", Repository: "renamed/repo", SuiteID: "99", RunAttempt: 1, Check: &store.WorkflowCheck{ID: "c", Status: "completed", Conclusion: "success", CompletedAt: at}}); err != nil {
		t.Fatal(err)
	}
	state, err = s.UpsertWorkflow(update)
	if err != nil || state.Key != "42\x0099" || state.Conclusion != "" || len(state.Checks) != 0 {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if len(workflowUUID(state.Key)) > 50 {
		t.Fatal("workflow UUID exceeds Lark limit")
	}
	card := makeWorkflowCard(state)
	if card["config"].(map[string]any)["update_multi"] != true {
		t.Fatal("card does not opt into multi-update")
	}
}
