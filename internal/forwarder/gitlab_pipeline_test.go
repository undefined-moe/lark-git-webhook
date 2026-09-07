package forwarder

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func gitlabPipelineBody(status, createdAt, finishedAt string) []byte {
	attrs := map[string]any{
		"id": 99, "iid": 99, "ref": "main", "tag": false,
		"sha":    "2222222222222222222222222222222222222222",
		"status": status, "source": "push", "created_at": createdAt,
		"url": "https://gitlab.example.com/acme/backend/-/pipelines/99",
	}
	if finishedAt != "" {
		attrs["finished_at"] = finishedAt
	}
	payload := map[string]any{
		"object_kind":       "pipeline",
		"object_attributes": attrs,
		"project": map[string]any{
			"id": 17, "name": "Backend", "path_with_namespace": "acme/backend",
			"web_url": "https://gitlab.example.com/acme/backend",
		},
		"commit": map[string]any{"id": "2222222222222222222222222222222222222222", "title": "fix", "message": "fix things", "timestamp": createdAt, "url": "https://gitlab.example.com/acme/backend/-/commit/2222222222222222222222222222222222222222"},
		"user":   map[string]any{"name": "John Smith", "username": "jsmith"},
	}
	raw, _ := json.Marshal(payload)
	return raw
}

func admitPipeline(t *testing.T, s *store.Store, id, status, createdAt, finishedAt string) {
	t.Helper()
	if _, err := s.AdmitToChats(id, "gitlab:pipeline", gitlabPipelineBody(status, createdAt, finishedAt), []string{"oc_default"}, time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive(id); err != nil {
		t.Fatal(err)
	}
}

func TestGitlabPipelineCardProgressesCreatedRunningSuccess(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := &fakeWorkflowClient{}
	m := &metrics.Metrics{}
	w := NewWorker(s, nil, m, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	admitPipeline(t, s, "pipeline-created", "created", "2026-08-01 12:00:00 UTC", "")
	admitPipeline(t, s, "pipeline-running", "running", "2026-08-01 12:00:00 UTC", "")
	admitPipeline(t, s, "pipeline-success", "success", "2026-08-01 12:00:00 UTC", "2026-08-01 12:05:00 UTC")
	for i := 0; i < 3; i++ {
		items, err := s.Due(time.Now(), 1)
		if err != nil || len(items) != 1 {
			t.Fatalf("round %d items=%+v err=%v", i, items, err)
		}
		if result := w.sendGitlabPipelines(context.Background(), items[0]); result != continueRound {
			t.Fatalf("round %d result=%v", i, result)
		}
	}
	if got := joinCalls(client.calls); got != "create,update,update,add:DONE" {
		t.Fatalf("calls=%s", got)
	}
	state, err := s.Workflow("oc_default\x00gitlab:17\x0099")
	if err != nil || state.Status != "success" || state.Conclusion != "success" || state.ReactionType != "DONE" || state.MessageID != "om_1" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	if !strings.HasPrefix(state.Key, "oc_default\x00gitlab:") {
		t.Fatalf("key is not gitlab-namespaced: %q", state.Key)
	}
	if len(state.RepositoryID) <= len("gitlab:") || !strings.HasPrefix(state.RepositoryID, "gitlab:") {
		t.Fatalf("repository ID is not gitlab-namespaced: %q", state.RepositoryID)
	}
	response := httptest.NewRecorder()
	m.Handler(func() metrics.Snapshot { return metrics.Snapshot{} }).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	for _, line := range []string{
		`lark_git_webhook_gitlab_events_delivered_total{event="pipeline"} 3`,
		"lark_git_webhook_workflow_cards_created_total 1",
		"lark_git_webhook_workflow_cards_updated_total 2",
		"lark_git_webhook_workflow_reactions_added_total 1",
	} {
		if !strings.Contains(response.Body.String(), line) {
			t.Fatalf("missing %q in %s", line, response.Body.String())
		}
	}
}

func TestGitlabPipelineFailureAndCanceledReactions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    string
		wantEmoji string
		wantCalls string
	}{
		{name: "failed", status: "failed", wantEmoji: "ERROR", wantCalls: "create,update,add:ERROR"},
		{name: "canceled", status: "canceled", wantEmoji: "CrossMark", wantCalls: "create,update,add:CrossMark"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			client := &fakeWorkflowClient{}
			w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
			w.SetLarkClient(client)
			admitPipeline(t, s, "pipeline-pending", "pending", "2026-08-01 12:00:00 UTC", "")
			admitPipeline(t, s, "pipeline-final", tc.status, "2026-08-01 12:00:00 UTC", "2026-08-01 12:06:00 UTC")
			for i := 0; i < 2; i++ {
				items, err := s.Due(time.Now(), 1)
				if err != nil || len(items) != 1 {
					t.Fatalf("round %d items=%+v err=%v", i, items, err)
				}
				if result := w.sendGitlabPipelines(context.Background(), items[0]); result != continueRound {
					t.Fatalf("round %d result=%v", i, result)
				}
			}
			if got := joinCalls(client.calls); got != tc.wantCalls {
				t.Fatalf("calls=%s want=%s", got, tc.wantCalls)
			}
			state, err := s.Workflow("oc_default\x00gitlab:17\x0099")
			if err != nil || state.ReactionType != tc.wantEmoji || state.Conclusion != tc.status {
				t.Fatalf("state=%+v err=%v", state, err)
			}
		})
	}
}

func TestGitlabPipelineStaleAndDuplicateStagesDoNotRegressCard(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := &fakeWorkflowClient{}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	// running, then success with a finished_at timestamp.
	admitPipeline(t, s, "pipeline-running", "running", "2026-08-01 12:00:00 UTC", "")
	admitPipeline(t, s, "pipeline-success", "success", "2026-08-01 12:00:00 UTC", "2026-08-01 12:05:00 UTC")
	for i := 0; i < 2; i++ {
		items, err := s.Due(time.Now(), 1)
		if err != nil || len(items) != 1 {
			t.Fatalf("round %d items=%+v err=%v", i, items, err)
		}
		if result := w.sendGitlabPipelines(context.Background(), items[0]); result != continueRound {
			t.Fatalf("round %d result=%v", i, result)
		}
	}
	if got := joinCalls(client.calls); got != "create,update,add:DONE" {
		t.Fatalf("calls=%s", got)
	}
	// Duplicate success and an out-of-order delayed running event must not
	// touch Lark and must drain from the queue.
	admitPipeline(t, s, "pipeline-success-dup", "success", "2026-08-01 12:00:00 UTC", "2026-08-01 12:05:00 UTC")
	admitPipeline(t, s, "pipeline-stale-running", "running", "2026-08-01 12:00:00 UTC", "")
	for i := 0; i < 2; i++ {
		items, err := s.Due(time.Now(), 1)
		if err != nil || len(items) != 1 {
			t.Fatalf("stale round %d items=%+v err=%v", i, items, err)
		}
		if result := w.sendGitlabPipelines(context.Background(), items[0]); result != continueRound {
			t.Fatalf("stale round %d result=%v", i, result)
		}
	}
	if got := joinCalls(client.calls); got != "create,update,add:DONE" {
		t.Fatalf("stale deliveries touched Lark: calls=%s", got)
	}
	state, err := s.Workflow("oc_default\x00gitlab:17\x0099")
	if err != nil || state.Status != "success" || state.Conclusion != "success" || state.ReactionType != "DONE" {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("queue not drained: %+v err=%v", items, err)
	}
}

func TestGitlabPipelineCardsDoNotCollideWithGitHubCards(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := &fakeWorkflowClient{}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	// GitLab project 17 / pipeline 99 and GitHub repository 17 / check suite 99
	// share numeric ids but must render separate cards in the same chat.
	github := []byte(`{"repository":{"id":17,"full_name":"acme/backend"},"workflow_run":{"id":10,"check_suite_id":99,"name":"CI","html_url":"https://github.com/acme/backend/actions/runs/10","status":"completed","conclusion":"success","updated_at":"2026-08-01T12:05:00Z"}}`)
	if _, err := s.AdmitToChats("github-delivery", "workflow_run", github, []string{"oc_default"}, time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive("github-delivery"); err != nil {
		t.Fatal(err)
	}
	admitPipeline(t, s, "pipeline-delivery", "success", "2026-08-01 12:00:00 UTC", "2026-08-01 12:05:00 UTC")
	items, err := s.Due(time.Now(), 2)
	if err != nil || len(items) != 2 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	for _, item := range items {
		if item.Event == "workflow_run" {
			if result := w.sendWorkflows(context.Background(), item); result != continueRound {
				t.Fatalf("github result=%v", result)
			}
		} else {
			if result := w.sendGitlabPipelines(context.Background(), item); result != continueRound {
				t.Fatalf("gitlab result=%v", result)
			}
		}
	}
	githubState, err := s.Workflow("oc_default\x0017\x0099")
	if err != nil || githubState.MessageID == "" {
		t.Fatalf("github state=%+v err=%v", githubState, err)
	}
	gitlabState, err := s.Workflow("oc_default\x00gitlab:17\x0099")
	if err != nil || gitlabState.MessageID == "" || gitlabState.Key == githubState.Key {
		t.Fatalf("gitlab state=%+v err=%v", gitlabState, err)
	}
	// Each source keeps its own durable state even though both deliveries went
	// to the same chat and used overlapping numeric ids, and each produced its
	// own Lark card (the fake client returns one message ID for every create).
	if githubState.ReactionID != gitlabState.ReactionID || githubState.ReactionType != gitlabState.ReactionType {
		t.Fatalf("github state=%+v gitlab state=%+v", githubState, gitlabState)
	}
}

func TestGitlabPipelineReactionMappingIsFixed(t *testing.T) {
	for status, want := range map[string]string{
		"success": "DONE", "failed": "ERROR",
		"canceled": "CrossMark", "skipped": "CrossMark",
		"created": "", "pending": "", "running": "", "canceling": "", "manual": "", "scheduled": "",
	} {
		if got := gitlabPipelineReaction(status); got != want {
			t.Fatalf("gitlabPipelineReaction(%q)=%q want=%q", status, got, want)
		}
	}
}

func TestGitlabPipelineUpdateParsesAndDerivesTimes(t *testing.T) {
	update, err := gitlabPipelineUpdate(store.Item{Event: "gitlab:pipeline", RawJSON: gitlabPipelineBody("skipped", "2026-08-01 12:00:00 UTC", "")})
	if err != nil {
		t.Fatal(err)
	}
	if update.RepositoryID != "gitlab:17" || update.SuiteID != "99" || update.Status != "skipped" || update.Conclusion != "skipped" || update.Repository != "acme/backend" || update.HeadSHA != "2222222222222222222222222222222222222222" || update.URL != "https://gitlab.example.com/acme/backend/-/pipelines/99" {
		t.Fatalf("update=%+v", update)
	}
	if !update.AuthoritativeTime || update.UpdatedAt.IsZero() {
		t.Fatalf("skipped pipeline should keep an authoritative created time: %+v", update)
	}
	if _, err := gitlabPipelineUpdate(store.Item{Event: "gitlab:pipeline", RawJSON: []byte(`{"project":{"id":1},"object_attributes":{"id":0}}`)}); err == nil {
		t.Fatal("missing pipeline id accepted")
	}
}

func TestGitlabPipelineCardEscapesUntrustedMarkdownAndOnlyLinksSafeURLs(t *testing.T) {
	state := store.WorkflowState{
		RepositoryID: "gitlab:17",
		Repository:   "acme/backend](https://evil.example/)",
		Status:       "running](https://evil.example/)",
		URL:          "https://gitlab.example.com/acme/backend?x=)[evil](https://evil.example)",
		WorkflowName: "pipeline [trusted]",
		HeadBranch:   "main",
	}
	card := makeGitlabPipelineCard(state)
	content := card["elements"].([]any)[0].(map[string]any)["text"].(map[string]string)["content"]
	for _, unwanted := range []string{"](" + "https://evil.example", "](https://evil.example)"} {
		if strings.Contains(content, unwanted) {
			t.Fatalf("untrusted Markdown/link was emitted: %q", content)
		}
	}
	if !strings.Contains(content, `running\]\(https://evil.example/\)`) {
		t.Fatalf("untrusted delimiters were not escaped: %q", content)
	}
	header := card["header"].(map[string]any)
	if header["template"] != "blue" {
		t.Fatalf("in-progress card template=%v", header["template"])
	}
	for value, want := range map[string]bool{
		"https://gitlab.example.com/acme/backend/-/pipelines/99": true,
		"http://gitlab.example.com/acme/backend":                 false,
		"https://gitlab.example.com/acme/backend?x=)[evil]":      false,
	} {
		if got := gitlabPipelineURL(value); got != want {
			t.Fatalf("gitlabPipelineURL(%q)=%v want=%v", value, got, want)
		}
	}
}

func TestGitlabPipelineCardDerivedURLWhenPayloadOmitsIt(t *testing.T) {
	payload := map[string]any{
		"object_kind": "pipeline",
		"object_attributes": map[string]any{
			"id": 7, "status": "running", "created_at": "2026-08-01 12:00:00 UTC", "ref": "main", "sha": "abc",
		},
		"project": map[string]any{"id": 3, "path_with_namespace": "a/b", "web_url": "https://gitlab.example.com/a/b"},
	}
	raw, _ := json.Marshal(payload)
	update, err := gitlabPipelineUpdate(store.Item{Event: "gitlab:pipeline", RawJSON: raw})
	if err != nil {
		t.Fatal(err)
	}
	if update.URL != "https://gitlab.example.com/a/b/-/pipelines/7" {
		t.Fatalf("derived URL=%q", update.URL)
	}
	// An http web URL cannot produce a safe https pipeline link.
	payload["project"].(map[string]any)["web_url"] = "http://gitlab.example.com/a/b"
	raw, _ = json.Marshal(payload)
	update, err = gitlabPipelineUpdate(store.Item{Event: "gitlab:pipeline", RawJSON: raw})
	if err != nil {
		t.Fatal(err)
	}
	if update.URL != "" {
		t.Fatalf("http web URL produced pipeline URL %q", update.URL)
	}
}

func TestGitlabPipelineKeyAndRank(t *testing.T) {
	update := store.WorkflowUpdate{RepositoryID: "gitlab:17", SuiteID: "99", Status: "running", UpdatedAt: time.Now()}
	if key := gitlabPipelineKey("oc_default", update); key != "oc_default\x00gitlab:17\x0099" {
		t.Fatalf("key=%q", key)
	}
	if key := gitlabPipelineKey("", update); key != "gitlab:17\x0099" {
		t.Fatalf("default chat key=%q", key)
	}
	state := store.WorkflowState{Status: "success", Conclusion: "success", UpdatedAt: time.Now().Add(-time.Minute)}
	olderRunning := store.WorkflowUpdate{Status: "running", UpdatedAt: state.UpdatedAt.Add(-2 * time.Minute)}
	if gitlabPipelineAdvances(state, olderRunning) {
		t.Fatal("stale running advanced a successful pipeline")
	}
	newerRunning := store.WorkflowUpdate{Status: "running", UpdatedAt: state.UpdatedAt.Add(time.Minute)}
	if gitlabPipelineAdvances(state, newerRunning) {
		t.Fatal("running advanced a successful pipeline even with a newer time")
	}
	first := store.WorkflowUpdate{Status: "created", UpdatedAt: time.Now()}
	if !gitlabPipelineAdvances(store.WorkflowState{}, first) {
		t.Fatal("first delivery did not advance")
	}
	pending := store.WorkflowState{Status: "pending", UpdatedAt: time.Now()}
	lateRunning := store.WorkflowUpdate{Status: "running", UpdatedAt: pending.UpdatedAt.Add(-time.Second)}
	if !gitlabPipelineAdvances(pending, lateRunning) {
		t.Fatal("later stage with older payload time did not advance")
	}
}
