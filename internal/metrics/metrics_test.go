package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEventMetricsUseFixedCategories(t *testing.T) {
	m := &Metrics{}
	m.Received()
	m.Received()
	m.EventReceived("push")
	m.EventReceived("future_event")
	m.EventDelivered("push")
	m.WorkflowCardCreated()
	m.WorkflowCardUpdated()
	m.WorkflowReactionAdded()
	m.WorkflowReactionDeleted()
	m.GitlabReceived()
	m.GitlabEventReceived("gitlab:push")
	m.GitlabEventReceived("gitlab:unknown_hook")
	m.GitlabEventDelivered("gitlab:pipeline")
	response := httptest.NewRecorder()
	m.Handler(func() Snapshot { return Snapshot{} }).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, line := range []string{
		"lark_git_webhook_webhooks_received_total 2",
		`lark_git_webhook_events_received_total{event="push"} 1`,
		`lark_git_webhook_events_received_total{event="other"} 1`,
		`lark_git_webhook_events_delivered_total{event="push"} 1`,
		`lark_git_webhook_events_delivered_total{event="other"} 0`,
		"lark_git_webhook_workflow_cards_created_total 1",
		"lark_git_webhook_workflow_cards_updated_total 1",
		"lark_git_webhook_workflow_reactions_added_total 1",
		"lark_git_webhook_workflow_reactions_deleted_total 1",
		"lark_git_webhook_gitlab_webhooks_received_total 1",
		"lark_git_webhook_gitlab_webhooks_verification_failed_total 0",
		"lark_git_webhook_gitlab_webhooks_duplicates_total 0",
		"lark_git_webhook_gitlab_webhooks_rejected_total 0",
		`lark_git_webhook_gitlab_events_received_total{event="push"} 1`,
		`lark_git_webhook_gitlab_events_received_total{event="other"} 1`,
		`lark_git_webhook_gitlab_events_delivered_total{event="pipeline"} 1`,
		`lark_git_webhook_gitlab_events_delivered_total{event="other"} 0`,
	} {
		if !strings.Contains(body, line) {
			t.Fatalf("missing %q in %s", line, body)
		}
	}
}

func TestGitlabEventIndexesAreNamespaced(t *testing.T) {
	for event, want := range map[string]int{
		"gitlab:push": 0, "gitlab:tag_push": 1, "gitlab:pipeline": 2, "gitlab:merge_request": 3, "gitlab:note_hook": 4, "push": 4, "": 4,
	} {
		if got := gitlabEventIndex(event); got != want {
			t.Fatalf("gitlabEventIndex(%q)=%d want=%d", event, got, want)
		}
	}
}
