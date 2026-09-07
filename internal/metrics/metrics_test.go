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
	} {
		if !strings.Contains(body, line) {
			t.Fatalf("missing %q in %s", line, body)
		}
	}
}
