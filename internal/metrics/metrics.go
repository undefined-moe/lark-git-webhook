package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const eventCount = 25

var eventCategories = [eventCount]string{
	"check_run", "check_suite", "create", "delete", "deployment", "deployment_status", "discussion", "fork", "gollum", "installation", "installation_repositories", "issue_comment", "issues", "member", "membership", "package", "ping", "project", "pull_request", "push", "release", "repository", "repository_dispatch", "workflow_job", "workflow_run",
}

type Metrics struct {
	received         atomic.Uint64
	verificationFail atomic.Uint64
	duplicates       atomic.Uint64
	rejected         atomic.Uint64
	larkSuccess      atomic.Uint64
	larkFailure      atomic.Uint64
	retries          atomic.Uint64
	rateWaitMillis   atomic.Uint64
	workflowCreated  atomic.Uint64
	workflowUpdated  atomic.Uint64
	reactionAdded    atomic.Uint64
	reactionDeleted  atomic.Uint64
	lastErrorUnix    atomic.Int64
	eventsReceived   [eventCount + 1]atomic.Uint64
	eventsDelivered  [eventCount + 1]atomic.Uint64
}

type Snapshot struct {
	QueueItems       uint64
	QueueBytes       uint64
	DeadLetters      uint64
	DeadBytes        uint64
	DBBytes          int64
	OldestAge        time.Duration
	WorkerLive       bool
	ArchiveFreeBytes uint64
}

func (m *Metrics) Received()                   { m.received.Add(1) }
func (m *Metrics) VerificationFailed()         { m.verificationFail.Add(1) }
func (m *Metrics) Duplicate()                  { m.duplicates.Add(1) }
func (m *Metrics) Rejected()                   { m.rejected.Add(1) }
func (m *Metrics) LarkSuccess()                { m.larkSuccess.Add(1) }
func (m *Metrics) LarkFailure()                { m.larkFailure.Add(1); m.lastErrorUnix.Store(time.Now().Unix()) }
func (m *Metrics) Retry()                      { m.retries.Add(1) }
func (m *Metrics) RateWait(wait time.Duration) { m.rateWaitMillis.Add(uint64(wait.Milliseconds())) }
func (m *Metrics) WorkflowCardCreated()        { m.workflowCreated.Add(1) }
func (m *Metrics) WorkflowCardUpdated()        { m.workflowUpdated.Add(1) }
func (m *Metrics) WorkflowReactionAdded()      { m.reactionAdded.Add(1) }
func (m *Metrics) WorkflowReactionDeleted()    { m.reactionDeleted.Add(1) }
func (m *Metrics) EventReceived(event string)  { m.eventsReceived[eventIndex(event)].Add(1) }
func (m *Metrics) EventDelivered(event string) { m.eventsDelivered[eventIndex(event)].Add(1) }

func (m *Metrics) Handler(snapshot func() Snapshot) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s := snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		lines := []string{
			counter("lark_git_webhook_webhooks_received_total", m.received.Load()),
			counter("lark_git_webhook_webhooks_verification_failed_total", m.verificationFail.Load()),
			counter("lark_git_webhook_webhooks_duplicates_total", m.duplicates.Load()),
			counter("lark_git_webhook_webhooks_rejected_total", m.rejected.Load()),
			eventCounter("lark_git_webhook_events_received_total", &m.eventsReceived),
			eventCounter("lark_git_webhook_events_delivered_total", &m.eventsDelivered),
			counter("lark_git_webhook_lark_requests_success_total", m.larkSuccess.Load()),
			counter("lark_git_webhook_lark_requests_failure_total", m.larkFailure.Load()),
			counter("lark_git_webhook_retries_total", m.retries.Load()),
			counter("lark_git_webhook_rate_limit_wait_milliseconds_total", m.rateWaitMillis.Load()),
			counter("lark_git_webhook_workflow_cards_created_total", m.workflowCreated.Load()),
			counter("lark_git_webhook_workflow_cards_updated_total", m.workflowUpdated.Load()),
			counter("lark_git_webhook_workflow_reactions_added_total", m.reactionAdded.Load()),
			counter("lark_git_webhook_workflow_reactions_deleted_total", m.reactionDeleted.Load()),
			gauge("lark_git_webhook_queue_items", float64(s.QueueItems)),
			gauge("lark_git_webhook_queue_logical_bytes", float64(s.QueueBytes)),
			gauge("lark_git_webhook_dead_letter_items", float64(s.DeadLetters)),
			gauge("lark_git_webhook_dead_letter_logical_bytes", float64(s.DeadBytes)),
			gauge("lark_git_webhook_bbolt_file_bytes", float64(s.DBBytes)),
			gauge("lark_git_webhook_archive_filesystem_free_bytes", float64(s.ArchiveFreeBytes)),
			gauge("lark_git_webhook_oldest_event_age_seconds", s.OldestAge.Seconds()),
			gauge("lark_git_webhook_worker_live", boolFloat(s.WorkerLive)),
			gauge("lark_git_webhook_last_error_timestamp_seconds", float64(m.lastErrorUnix.Load())),
		}
		_, _ = fmt.Fprint(w, strings.Join(lines, "\n")+"\n")
	}
}

func eventIndex(event string) int {
	for i, category := range eventCategories {
		if event == category {
			return i
		}
	}
	return eventCount
}

func eventCounter(name string, values *[eventCount + 1]atomic.Uint64) string {
	lines := []string{fmt.Sprintf("# TYPE %s counter", name)}
	for i, category := range eventCategories {
		lines = append(lines, fmt.Sprintf("%s{event=%q} %d", name, category, values[i].Load()))
	}
	return strings.Join(append(lines, fmt.Sprintf("%s{event=%q} %d", name, "other", values[eventCount].Load())), "\n")
}

func counter(name string, value uint64) string {
	return fmt.Sprintf("# TYPE %s counter\n%s %d", name, name, value)
}
func gauge(name string, value float64) string {
	return fmt.Sprintf("# TYPE %s gauge\n%s %g", name, name, value)
}
func boolFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
