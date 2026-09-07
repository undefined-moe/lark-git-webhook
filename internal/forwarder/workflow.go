package forwarder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

type workflowPayload struct {
	Repository struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
	} `json:"repository"`
	WorkflowRun struct {
		ID           int64     `json:"id"`
		CheckSuiteID int64     `json:"check_suite_id"`
		RunAttempt   int       `json:"run_attempt"`
		Name         string    `json:"name"`
		HTMLURL      string    `json:"html_url"`
		Status       string    `json:"status"`
		Conclusion   string    `json:"conclusion"`
		HeadBranch   string    `json:"head_branch"`
		HeadSHA      string    `json:"head_sha"`
		RunStartedAt time.Time `json:"run_started_at"`
		UpdatedAt    time.Time `json:"updated_at"`
	} `json:"workflow_run"`
	CheckRun struct {
		ID          int64     `json:"id"`
		Name        string    `json:"name"`
		Status      string    `json:"status"`
		Conclusion  string    `json:"conclusion"`
		DetailsURL  string    `json:"details_url"`
		StartedAt   time.Time `json:"started_at"`
		CompletedAt time.Time `json:"completed_at"`
		CheckSuite  struct {
			ID int64 `json:"id"`
		} `json:"check_suite"`
	} `json:"check_run"`
}

func workflowUpdate(item store.Item) (store.WorkflowUpdate, error) {
	var payload workflowPayload
	if err := json.Unmarshal(item.RawJSON, &payload); err != nil {
		return store.WorkflowUpdate{}, err
	}
	update := store.WorkflowUpdate{RepositoryID: fmt.Sprint(payload.Repository.ID), Repository: payload.Repository.FullName}
	switch item.Event {
	case "workflow_run":
		if payload.WorkflowRun.CheckSuiteID == 0 {
			return store.WorkflowUpdate{}, errors.New("workflow event missing check suite ID")
		}
		update.SuiteID = fmt.Sprint(payload.WorkflowRun.CheckSuiteID)
		update.RunID = payload.WorkflowRun.ID
		update.RunAttempt = payload.WorkflowRun.RunAttempt
		update.WorkflowName = payload.WorkflowRun.Name
		update.URL = payload.WorkflowRun.HTMLURL
		update.Status = payload.WorkflowRun.Status
		update.Conclusion = payload.WorkflowRun.Conclusion
		update.HeadBranch = payload.WorkflowRun.HeadBranch
		update.HeadSHA = payload.WorkflowRun.HeadSHA
		update.RunStartedAt = payload.WorkflowRun.RunStartedAt
		update.UpdatedAt = payload.WorkflowRun.UpdatedAt
		update.AuthoritativeTime = !update.UpdatedAt.IsZero()
	case "check_run":
		update.SuiteID = fmt.Sprint(payload.CheckRun.CheckSuite.ID)
		update.Check = &store.WorkflowCheck{
			ID:          fmt.Sprint(payload.CheckRun.ID),
			Name:        payload.CheckRun.Name,
			Status:      payload.CheckRun.Status,
			Conclusion:  payload.CheckRun.Conclusion,
			DetailsURL:  payload.CheckRun.DetailsURL,
			StartedAt:   payload.CheckRun.StartedAt,
			CompletedAt: payload.CheckRun.CompletedAt,
		}
		if !payload.CheckRun.CompletedAt.IsZero() {
			update.UpdatedAt, update.AuthoritativeTime = payload.CheckRun.CompletedAt, true
		} else if !payload.CheckRun.StartedAt.IsZero() {
			update.UpdatedAt, update.AuthoritativeTime = payload.CheckRun.StartedAt, true
		}
	default:
		return store.WorkflowUpdate{}, errors.New("unsupported workflow event")
	}
	if payload.Repository.ID == 0 || update.Repository == "" || update.SuiteID == "" || update.SuiteID == "0" {
		return store.WorkflowUpdate{}, errors.New("workflow event missing repository or suite ID")
	}
	return update, nil
}

func (w *Worker) sendWorkflow(ctx context.Context, item store.Item) batchResult {
	return w.sendWorkflowToChat(ctx, item, itemChatIDs(item)[0])
}

func (w *Worker) sendWorkflows(ctx context.Context, item store.Item) batchResult {
	for _, chatID := range itemChatIDs(item) {
		if result := w.sendWorkflowToChat(ctx, item, chatID); result != continueRound {
			return result
		}
	}
	return continueRound
}

func (w *Worker) sendWorkflowToChat(ctx context.Context, item store.Item, chatID string) batchResult {
	if w.lark == nil {
		return w.workflowFailure(item, errors.New("Lark app client is not configured"))
	}
	update, err := workflowUpdate(item)
	if err != nil {
		return w.workflowFailure(item, fmt.Errorf("parse workflow event: %w", err))
	}
	update.ChatID = chatID
	state, err := w.store.UpsertWorkflow(update)
	if err != nil {
		return w.workflowFailure(item, fmt.Errorf("persist workflow state: %w", err))
	}
	card := makeWorkflowCard(state)
	messageID := state.MessageID
	if messageID == "" {
		if result := w.reserveWorkflowRequest(ctx, item); result != continueRound {
			return result
		}
		messageID, err = w.createInteractive(ctx, chatID, card, workflowUUID(state.Key))
		if err == nil {
			err = w.store.SetWorkflowMessage(state.Key, messageID)
		}
		if err != nil {
			return w.workflowFailure(item, err)
		}
		w.metrics.WorkflowCardCreated()
		w.metrics.LarkSuccess()
	} else {
		if result := w.reserveWorkflowRequest(ctx, item); result != continueRound {
			return result
		}
		if err := w.lark.UpdateInteractive(ctx, messageID, card); err != nil {
			return w.workflowFailure(item, err)
		}
		w.metrics.WorkflowCardUpdated()
		w.metrics.LarkSuccess()
	}
	reaction := workflowReaction(state.Conclusion)
	if state.ReactionID != "" && reaction != state.ReactionType {
		if result := w.reserveWorkflowRequest(ctx, item); result != continueRound {
			return result
		}
		if err := w.lark.DeleteReaction(ctx, messageID, state.ReactionID); err != nil {
			return w.workflowFailure(item, err)
		}
		if err := w.store.SetWorkflowReaction(state.Key, "", ""); err != nil {
			return w.workflowFailure(item, err)
		}
		state.ReactionID, state.ReactionType = "", ""
		w.metrics.WorkflowReactionDeleted()
		w.metrics.LarkSuccess()
	}
	if reaction != "" && reaction != state.ReactionType {
		if result := w.reserveWorkflowRequest(ctx, item); result != continueRound {
			return result
		}
		reactionID, err := w.lark.AddReaction(ctx, messageID, reaction)
		if err != nil {
			return w.workflowFailure(item, err)
		}
		if err := w.store.SetWorkflowReaction(state.Key, reactionID, reaction); err != nil {
			return w.workflowFailure(item, err)
		}
		w.metrics.WorkflowReactionAdded()
		w.metrics.LarkSuccess()
	}
	if err := w.store.CompleteChatDelivery([]store.Item{item}, chatID); err != nil {
		w.logger.Error("record delivered workflow chat target", "error", err)
		return stopWorker
	}
	w.metrics.EventDelivered(item.Event)
	return continueRound
}

func (w *Worker) createInteractive(ctx context.Context, chatID string, card any, uuid string) (string, error) {
	if chatID != "" {
		if client, ok := w.lark.(chatWorkflowClient); ok {
			return client.CreateInteractiveToChat(ctx, chatID, card, uuid)
		}
	}
	return w.lark.CreateInteractive(ctx, card, uuid)
}

func (w *Worker) reserveWorkflowRequest(ctx context.Context, item store.Item) batchResult {
	for {
		wait, err := w.store.ReserveRequest(time.Now())
		if err != nil {
			return w.workflowFailure(item, fmt.Errorf("reserve rate-limit slot: %w", err))
		}
		if wait <= 0 {
			return continueRound
		}
		w.metrics.RateWait(wait)
		if !waitContext(ctx, wait) {
			return stopWorker
		}
	}
}

func (w *Worker) workflowFailure(item store.Item, err error) batchResult {
	w.metrics.LarkFailure()
	retryErr := new(lark.RetryError)
	isRetry := errors.As(err, &retryErr)
	delay := w.backoff([]store.Item{item})
	retryAfter := time.Duration(0)
	permanent := isRetry && retryErr.Permanent
	if isRetry {
		retryAfter = retryErr.RetryAfter
		if retryAfter > delay {
			delay = retryAfter
		}
	}
	result, updateErr := w.store.HandleFailure([]store.Item{item}, time.Now(), time.Now().Add(delay), err.Error(), retryAfter, w.maxAttempts, permanent)
	if updateErr != nil {
		w.logger.Error("persist failed workflow delivery state", "error", updateErr)
		return stopWorker
	}
	if result.Retried > 0 {
		w.metrics.Retry()
	}
	if retryAfter > 0 {
		w.logger.Warn("Lark rate limited; scheduled workflow retry", "delay", delay)
		return stopRound
	}
	w.logger.Warn("Lark workflow delivery failed", "retried", result.Retried, "dead_lettered", result.DeadLettered, "error", err)
	return continueRound
}

func workflowReaction(conclusion string) string {
	switch conclusion {
	case "success":
		return "DONE"
	case "failure", "timed_out", "action_required":
		return "ERROR"
	case "cancelled", "skipped", "neutral", "stale":
		return "CrossMark"
	default:
		return ""
	}
}

func workflowUUID(key string) string {
	digest := sha256.Sum256([]byte(key))
	return "github-suite-" + hex.EncodeToString(digest[:16])
}

func makeWorkflowCard(state store.WorkflowState) map[string]any {
	checks := make([]store.WorkflowCheck, 0, len(state.Checks))
	for _, check := range state.Checks {
		checks = append(checks, check)
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	lines := make([]string, 0, len(checks)+3)
	if state.URL != "" {
		if workflowGitHubURL(state.URL) {
			lines = append(lines, "[Open workflow run]("+state.URL+")")
		} else {
			lines = append(lines, workflowMarkdown(state.URL))
		}
	}
	if state.RunAttempt > 0 {
		lines = append(lines, fmt.Sprintf("**Attempt:** %d", state.RunAttempt))
	}
	lines = append(lines, "**Status:** "+workflowMarkdown(firstNonEmpty(state.Conclusion, state.Status, "pending")))
	for i, check := range checks {
		if i == 50 {
			lines = append(lines, fmt.Sprintf("… and %d more checks", len(checks)-i))
			break
		}
		name := workflowMarkdown(firstNonEmpty(check.Name, "unnamed check"))
		status := workflowMarkdown(firstNonEmpty(check.Conclusion, check.Status, "pending"))
		if check.DetailsURL != "" && workflowGitHubURL(check.DetailsURL) {
			lines = append(lines, "• ["+name+"]("+check.DetailsURL+"): "+status)
		} else if check.DetailsURL != "" {
			lines = append(lines, "• "+name+": "+status+" | "+workflowMarkdown(check.DetailsURL))
		} else {
			lines = append(lines, "• "+name+": "+status)
		}
	}
	return map[string]any{
		"config":   map[string]any{"wide_screen_mode": true, "update_multi": true},
		"header":   map[string]any{"title": map[string]string{"tag": "plain_text", "content": workflowText(firstNonEmpty(state.WorkflowName, "GitHub checks"))}, "template": cardTemplate(state.Conclusion)},
		"elements": []any{map[string]any{"tag": "div", "text": map[string]string{"tag": "lark_md", "content": strings.Join(lines, "\n")}}},
	}
}

func cardTemplate(conclusion string) string {
	switch workflowReaction(conclusion) {
	case "DONE":
		return "green"
	case "ERROR":
		return "red"
	case "CrossMark":
		return "grey"
	default:
		return "blue"
	}
}
func workflowText(value string) string {
	value = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, value)
	return strings.Join(strings.Fields(value), " ")
}

func workflowMarkdown(value string) string {
	value = workflowText(value)
	return strings.NewReplacer(
		"\\", "\\\\",
		"[", "\\[",
		"]", "\\]",
		"(", "\\(",
		")", "\\)",
		"*", "\\*",
		"_", "\\_",
		"~", "\\~",
		"`", "\\`",
	).Replace(value)
}

func workflowGitHubURL(value string) bool {
	if strings.ContainsAny(value, "\\[]()\r\n") {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host == "github.com" && parsed.User == nil && parsed.Path != ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
