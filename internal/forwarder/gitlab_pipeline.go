package forwarder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

// GitLab pipeline cards mirror the GitHub workflow cards: one durable
// interactive card per (project, pipeline) that is updated in place. Pipeline
// deliveries ride the same store.WorkflowState machinery, but their repository
// ID is namespaced ("gitlab:<project id>") so a GitLab project/pipeline pair
// can never collide with a GitHub repository/check-suite pair in the shared
// workflow state bucket.

const gitlabWorkflowRepositoryPrefix = "gitlab:"

type gitlabPipelinePayload struct {
	ObjectKind       string `json:"object_kind"`
	ObjectAttributes struct {
		ID         int64  `json:"id"`
		IID        int64  `json:"iid"`
		Name       string `json:"name"`
		Ref        string `json:"ref"`
		Tag        bool   `json:"tag"`
		SHA        string `json:"sha"`
		Status     string `json:"status"`
		Source     string `json:"source"`
		CreatedAt  string `json:"created_at"`
		FinishedAt string `json:"finished_at"`
		URL        string `json:"url"`
	} `json:"object_attributes"`
	Project struct {
		ID                int64  `json:"id"`
		Name              string `json:"name"`
		PathWithNamespace string `json:"path_with_namespace"`
		WebURL            string `json:"web_url"`
	} `json:"project"`
	Commit struct {
		ID        string `json:"id"`
		Title     string `json:"title"`
		Message   string `json:"message"`
		Timestamp string `json:"timestamp"`
		URL       string `json:"url"`
	} `json:"commit"`
	User struct {
		Name     string `json:"name"`
		Username string `json:"username"`
	} `json:"user"`
}

// gitlabPipelineStatusRank orders pipeline lifecycle stages so a delayed
// duplicate of an earlier stage can never regress a stored card. Pipeline
// statuses advance along a single path per pipeline; terminals are last.
var gitlabPipelineStatusRank = map[string]int{
	"created": 0, "waiting_for_resource": 1, "preparing": 2, "waiting_for_callback": 3,
	"pending": 4, "scheduled": 4, "manual": 4,
	"running":   5,
	"canceling": 6,
	"success":   7, "failed": 7, "skipped": 7, "canceled": 8,
}

var gitlabPipelineTerminal = map[string]bool{
	"success": true, "failed": true, "canceled": true, "skipped": true,
}

func gitlabPipelineUpdate(item store.Item) (store.WorkflowUpdate, error) {
	var payload gitlabPipelinePayload
	if err := json.Unmarshal(item.RawJSON, &payload); err != nil {
		return store.WorkflowUpdate{}, err
	}
	if payload.Project.ID == 0 || payload.ObjectAttributes.ID == 0 {
		return store.WorkflowUpdate{}, errors.New("pipeline event missing project or pipeline ID")
	}
	update := store.WorkflowUpdate{
		RepositoryID: gitlabWorkflowRepositoryPrefix + fmt.Sprint(payload.Project.ID),
		Repository:   payload.Project.PathWithNamespace,
		SuiteID:      fmt.Sprint(payload.ObjectAttributes.ID),
		WorkflowName: payload.ObjectAttributes.Name,
		Status:       payload.ObjectAttributes.Status,
		HeadBranch:   payload.ObjectAttributes.Ref,
		HeadSHA:      payload.ObjectAttributes.SHA,
	}
	update.URL = firstNonEmpty(payload.ObjectAttributes.URL, gitlabPipelineDerivedURL(payload))
	if terminal := gitlabPipelineTerminal[payload.ObjectAttributes.Status]; terminal {
		update.Conclusion = payload.ObjectAttributes.Status
	}
	update.UpdatedAt, update.AuthoritativeTime = gitlabPipelineTime(payload)
	return update, nil
}

// gitlabPipelineDerivedURL builds the pipeline page URL from the project web
// URL when the payload omits object_attributes.url.
func gitlabPipelineDerivedURL(payload gitlabPipelinePayload) string {
	base := firstNonEmpty(payload.Project.WebURL, "")
	if base == "" || payload.ObjectAttributes.ID <= 0 {
		return ""
	}
	if !gitlabHTTPSURL(base + "/-/pipelines/" + fmt.Sprint(payload.ObjectAttributes.ID)) {
		return ""
	}
	return base + "/-/pipelines/" + fmt.Sprint(payload.ObjectAttributes.ID)
}

// gitlabPipelineTime returns the authoritative delivery timestamp: the
// finished_at time for completed pipelines (GitLab sets it for success,
// failed, and canceled; skipped pipelines finish without it) and the
// created_at time otherwise.
func gitlabPipelineTime(payload gitlabPipelinePayload) (time.Time, bool) {
	if gitlabPipelineTerminal[payload.ObjectAttributes.Status] {
		if at, ok := parseGitlabTime(payload.ObjectAttributes.FinishedAt); ok {
			return at, true
		}
	}
	if at, ok := parseGitlabTime(payload.ObjectAttributes.CreatedAt); ok {
		return at, true
	}
	return time.Time{}, false
}

func gitlabPipelineKey(chatID string, update store.WorkflowUpdate) string {
	repositoryID := update.RepositoryID
	if repositoryID == "" {
		repositoryID = update.Repository
	}
	key := repositoryID + "\x00" + update.SuiteID
	if chatID != "" {
		key = chatID + "\x00" + key
	}
	return key
}

// gitlabPipelineAdvances reports whether a pipeline delivery should update the
// stored card. A pipeline's statuses only move forward, so a lower (or equal)
// lifecycle rank is stale unless it carries strictly newer authoritative
// timing; unknown statuses fall back to time comparison only.
func gitlabPipelineAdvances(state store.WorkflowState, update store.WorkflowUpdate) bool {
	if state.Status == "" || state.UpdatedAt.IsZero() {
		return true
	}
	incoming, ok := gitlabPipelineStatusRank[update.Status]
	if !ok {
		return update.UpdatedAt.After(state.UpdatedAt)
	}
	stored, ok := gitlabPipelineStatusRank[state.Status]
	if !ok {
		return true
	}
	switch {
	case incoming > stored:
		return true
	case incoming < stored:
		return false
	default:
		return update.UpdatedAt.After(state.UpdatedAt)
	}
}

func (w *Worker) sendGitlabPipelines(ctx context.Context, item store.Item) batchResult {
	for _, chatID := range itemChatIDs(item) {
		if result := w.sendGitlabPipeline(ctx, item, chatID); result != continueRound {
			return result
		}
	}
	return continueRound
}

func (w *Worker) sendGitlabPipeline(ctx context.Context, item store.Item, chatID string) batchResult {
	if w.lark == nil {
		return w.workflowFailure(item, errors.New("Lark app client is not configured"))
	}
	update, err := gitlabPipelineUpdate(item)
	if err != nil {
		return w.workflowFailure(item, fmt.Errorf("parse pipeline event: %w", err))
	}
	update.ChatID = chatID
	key := gitlabPipelineKey(chatID, update)
	existing, err := w.store.Workflow(key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return w.workflowFailure(item, fmt.Errorf("read pipeline state: %w", err))
	}
	if err == nil && !gitlabPipelineAdvances(existing, update) {
		// A stale duplicate of an earlier pipeline stage: the durable state is
		// already newer, so drain the delivery without touching Lark.
		if completeErr := w.store.CompleteChatDelivery([]store.Item{item}, chatID); completeErr != nil {
			w.logger.Error("record delivered stale pipeline target", "error", completeErr)
			return stopWorker
		}
		return continueRound
	}
	state, err := w.store.UpsertWorkflow(update)
	if err != nil {
		return w.workflowFailure(item, fmt.Errorf("persist pipeline state: %w", err))
	}
	return w.sendCardToChat(ctx, item, chatID, state, makeGitlabPipelineCard, gitlabPipelineReaction, gitlabPipelineUUID, w.countDelivered)
}

func gitlabPipelineReaction(conclusion string) string {
	switch conclusion {
	case "success":
		return "DONE"
	case "failed":
		return "ERROR"
	case "canceled", "skipped":
		return "CrossMark"
	default:
		return ""
	}
}

func gitlabPipelineUUID(key string) string {
	digest := sha256.Sum256([]byte(key))
	return "gitlab-pipeline-" + hex.EncodeToString(digest[:16])
}

func makeGitlabPipelineCard(state store.WorkflowState) map[string]any {
	lines := make([]string, 0, 5)
	if state.URL != "" {
		if gitlabPipelineURL(state.URL) {
			lines = append(lines, "[Open pipeline]("+state.URL+")")
		} else {
			lines = append(lines, workflowMarkdown(state.URL))
		}
	}
	lines = append(lines, "**Status:** "+workflowMarkdown(firstNonEmpty(state.Conclusion, state.Status, "pending")))
	if repo := workflowMarkdown(state.Repository); repo != "" {
		lines = append(lines, "**Project:** "+repo)
	}
	if ref := workflowMarkdown(state.HeadBranch); ref != "" {
		lines = append(lines, "**Ref:** "+ref)
	}
	if sha := shortSHA(state.HeadSHA); sha != "" {
		lines = append(lines, "**Commit:** "+workflowMarkdown(sha))
	}
	return map[string]any{
		"config":   map[string]any{"wide_screen_mode": true, "update_multi": true},
		"header":   map[string]any{"title": map[string]string{"tag": "plain_text", "content": workflowText(firstNonEmpty(state.WorkflowName, state.Repository, "GitLab pipeline"))}, "template": reactionTemplate(gitlabPipelineReaction(state.Conclusion))},
		"elements": []any{map[string]any{"tag": "div", "text": map[string]string{"tag": "lark_md", "content": strings.Join(lines, "\n")}}},
	}
}

// gitlabPipelineURL allows an https pipeline link on any host, mirroring the
// GitHub card policy of only emitting safe markdown links.
func gitlabPipelineURL(value string) bool {
	if strings.ContainsAny(value, "\\[]()\r\n") {
		return false
	}
	return gitlabHTTPSURL(value)
}

// reactionTemplate maps a status reaction to the Lark card header template.
func reactionTemplate(reaction string) string {
	switch reaction {
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
