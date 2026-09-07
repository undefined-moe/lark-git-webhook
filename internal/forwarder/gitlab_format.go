package forwarder

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

// GitLab post formatting. GitLab queue event labels carry the "gitlab:"
// prefix (for example "gitlab:push"); GitLab payloads are decoded into the
// structures below and rendered as single-paragraph Lark post lines whose URL
// segments are their own " | " parts so gitlabRichText can link them.

type gitlabProject struct {
	ID                int64  `json:"id"`
	Name              string `json:"name"`
	PathWithNamespace string `json:"path_with_namespace"`
	WebURL            string `json:"web_url"`
	Homepage          string `json:"homepage"`
}

type gitlabCommit struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	Title     string `json:"title"`
	Timestamp string `json:"timestamp"`
	URL       string `json:"url"`
}

type gitlabPushPayload struct {
	ObjectKind        string         `json:"object_kind"`
	Before            string         `json:"before"`
	After             string         `json:"after"`
	Ref               string         `json:"ref"`
	CheckoutSHA       string         `json:"checkout_sha"`
	Message           string         `json:"message"`
	UserName          string         `json:"user_name"`
	UserUsername      string         `json:"user_username"`
	Project           gitlabProject  `json:"project"`
	Commits           []gitlabCommit `json:"commits"`
	TotalCommitsCount *int           `json:"total_commits_count"`
}

type gitlabMergeRequestPayload struct {
	ObjectKind string `json:"object_kind"`
	User       struct {
		Name     string `json:"name"`
		Username string `json:"username"`
	} `json:"user"`
	Project          gitlabProject `json:"project"`
	ObjectAttributes struct {
		IID          int    `json:"iid"`
		Title        string `json:"title"`
		State        string `json:"state"`
		Action       string `json:"action"`
		URL          string `json:"url"`
		SourceBranch string `json:"source_branch"`
		TargetBranch string `json:"target_branch"`
		CreatedAt    string `json:"created_at"`
		UpdatedAt    string `json:"updated_at"`
	} `json:"object_attributes"`
}

func formatGitlab(item store.Item) string {
	switch item.Event {
	case "gitlab:push", "gitlab:tag_push":
		return formatGitlabPush(item.Event, item.RawJSON)
	case "gitlab:merge_request":
		return formatGitlabMergeRequest(item.RawJSON)
	default:
		return formatGitlabUnknown(item)
	}
}

func formatGitlabPush(event string, raw []byte) string {
	var p gitlabPushPayload
	_ = json.Unmarshal(raw, &p)
	repo := fallback(sanitize(p.Project.PathWithNamespace), "unknown repository")
	label := event
	parts := []string{fmt.Sprintf("[%s] %s", label, repo)}
	ref, isTag := gitlabRefParts(p.Ref)
	switch {
	case isTag:
		parts = append(parts, "tag "+ref)
	case ref != "":
		parts = append(parts, "branch "+ref)
	}
	before, after := shortSHA(p.Before), shortSHA(p.After)
	if after == "" {
		after = shortSHA(p.CheckoutSHA)
	}
	if before != "" && after != "" {
		parts = append(parts, before+".."+after)
	}
	if count := gitlabCommitCount(&p); count != nil {
		parts = append(parts, fmt.Sprintf("%d %s", *count, commitUnit(*count)))
	}
	if message := gitlabHeadMessage(p, isTag); message != "" {
		parts = append(parts, message)
	}
	if pusher := gitlabPusher(p.UserUsername, p.UserName); pusher != "" {
		parts = append(parts, pusher)
	}
	if at := gitlabNewestCommitTime(p); at != "" {
		parts = append(parts, "at "+at)
	}
	if link := gitlabPushLink(p, ref, isTag); link != "" {
		parts = append(parts, link)
	}
	return strings.Join(nonEmpty(parts), " | ")
}

func gitlabCommitCount(p *gitlabPushPayload) *int {
	if p.TotalCommitsCount != nil {
		count := *p.TotalCommitsCount
		return &count
	}
	if len(p.Commits) > 0 {
		count := len(p.Commits)
		return &count
	}
	return nil
}

func commitUnit(count int) string {
	if count == 1 {
		return "commit"
	}
	return "commits"
}

// gitlabHeadMessage prefers the newest commit message (commits are listed
// oldest first and the push tip is the last entry) and falls back to the
// payload message used by annotated tag pushes.
func gitlabHeadMessage(p gitlabPushPayload, isTag bool) string {
	if len(p.Commits) > 0 {
		return trim(sanitize(firstNonEmpty(p.Commits[len(p.Commits)-1].Title, p.Commits[len(p.Commits)-1].Message)), maxHeadMessageBytes)
	}
	if isTag {
		return trim(sanitize(p.Message), maxHeadMessageBytes)
	}
	return ""
}

func gitlabPusher(username, name string) string {
	if username != "" {
		return mention(username)
	}
	return mention(name)
}

func gitlabNewestCommitTime(p gitlabPushPayload) string {
	if len(p.Commits) == 0 {
		return ""
	}
	if at, ok := parseGitlabTime(p.Commits[len(p.Commits)-1].Timestamp); ok {
		return at.UTC().Format(time.RFC3339)
	}
	return ""
}

// gitlabPushLink prefers the instance compare page for a real branch push,
// then the commit page of the new head, then the project page.
func gitlabPushLink(p gitlabPushPayload, ref string, isTag bool) string {
	base := firstURL(p.Project.WebURL, p.Project.Homepage)
	if base == "" {
		return ""
	}
	switch {
	case !isTag && zeroSHA(p.Before) && !zeroSHA(p.After):
		return base + "/-/commit/" + url.PathEscape(p.After)
	case !isTag && !zeroSHA(p.Before) && !zeroSHA(p.After):
		return base + "/-/compare/" + url.PathEscape(p.Before) + "..." + url.PathEscape(p.After)
	case isTag && ref != "" && !zeroSHA(p.After):
		return base + "/-/tags/" + url.PathEscape(ref)
	default:
		return base
	}
}

func zeroSHA(value string) bool {
	for _, r := range value {
		if r != '0' {
			return false
		}
	}
	return true
}

func formatGitlabMergeRequest(raw []byte) string {
	var p gitlabMergeRequestPayload
	_ = json.Unmarshal(raw, &p)
	repo := fallback(sanitize(p.Project.PathWithNamespace), "unknown repository")
	label := "gitlab:merge_request"
	if action := sanitize(p.ObjectAttributes.Action); action != "" {
		label += ":" + action
	}
	parts := []string{fmt.Sprintf("[%s] %s", label, repo)}
	if p.ObjectAttributes.IID > 0 {
		parts = append(parts, fmt.Sprintf("MR !%d %s", p.ObjectAttributes.IID, sanitize(p.ObjectAttributes.Title)))
	} else if title := sanitize(p.ObjectAttributes.Title); title != "" {
		parts = append(parts, "MR "+title)
	}
	if author := gitlabPusher(p.User.Username, p.User.Name); author != "" {
		parts = append(parts, "by "+author)
	}
	if source, target := sanitize(p.ObjectAttributes.SourceBranch), sanitize(p.ObjectAttributes.TargetBranch); source != "" && target != "" {
		parts = append(parts, source+" → "+target)
	} else if source != "" || target != "" {
		parts = append(parts, firstNonEmpty(source, target))
	}
	if state := sanitize(p.ObjectAttributes.State); state != "" {
		parts = append(parts, "state "+state)
	}
	if updated, ok := parseGitlabTime(p.ObjectAttributes.UpdatedAt); ok {
		parts = append(parts, "updated at "+updated.UTC().Format(time.RFC3339))
	} else if created, ok := parseGitlabTime(p.ObjectAttributes.CreatedAt); ok {
		parts = append(parts, "created at "+created.UTC().Format(time.RFC3339))
	}
	parts = append(parts, firstURL(p.ObjectAttributes.URL, p.Project.WebURL, p.Project.Homepage))
	return strings.Join(nonEmpty(parts), " | ")
}

func formatGitlabUnknown(item store.Item) string {
	var p struct {
		Project gitlabProject `json:"project"`
	}
	_ = json.Unmarshal(item.RawJSON, &p)
	repo := fallback(sanitize(p.Project.PathWithNamespace), "unknown repository")
	return fmt.Sprintf("[%s] %s", item.Event, repo)
}

func gitlabProjectPath(raw []byte) string {
	var p struct {
		Project gitlabProject `json:"project"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return ""
	}
	return p.Project.PathWithNamespace
}

func gitlabRefParts(ref string) (string, bool) {
	ref = sanitize(ref)
	switch {
	case strings.HasPrefix(ref, "refs/heads/"):
		return strings.TrimPrefix(ref, "refs/heads/"), false
	case strings.HasPrefix(ref, "refs/tags/"):
		return strings.TrimPrefix(ref, "refs/tags/"), true
	default:
		return ref, false
	}
}

// parseGitlabTime accepts the RFC3339 timestamps GitLab uses for commits and
// the "2016-08-12 15:23:28 UTC" layout used by pipeline and merge request
// object attributes.
func parseGitlabTime(value string) (time.Time, bool) {
	if value = strings.TrimSpace(value); value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05 MST", "2006-01-02 15:04:05 -0700", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

// gitlabRichText renders a GitLab event line for a Lark post. Every " | "
// segment that is a standalone https URL becomes a clickable link; all other
// content is plain text (repository and commit URLs are never fabricated).
func gitlabRichText(line string) []lark.Text {
	parts := strings.Split(line, " | ")
	segments := make([]lark.Text, 0, len(parts)*2)
	for i, part := range parts {
		if i > 0 {
			segments = append(segments, lark.Text{Tag: "text", Text: " | "})
		}
		if gitlabHTTPSURL(part) {
			segments = append(segments, lark.Text{Tag: "a", Text: strings.TrimPrefix(part, "https://"), Href: part})
		} else {
			segments = append(segments, lark.Text{Tag: "text", Text: part})
		}
	}
	return segments
}

func gitlabHTTPSURL(value string) bool {
	if strings.ContainsAny(value, " \t\r\n") {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.Path != ""
}
