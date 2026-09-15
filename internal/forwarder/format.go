package forwarder

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/undefined-moe/lark-git-webhook/internal/geoip"
	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

const (
	maxHeadMessageBytes   = 240
	maxCommitMessageBytes = 240
)

type githubPayload struct {
	Action     string `json:"action"`
	Repository struct {
		FullName string `json:"full_name"`
		HTMLURL  string `json:"html_url"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
	Compare    string `json:"compare"`
	Ref        string `json:"ref"`
	Before     string `json:"before"`
	After      string `json:"after"`
	Size       *int   `json:"size"`
	HeadCommit struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	} `json:"head_commit"`
	Commits []struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	} `json:"commits"`
	PullRequest struct {
		Title   string `json:"title"`
		HTMLURL string `json:"html_url"`
		Number  int    `json:"number"`
	} `json:"pull_request"`
	Issue struct {
		Title   string `json:"title"`
		HTMLURL string `json:"html_url"`
		Number  int    `json:"number"`
	} `json:"issue"`
	Comment struct {
		HTMLURL string `json:"html_url"`
	} `json:"comment"`
	Release struct {
		Name    string `json:"name"`
		HTMLURL string `json:"html_url"`
		TagName string `json:"tag_name"`
	} `json:"release"`
	WorkflowRun struct {
		Name       string `json:"name"`
		HTMLURL    string `json:"html_url"`
		Conclusion string `json:"conclusion"`
	} `json:"workflow_run"`
	CheckRun struct {
		Name       string `json:"name"`
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		DetailsURL string `json:"details_url"`
		App        struct {
			Slug string `json:"slug"`
		} `json:"app"`
	} `json:"check_run"`
	CheckSuite struct {
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		HeadBranch string `json:"head_branch"`
		App        struct {
			Slug string `json:"slug"`
		} `json:"app"`
	} `json:"check_suite"`
	Label struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Color       string `json:"color"`
	} `json:"label"`
	RefType string `json:"ref_type"`
}

// GeoLookup returns a formatted offline MaxMind location for an IP address.
type GeoLookup func(string) string

// Format formats an item without an optional MaxMind lookup.
func Format(item store.Item) string {
	return format(item, nil)
}

func format(item store.Item, lookup GeoLookup) string {
	if item.Event == "audit_log" {
		return formatAuditLog(item.RawJSON, lookup)
	}
	if strings.HasPrefix(item.Event, "gitlab:") {
		return formatGitlab(item)
	}
	var p githubPayload
	_ = json.Unmarshal(item.RawJSON, &p)
	repo, sender := fallback(sanitize(p.Repository.FullName), "unknown repository"), mention(p.Sender.Login)
	displayRepo := repo
	if branch := pushBranch(item.Event, p.Ref); branch != "" && repo != "unknown repository" {
		displayRepo += "/" + branch
	}
	label := item.Event
	if p.Action != "" {
		label += ":" + sanitize(p.Action)
	}
	parts := []string{fmt.Sprintf("[%s] %s", label, displayRepo)}
	switch item.Event {
	case "push":
		parts = append(parts, pushParts(p)...)
	case "pull_request":
		parts = append(parts, fmt.Sprintf("PR #%d %s", p.PullRequest.Number, sanitize(p.PullRequest.Title)), firstURL(p.PullRequest.HTMLURL))
	case "label":
		parts = append(parts, "label "+fallback(sanitize(p.Label.Name), "unknown"))
		if description := sanitize(p.Label.Description); description != "" {
			parts = append(parts, description)
		}
		if color := sanitize(p.Label.Color); color != "" {
			parts = append(parts, "color #"+color)
		}
	case "issues":
		parts = append(parts, fmt.Sprintf("issue #%d %s", p.Issue.Number, sanitize(p.Issue.Title)), firstURL(p.Issue.HTMLURL, p.Repository.HTMLURL))
	case "issue_comment":
		parts = append(parts, fmt.Sprintf("issue comment by %s", sender), firstURL(p.Comment.HTMLURL, p.Issue.HTMLURL, p.Repository.HTMLURL))
	case "release":
		parts = append(parts, fmt.Sprintf("release %s", fallback(sanitize(p.Release.Name), sanitize(p.Release.TagName))), firstURL(p.Release.HTMLURL, p.Repository.HTMLURL))
	case "workflow_run":
		parts = append(parts, fmt.Sprintf("workflow %s %s", sanitize(p.WorkflowRun.Name), sanitize(p.WorkflowRun.Conclusion)), firstURL(p.WorkflowRun.HTMLURL, p.Repository.HTMLURL))
	case "check_run":
		if name := sanitize(p.CheckRun.Name); name != "" {
			parts = append(parts, "check "+name)
		}
		if conclusion := sanitize(p.CheckRun.Conclusion); conclusion != "" {
			parts = append(parts, conclusion)
		} else if status := sanitize(p.CheckRun.Status); status != "" {
			parts = append(parts, status)
		}
		if app := sanitize(p.CheckRun.App.Slug); app != "" {
			parts = append(parts, "via "+app)
		}
		parts = append(parts, firstURL(p.CheckRun.DetailsURL))
	case "check_suite":
		if branch := sanitize(p.CheckSuite.HeadBranch); branch != "" {
			parts = append(parts, "branch "+branch)
		}
		if conclusion := sanitize(p.CheckSuite.Conclusion); conclusion != "" {
			parts = append(parts, conclusion)
		} else if status := sanitize(p.CheckSuite.Status); status != "" {
			parts = append(parts, status)
		}
		if app := sanitize(p.CheckSuite.App.Slug); app != "" {
			parts = append(parts, "via "+app)
		}
	case "create", "delete":
		parts = append(parts, fmt.Sprintf("%s %s %s", item.Event, sanitize(p.RefType), sanitize(p.Ref)), firstURL(p.Repository.HTMLURL))
	default:
		parts = append(parts, sender, firstURL(p.Repository.HTMLURL))
	}
	line := strings.Join(nonEmpty(parts), " | ")
	if item.Event != "push" {
		return trim(line, 1800)
	}
	return strings.Join(append([]string{line}, pushCommitLines(p)...), "\n")
}

func formatAuditLog(raw []byte, lookup GeoLookup) string {
	var record map[string]any
	_ = json.Unmarshal(raw, &record)
	value := func(keys ...string) string {
		for _, key := range keys {
			if v, ok := record[key].(string); ok && sanitize(v) != "" {
				return sanitize(v)
			}
		}
		return ""
	}
	action := fallback(value("action"), "unknown")
	org := fallback(value("org", "organization"), "unknown organization")
	actor := value("actor", "actor_login", "user")
	target := value("repo", "repository", "target")
	ip := value("actor_ip", "ip_address")
	parsedIP := geoip.ParseIP(ip)
	githubGeo := auditGeo(record)
	maxMindGeo := ""
	if lookup != nil && geoip.IsPublicRoutable(parsedIP) {
		maxMindGeo = sanitize(lookup(ip))
	}
	at := value("created_at", "@timestamp", "timestamp")
	parts := []string{fmt.Sprintf("[audit:%s] %s", action, org)}
	if actor != "" {
		parts = append(parts, "actor "+actor)
	}
	if target != "" {
		parts = append(parts, "target "+target)
	}
	if parsedIP != nil {
		parts = append(parts, "IP "+ip)
	}
	if githubGeo != "" {
		parts = append(parts, "github_geo "+githubGeo)
	}
	if maxMindGeo != "" {
		parts = append(parts, "maxmind_geo "+maxMindGeo)
	}
	if at != "" {
		parts = append(parts, at)
	}
	return trim(strings.Join(parts, " | "), 1800)
}

func auditGeo(record map[string]any) string {
	location, _ := record["actor_location"].(map[string]any)
	value := func(keys ...string) string {
		for _, source := range []map[string]any{location, record} {
			for _, key := range keys {
				if v, ok := source[key].(string); ok && sanitize(v) != "" {
					return sanitize(v)
				}
			}
		}
		return ""
	}

	values := make([]string, 0, 4)
	if locationText, ok := record["actor_location"].(string); ok && sanitize(locationText) != "" {
		values = append(values, sanitize(locationText))
	}
	for _, keys := range [][]string{
		{"country", "country_code"},
		{"region", "region_name", "state", "state_name"},
		{"city", "city_name"},
		{"location", "location_name", "display_name", "name", "label"},
	} {
		if v := value(keys...); v != "" {
			values = append(values, v)
		}
	}
	unique := values[:0]
	for _, value := range values {
		duplicate := false
		for _, seen := range unique {
			if value == seen {
				duplicate = true
				break
			}
		}
		if !duplicate {
			unique = append(unique, value)
		}
	}
	return strings.Join(unique, ", ")
}

func pushParts(p githubPayload) []string {
	parts := []string{pushRef(p.Ref)}
	before, after := shortSHA(p.Before), shortSHA(p.After)
	if after == "" {
		after = shortSHA(p.HeadCommit.ID)
	}
	if before != "" && after != "" {
		parts = append(parts, before+".."+after)
	}
	commitCount := len(p.Commits)
	if p.Size != nil {
		commitCount = *p.Size
	}
	if commitCount >= 0 && (p.Size != nil || len(p.Commits) > 0) {
		unit := "commits"
		if commitCount == 1 {
			unit = "commit"
		}
		parts = append(parts, fmt.Sprintf("%d %s", commitCount, unit))
	}
	if len(p.Commits) == 0 {
		if message := trim(sanitize(p.HeadCommit.Message), maxHeadMessageBytes); message != "" {
			parts = append(parts, message)
		}
	}
	parts = append(parts, mention(p.Sender.Login), firstURL(p.Compare))
	return parts
}

func pushCommitLines(p githubPayload) []string {
	lines := make([]string, 0, len(p.Commits))
	for _, commit := range p.Commits {
		sha := sanitize(commit.ID)
		if validCommitSHA(sha) {
			sha = "commit:" + sha
		} else {
			sha = shortSHA(sha)
		}
		message := trim(sanitize(commit.Message), maxCommitMessageBytes)
		if line := strings.Join(nonEmpty([]string{sha, message}), " | "); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func pushBranch(event, ref string) string {
	ref = sanitize(ref)
	if event == "push" && strings.HasPrefix(ref, "refs/heads/") {
		return strings.TrimPrefix(ref, "refs/heads/")
	}
	return ""
}

func pushRef(ref string) string {
	ref = sanitize(ref)
	switch {
	case strings.HasPrefix(ref, "refs/heads/"):
		return ""
	case strings.HasPrefix(ref, "refs/tags/"):
		return "tag " + strings.TrimPrefix(ref, "refs/tags/")
	case ref != "":
		return "ref " + ref
	default:
		return ""
	}
}

func shortSHA(value string) string {
	value = sanitize(value)
	if len(value) > 7 {
		return value[:7]
	}
	return value
}

func validCommitSHA(value string) bool {
	if len(value) < 7 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

// FormatForLimit formats an item without an optional MaxMind lookup and fits it within the message limit.
func FormatForLimit(item store.Item, maxBytes int) string {
	return formatForLimit(item, maxBytes, nil)
}

func formatForLimit(item store.Item, maxBytes int, lookup GeoLookup) string {
	line := format(item, lookup)
	if serializedMessageSize([]string{line})+300 <= maxBytes {
		return line
	}
	best := ""
	for low, high := 0, len(line); low <= high; {
		middle := low + (high-low)/2
		candidate := trim(line, middle)
		if serializedMessageSize([]string{candidate})+300 <= maxBytes {
			best = candidate
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	return best
}

func serializedMessageSize(lines []string) int {
	size, _ := lark.PostRequestBodySize("", MakeMessage(lines))
	return size
}

func MakeMessage(lines []string) lark.Message {
	content := make([][]lark.Text, 0, len(lines))
	for _, line := range lines {
		repo := pushRepository(line)
		for _, paragraph := range strings.Split(line, "\n") {
			content = append(content, richTextForRepo(paragraph, repo))
		}
	}
	return lark.Message{MsgType: "post", Content: lark.Content{Post: lark.Post{"zh_cn": {Content: content}}}}
}

func richText(line string) []lark.Text {
	return richTextForRepo(line, "")
}

func richTextForRepo(line, inheritedRepo string) []lark.Text {
	if strings.HasPrefix(line, "[audit:") {
		return []lark.Text{{Tag: "text", Text: line}}
	}
	if strings.HasPrefix(line, "[gitlab:") {
		return gitlabRichText(line)
	}
	parts := strings.Split(line, " | ")
	if len(parts) == 0 {
		return []lark.Text{{Tag: "text", Text: line}}
	}
	segments := make([]lark.Text, 0, len(parts)*2)
	repo := inheritedRepo
	isCommitLine := false
	if marker := strings.Index(parts[0], "] "); marker >= 0 {
		displayRepo := parts[0][marker+2:]
		repo = displayRepo
		repoHref := "https://github.com/" + displayRepo
		if strings.HasPrefix(parts[0], "[push") {
			if repoParts := strings.SplitN(displayRepo, "/", 3); len(repoParts) == 3 {
				repo = repoParts[0] + "/" + repoParts[1]
				repoHref = githubBranchURL(repo, repoParts[2])
			}
		}
		if displayRepo != "" && displayRepo != "unknown repository" {
			segments = append(segments,
				lark.Text{Tag: "text", Text: parts[0][:marker+2]},
				lark.Text{Tag: "a", Text: displayRepo, Href: repoHref},
			)
		} else {
			segments = append(segments, lark.Text{Tag: "text", Text: parts[0]})
		}
	} else if sha := commitSHA(parts[0]); sha != "" {
		isCommitLine = true
		if validGitHubRepository(repo) {
			segments = append(segments, lark.Text{Tag: "a", Text: shortSHA(sha), Href: githubCommitURL(repo, sha)})
		} else if strings.HasPrefix(repo, "https://") {
			segments = append(segments, lark.Text{Tag: "a", Text: shortSHA(sha), Href: gitlabCommitURL(repo, sha)})
		} else {
			segments = append(segments, lark.Text{Tag: "text", Text: shortSHA(sha)})
		}
	} else {
		segments = append(segments, lark.Text{Tag: "text", Text: parts[0]})
	}
	repoURL := "https://github.com/" + repo
	for i := 1; i < len(parts); i++ {
		part := parts[i]
		if repo != "" && part == repoURL {
			continue
		}
		segments = append(segments, lark.Text{Tag: "text", Text: " | "})
		if !isCommitLine && strings.HasPrefix(part, "PR #") && i+1 < len(parts) && strings.HasPrefix(parts[i+1], "https://github.com/") {
			segments = append(segments, lark.Text{Tag: "a", Text: part, Href: parts[i+1]})
			i++
		} else if !isCommitLine && strings.HasPrefix(part, "https://github.com/") {
			segments = append(segments, lark.Text{Tag: "a", Text: strings.TrimPrefix(part, "https://github.com/"), Href: part})
		} else if !isCommitLine && strings.HasPrefix(part, "https://") {
			segments = append(segments, lark.Text{Tag: "a", Text: part, Href: part})
		} else {
			segments = append(segments, lark.Text{Tag: "text", Text: part})
		}
	}
	return segments
}

func pushRepository(line string) string {
	line, _, _ = strings.Cut(line, "\n")
	marker := strings.Index(line, "] ")
	if marker < 0 {
		return ""
	}
	if strings.HasPrefix(line, "[gitlab:") {
		return gitlabProjectBase(line)
	}
	if !strings.HasPrefix(line, "[push") {
		return ""
	}
	displayRepo, _, _ := strings.Cut(line[marker+2:], " | ")
	if repoParts := strings.SplitN(displayRepo, "/", 3); len(repoParts) >= 2 {
		return repoParts[0] + "/" + repoParts[1]
	}
	return displayRepo
}

// gitlabProjectBase recovers the project web URL from a GitLab push header so
// its commit lines can link to the GitLab instance; the header's trailing link
// always starts with the project base followed by "/-/".
func gitlabProjectBase(line string) string {
	for _, part := range strings.Split(line, " | ") {
		base, ok := strings.CutPrefix(part, "https://")
		if !ok || base == "" {
			continue
		}
		if bare, _, _ := strings.Cut(base, "/-/"); bare != "" {
			return "https://" + bare
		}
	}
	return ""
}

func gitlabCommitURL(base, sha string) string {
	return base + "/-/commit/" + sha
}

func commitSHA(value string) string {
	sha, ok := strings.CutPrefix(value, "commit:")
	if !ok || !validCommitSHA(sha) {
		return ""
	}
	return sha
}

func validGitHubRepository(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
				return false
			}
		}
	}
	return true
}

func githubCommitURL(repo, sha string) string {
	return "https://github.com/" + repo + "/commit/" + sha
}

func githubBranchURL(repo, branch string) string {
	parts := strings.Split(branch, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return "https://github.com/" + repo + "/tree/" + strings.Join(parts, "/")
}

func firstURL(urls ...string) string {
	for _, u := range urls {
		if u = sanitize(u); u != "" {
			return u
		}
	}
	return ""
}
func mention(login string) string {
	if login == "" {
		return "unknown sender"
	}
	return "@" + sanitize(login)
}

func fallback(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
func nonEmpty(parts []string) []string {
	out := parts[:0]
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
func sanitize(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	return strings.ReplaceAll(strings.Join(strings.Fields(value), " "), " | ", " / ")
}
func trim(value string, max int) string {
	if max <= len("…") {
		return ""
	}
	if len(value) <= max {
		return value
	}
	cut := max - len("…")
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + "…"
}
