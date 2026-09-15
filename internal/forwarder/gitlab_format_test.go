package forwarder

import (
	"strings"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func TestFormatGitlabPushIncludesExpectedFields(t *testing.T) {
	item := store.Item{Event: "gitlab:push", DeliveryID: "delivery-gitlab-123", RawJSON: []byte(`{
		"object_kind": "push",
		"before": "95790bf891e76fee5e1747ab589903a6a1f80f22",
		"after": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
		"ref": "refs/heads/master",
		"checkout_sha": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
		"message": "Hello World",
		"user_name": "John Smith",
		"user_username": "jsmith",
		"project": {"id": 15, "name": "Diaspora", "path_with_namespace": "mike/diaspora", "web_url": "https://gitlab.example.com/mike/diaspora", "homepage": "https://gitlab.example.com/mike/diaspora"},
		"commits": [
			{"id": "b6568db1bc1dcd7f8b4d5a946b0b91f9dacd7327", "message": "Update Catalan translation", "title": "Update Catalan translation", "timestamp": "2011-12-12T14:27:31+02:00", "url": "https://gitlab.example.com/mike/diaspora/commit/b6568db1bc1dcd7f8b4d5a946b0b91f9dacd7327"},
			{"id": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7", "message": "fixed readme", "title": "fixed readme", "timestamp": "2012-01-03T23:36:29+02:00", "url": "https://gitlab.example.com/mike/diaspora/commit/da1560886d4f094c3e6c9ef40349f7d38b5d27d7"}
		],
		"total_commits_count": 4
	}`)}
	line := Format(item)
	for _, text := range []string{
		"[gitlab:push] mike/diaspora/master",
		"95790bf..da15608",
		"4 commits",
		"@jsmith",
		"https://gitlab.example.com/mike/diaspora/-/compare/95790bf891e76fee5e1747ab589903a6a1f80f22...da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
		"commit:b6568db1bc1dcd7f8b4d5a946b0b91f9dacd7327 | Update Catalan translation",
		"commit:da1560886d4f094c3e6c9ef40349f7d38b5d27d7 | fixed readme",
	} {
		if !strings.Contains(line, text) {
			t.Fatalf("missing %q in %q", text, line)
		}
	}
	for _, forbidden := range []string{"delivery=", "unknown repository", "refs/heads/", "branch master", "at "} {
		if strings.Contains(line, forbidden) {
			t.Fatalf("unexpected %q in %q", forbidden, line)
		}
	}
}

func TestFormatGitlabPushUsesTotalCommitCountWhenTruncated(t *testing.T) {
	var raw strings.Builder
	raw.WriteString(`{"ref":"refs/heads/main","project":{"path_with_namespace":"group/project","web_url":"https://gitlab.example.com/group/project"},"commits":[`)
	for i := 0; i < 20; i++ {
		if i > 0 {
			raw.WriteByte(',')
		}
		raw.WriteString(`{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","title":"c"}`)
	}
	raw.WriteString(`],"total_commits_count":25}`)
	line := Format(store.Item{Event: "gitlab:push", RawJSON: []byte(raw.String())})
	if !strings.Contains(line, "25 commits") {
		t.Fatalf("payload count not honored in %q", line)
	}
}

func TestFormatGitlabTagPushIncludesTagMessage(t *testing.T) {
	line := Format(store.Item{Event: "gitlab:tag_push", RawJSON: []byte(`{
		"object_kind": "tag_push",
		"before": "0000000000000000000000000000000000000000",
		"after": "82b3d5ae55f7080f1e6022629cdb57bfae7cccc7",
		"ref": "refs/tags/v1.0.0",
		"message": "Tag message",
		"user_name": "John Smith",
		"user_username": "jsmith",
		"project": {"id": 1, "name": "Example", "path_with_namespace": "jsmith/example", "web_url": "https://gitlab.example.com/jsmith/example", "homepage": "https://gitlab.example.com/jsmith/example"},
		"commits": [],
		"total_commits_count": 0
	}`)})
	for _, text := range []string{
		"[gitlab:tag_push] jsmith/example",
		"tag v1.0.0",
		"0000000..82b3d5a",
		"0 commits",
		"Tag message",
		"@jsmith",
		"https://gitlab.example.com/jsmith/example/-/tags/v1.0.0",
	} {
		if !strings.Contains(line, text) {
			t.Fatalf("missing %q in %q", text, line)
		}
	}
	if strings.Contains(line, "at ") {
		t.Fatalf("tag push without commit timestamps shows a time in %q", line)
	}
}

func TestFormatGitlabMergeRequestIncludesActionAndRefs(t *testing.T) {
	item := store.Item{Event: "gitlab:merge_request", RawJSON: []byte(`{
		"object_kind": "merge_request",
		"user": {"name": "Administrator", "username": "root"},
		"project": {"id": 1, "name": "Gitlab Test", "path_with_namespace": "gitlabhq/gitlab-test", "web_url": "https://gitlab.example.com/gitlabhq/gitlab-test", "homepage": "https://gitlab.example.com/gitlabhq/gitlab-test"},
		"object_attributes": {
			"iid": 1,
			"title": "MS-Viewport",
			"state": "opened",
			"action": "open",
			"url": "https://gitlab.example.com/gitlabhq/gitlab-test/-/merge_requests/1",
			"source_branch": "ms-viewport",
			"target_branch": "master",
			"author_id": 10,
			"created_at": "2026-08-01 12:00:00 UTC",
			"updated_at": "2026-08-01 13:30:00 UTC"
		}
	}`)}
	line := Format(item)
	for _, text := range []string{
		"[gitlab:merge_request:open] gitlabhq/gitlab-test",
		"MR !1 MS-Viewport",
		"by @root",
		"ms-viewport → master",
		"state opened",
		"updated at 2026-08-01T13:30:00Z",
		"https://gitlab.example.com/gitlabhq/gitlab-test/-/merge_requests/1",
	} {
		if !strings.Contains(line, text) {
			t.Fatalf("missing %q in %q", text, line)
		}
	}
}

func TestMakeMessageLinksOnlyStandaloneGitlabHTTPSURLs(t *testing.T) {
	for _, tc := range []struct {
		url       string
		wantLink  bool
		wantShort string
	}{
		{url: "https://gitlab.example.com/a/b/-/merge_requests/7", wantLink: true, wantShort: "gitlab.example.com/a/b/-/merge_requests/7"},
		{url: "http://gitlab.example.com/a/b"},
		{url: "mailto:ci@example.com"},
	} {
		line := "[gitlab:push] group/project | branch main | " + tc.url
		segments := MakeMessage([]string{line}).Content.Post["zh_cn"].Content[0]
		found := false
		for _, segment := range segments {
			if segment.Text != tc.url && segment.Text != tc.wantShort {
				continue
			}
			found = true
			if tc.wantLink && (segment.Tag != "a" || segment.Href != tc.url) {
				t.Fatalf("URL was not linked: %+v", segment)
			}
			if !tc.wantLink && (segment.Tag != "text" || segment.Href != "") {
				t.Fatalf("URL was linked: %+v", segment)
			}
			if tc.wantLink && segment.Tag == "a" && segment.Text != tc.wantShort {
				t.Fatalf("link text=%q want=%q", segment.Text, tc.wantShort)
			}
		}
		if !found {
			t.Fatalf("URL segment missing from %+v", segments)
		}
	}
}

func TestMakeMessageGitlabRendersNoGithubLinks(t *testing.T) {
	line := "[gitlab:merge_request:merge] gitlabhq/gitlab-test | MR !1 MS-Viewport | by @root | ms-viewport → master | https://gitlab.example.com/gitlabhq/gitlab-test/-/merge_requests/1"
	segments := MakeMessage([]string{line}).Content.Post["zh_cn"].Content[0]
	allowed := map[string]bool{
		"https://gitlab.example.com/gitlabhq/gitlab-test":                    true,
		"https://gitlab.example.com/gitlabhq/gitlab-test/-/merge_requests/1": true,
	}
	for _, segment := range segments {
		if segment.Href != "" && !allowed[segment.Href] {
			t.Fatalf("unexpected link href %+v", segment)
		}
		if strings.Contains(segment.Text, "github.com") {
			t.Fatalf("github URL fabricated in %+v", segment)
		}
	}
}

func TestMakeMessageLinksGitlabRepositoryHeader(t *testing.T) {
	for _, tc := range []struct {
		name     string
		line     string
		repoText string
		repoHref string
	}{
		{
			name:     "branch push links to tree page",
			line:     "[gitlab:push] group/project/main | 1111111..2222222 | 1 commit | @root | https://gitlab.example.com/group/project/-/compare/1111111111111111111111111111111111111111...2222222222222222222222222222222222222222",
			repoText: "group/project/main",
			repoHref: "https://gitlab.example.com/group/project/-/tree/main",
		},
		{
			name:     "subgroup branch push links to tree page",
			line:     "[gitlab:push] group/sub/repo/feat/x | 1111111..2222222 | @root | https://gitlab.example.com/group/sub/repo/-/compare/1111111111111111111111111111111111111111...2222222222222222222222222222222222222222",
			repoText: "group/sub/repo/feat/x",
			repoHref: "https://gitlab.example.com/group/sub/repo/-/tree/feat/x",
		},
		{
			name:     "tag push links to project page",
			line:     "[gitlab:tag_push] jsmith/example | tag v1.0.0 | @jsmith | https://gitlab.example.com/jsmith/example/-/tags/v1.0.0",
			repoText: "jsmith/example",
			repoHref: "https://gitlab.example.com/jsmith/example",
		},
		{
			name:     "merge request links to project page",
			line:     "[gitlab:merge_request:open] gitlabhq/gitlab-test | MR !1 MS-Viewport | https://gitlab.example.com/gitlabhq/gitlab-test/-/merge_requests/1",
			repoText: "gitlabhq/gitlab-test",
			repoHref: "https://gitlab.example.com/gitlabhq/gitlab-test",
		},
		{
			name:     "no project URL leaves repository as plain text",
			line:     "[gitlab:push] group/project/main | 1111111..2222222 | @root",
			repoText: "group/project/main",
			repoHref: "",
		},
	} {
		segments := MakeMessage([]string{tc.line}).Content.Post["zh_cn"].Content[0]
		var href string
		for _, segment := range segments {
			if segment.Text == tc.repoText {
				href = segment.Href
			}
		}
		if href != tc.repoHref {
			t.Fatalf("%s: repository link=%q want %q in %+v", tc.name, href, tc.repoHref, segments)
		}
	}
}

func TestMakeMessageLinksGitlabCommitLinesToInstance(t *testing.T) {
	line := "[gitlab:push] group/project/main | 1111111..2222222 | 1 commit | @root | https://gitlab.example.com/group/project/-/compare/1111111111111111111111111111111111111111...2222222222222222222222222222222222222222\ncommit:2222222222222222222222222222222222222222 | fix: everything"
	content := MakeMessage([]string{line}).Content.Post["zh_cn"].Content
	if len(content) != 2 {
		t.Fatalf("paragraphs=%d want 2", len(content))
	}
	var href string
	for _, segment := range content[1] {
		if segment.Tag == "a" {
			href = segment.Href
		}
	}
	if href != "https://gitlab.example.com/group/project/-/commit/2222222222222222222222222222222222222222" {
		t.Fatalf("commit link=%q want GitLab commit URL in %+v", href, content[1])
	}
}

func TestFormatGitlabPushFitsMessageLimit(t *testing.T) {
	item := store.Item{Event: "gitlab:push", RawJSON: []byte(`{
		"ref": "refs/heads/main",
		"before": "1111111111111111111111111111111111111111",
		"after": "2222222222222222222222222222222222222222",
		"project": {"path_with_namespace": "group/project", "web_url": "https://gitlab.example.com/group/project"},
		"commits": [{"id": "2222222222222222222222222222222222222222", "message": "` + strings.Repeat("x", 2000) + `", "title": "` + strings.Repeat("y", 2000) + `"}],
		"total_commits_count": 1
	}`)}
	line := Format(item)
	if strings.Contains(line, "delivery=") {
		t.Fatalf("delivery marker shown in %q", line)
	}
	if got := serializedMessageSize([]string{line}) + 300; got > 15360 {
		t.Fatalf("unlimited push message is %d bytes", got)
	}
	limited := FormatForLimit(item, 1024)
	if serializedMessageSize([]string{limited})+300 > 1024 {
		t.Fatalf("limited push exceeds 1024 bytes: %q", limited)
	}
}

func TestParseGitlabTimeLayouts(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
		ok    bool
	}{
		{value: "2012-01-03T23:36:29+02:00", want: "2012-01-03T21:36:29Z", ok: true},
		{value: "2016-08-12 15:23:28 UTC", want: "2016-08-12T15:23:28Z", ok: true},
		{value: "2016-08-12 15:23:28 +0200", want: "2016-08-12T13:23:28Z", ok: true},
		{value: "2016-08-12 15:23:28", want: "2016-08-12T15:23:28Z", ok: true},
		{value: "", ok: false},
		{value: "not-a-time", ok: false},
	} {
		parsed, ok := parseGitlabTime(tc.value)
		if ok != tc.ok {
			t.Fatalf("parseGitlabTime(%q) ok=%v want=%v", tc.value, ok, tc.ok)
		}
		if ok && parsed.UTC().Format(time.RFC3339) != tc.want {
			t.Fatalf("parseGitlabTime(%q)=%s want=%s", tc.value, parsed.UTC().Format(time.RFC3339), tc.want)
		}
	}
}

func TestFormatGitlabRichTextLeavesMessageEmbeddedURLsAsText(t *testing.T) {
	// A URL embedded inside a longer segment must not be turned into a link.
	const line = "[gitlab:push] group/project | branch main | Update docs see https://gitlab.example.com/x for details"
	segments := MakeMessage([]string{line}).Content.Post["zh_cn"].Content[0]
	for _, segment := range segments {
		if segment.Tag == "a" {
			t.Fatalf("embedded URL became a link: %+v", segments)
		}
	}
	var text strings.Builder
	for _, segment := range segments {
		text.WriteString(segment.Text)
	}
	if text.String() != line {
		t.Fatalf("message text corrupted: %q want %q", text.String(), line)
	}
}

func TestFormatGitlabPushBatchSplittingUnderLimit(t *testing.T) {
	raw := []byte(`{"ref":"refs/heads/main","project":{"path_with_namespace":"group/project","web_url":"https://gitlab.example.com/group/project"},"commits":[{"id":"2222222222222222222222222222222222222222","message":"` + strings.Repeat("x", 4000) + `","title":"` + strings.Repeat("x", 4000) + `"}],"total_commits_count":1}`)
	items := []store.Item{
		{Event: "gitlab:push", RawJSON: raw},
		{Event: "gitlab:push", RawJSON: raw},
	}
	batches := split(items, 1024)
	if len(batches) != 2 {
		t.Fatalf("batches=%d want 2", len(batches))
	}
	for _, batch := range batches {
		if got := messageSize(batch, 1024) + 300; got > 1024 {
			t.Fatalf("gitlab batch is %d bytes", got)
		}
	}
}
