package forwarder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/lark"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func TestWorkerBatchesAndDeletesDeliveries(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	now := time.Now()
	for _, id := range []string{"delivery-111111", "delivery-222222", "delivery-333333"} {
		if _, err := s.Enqueue(id, "push", []byte(`{"repository":{"full_name":"acme/repo"}}`), now, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var sent []string
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := NewWorker(s, func(_ context.Context, message lark.Message) error {
		mu.Lock()
		calls++
		mu.Unlock()
		for _, paragraph := range message.Content.Post["zh_cn"].Content {
			var text strings.Builder
			for _, segment := range paragraph {
				text.WriteString(segment.Text)
			}
			mu.Lock()
			sent = append(sent, text.String())
			mu.Unlock()
		}
		cancel()
		return nil
	}, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("remaining=%d err=%v", len(items), err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("batch sends=%d", calls)
	}
	if len(sent) != 3 {
		t.Fatalf("sent=%v", sent)
	}
	for _, line := range sent {
		if strings.Contains(line, "delivery=") {
			t.Fatalf("delivery marker shown in %q", line)
		}
	}
}

func TestWorkerStopsAfterSuccessfulSendDeleteFailure(t *testing.T) {
	s := openStore(t)
	if _, err := s.Enqueue("delivery", "push", []byte(`{}`), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	closed := make(chan error, 1)
	w := NewWorker(s, func(context.Context, lark.Message) error {
		sends.Add(1)
		closed <- s.Close()
		return nil
	}, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	done := make(chan struct{})
	go func() { w.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after delete failure")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if w.Alive(time.Now()) {
		t.Fatal("worker remained healthy after delete failure")
	}
	if sends.Load() != 1 {
		t.Fatalf("successful delivery resent %d times", sends.Load())
	}
}

func TestWorkerStopsAfterFailureStatePersistenceError(t *testing.T) {
	s := openStore(t)
	if _, err := s.Enqueue("delivery", "push", []byte(`{}`), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	w := NewWorker(s, func(context.Context, lark.Message) error {
		closed <- s.Close()
		return errors.New("send failed")
	}, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	done := make(chan struct{})
	go func() { w.Run(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after failure-state persistence error")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if w.Alive(time.Now()) {
		t.Fatal("worker remained healthy after failure-state persistence error")
	}
}

func TestWorkerSplitsBatchesUnderLimit(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	now := time.Now()
	for i := 0; i < 9; i++ {
		raw := []byte(`{"pull_request":{"title":"` + strings.Repeat("x", 4000) + `"}}`)
		if _, err := s.Enqueue(string(rune('a'+i)), "pull_request", raw, now, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.Due(now, 10)
	if err != nil {
		t.Fatal(err)
	}
	batches := split(items, 15360)
	if len(batches) < 2 {
		t.Fatalf("expected split batches, got %d", len(batches))
	}
	seen := 0
	for _, batch := range batches {
		if got := messageSize(batch, 15360) + 300; got > 15360 {
			t.Fatalf("message is %d bytes", got)
		}
		seen += len(batch)
	}
	if seen != 9 {
		t.Fatalf("seen=%d", seen)
	}
}

func TestWorkerSplitsQuoteHeavyPushBatchesUnderLimit(t *testing.T) {
	const (
		sha             = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		maxMessageBytes = 10000
	)
	message := strings.Repeat(`\"\\`, 120)
	var raw strings.Builder
	raw.WriteString(`{"repository":{"full_name":"acme/repo"},"commits":[`)
	for i := 0; i < 5; i++ {
		if i > 0 {
			raw.WriteByte(',')
		}
		raw.WriteString(`{"id":"` + sha + `","message":"` + message + `"}`)
	}
	raw.WriteString(`]}`)
	items := []store.Item{
		{Event: "push", RawJSON: []byte(raw.String())},
		{Event: "push", RawJSON: []byte(raw.String())},
		{Event: "push", RawJSON: []byte(raw.String())},
	}
	batches := split(items, maxMessageBytes)
	if len(batches) < 2 {
		t.Fatalf("expected split push batches, got %d", len(batches))
	}
	for _, batch := range batches {
		if got := messageSize(batch, maxMessageBytes) + 300; got > maxMessageBytes {
			t.Fatalf("push batch size=%d", got)
		}
	}
}

func TestWorkerSplitsAndSendsMaxMindEnrichedAuditMessagesWithinLimit(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	now := time.Now()
	raw := []byte(`{"actor_ip":"8.8.8.8","actor_location":{"country":"US","city":"Seattle"}}`)
	for _, id := range []string{"audit-1", "audit-2"} {
		if _, err := s.Enqueue(id, "audit_log", raw, now, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.Due(now, 10)
	if err != nil {
		t.Fatal(err)
	}
	lookup := GeoLookup(func(string) string { return "US, Washington, Seattle, " + strings.Repeat("x", 900) })
	line := formatForLimit(items[0], 15360, lookup)
	maxBytes := serializedMessageSize([]string{line}) + 300
	batches := splitWithLookup(items, maxBytes, lookup)
	if len(batches) != 2 {
		t.Fatalf("batches=%d want 2", len(batches))
	}
	var sent []lark.Message
	w := NewWorker(s, func(_ context.Context, message lark.Message) error {
		sent = append(sent, message)
		return nil
	}, &metrics.Metrics{}, testLogger(), 0, 10, maxBytes, time.Second, time.Minute, 3)
	w.SetGeoLookup(lookup)
	for _, batch := range batches {
		if result := w.sendBatch(context.Background(), batch); result != continueRound {
			t.Fatalf("send result=%v", result)
		}
	}
	if len(sent) != 2 {
		t.Fatalf("sent=%d want 2", len(sent))
	}
	for _, message := range sent {
		size, err := lark.PostRequestBodySize("", message)
		if err != nil || size+300 > maxBytes {
			t.Fatalf("sent size=%d max=%d err=%v", size+300, maxBytes, err)
		}
		var text strings.Builder
		for _, segment := range message.Content.Post["zh_cn"].Content[0] {
			text.WriteString(segment.Text)
		}
		if !strings.Contains(text.String(), "github_geo US, Seattle") || !strings.Contains(text.String(), "maxmind_geo US, Washington, Seattle") {
			t.Fatalf("missing enriched audit text in %q", text.String())
		}
	}
}

func TestWorkerPersistsRetryAfterCooldown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := store.Open(path, 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := s.Enqueue("delivery-1", "push", []byte(`{}`), now, time.Hour); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 1)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(s, func(context.Context, lark.Message) error {
		return &lark.RetryError{Err: errors.New("rate limited"), RetryAfter: 2 * time.Second}
	}, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	if result := w.sendBatch(context.Background(), items); result != stopRound {
		t.Fatalf("result=%v", result)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(path, 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if wait, err := s.ReserveRequest(time.Now()); err != nil || wait <= 0 {
		t.Fatalf("cooldown not persisted: wait=%v err=%v", wait, err)
	}
}

func TestWorkerMovesPermanentAndExhaustedItemsToDeadLetter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		send     SendFunc
		attempts int
	}{
		{"permanent", func(context.Context, lark.Message) error {
			return &lark.RetryError{Err: errors.New("unauthorized"), Permanent: true}
		}, 3},
		{"exhausted", func(context.Context, lark.Message) error { return errors.New("temporary") }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t)
			defer s.Close()
			now := time.Now()
			if _, err := s.Enqueue("delivery", "push", []byte(`{}`), now, time.Hour); err != nil {
				t.Fatal(err)
			}
			items, _ := s.Due(now, 1)
			w := NewWorker(s, tc.send, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, tc.attempts)
			_ = w.sendBatch(context.Background(), items)
			stats, err := s.Stats()
			if err != nil || stats.Items != 0 || stats.DeadLetters != 1 {
				t.Fatalf("stats=%+v err=%v", stats, err)
			}
		})
	}
}

func TestWorkerRejectsConcurrentRun(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	if _, err := s.Enqueue("delivery", "push", []byte(`{}`), time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	w := NewWorker(s, func(context.Context, lark.Message) error { sends.Add(1); close(started); <-release; return nil }, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first worker did not send")
	}
	w.Run(ctx)
	close(release)
	time.Sleep(50 * time.Millisecond)
	if sends.Load() != 1 {
		t.Fatalf("concurrent runs sent %d times", sends.Load())
	}
}

func TestUnknownEventFallsBack(t *testing.T) {
	line := Format(store.Item{Event: "future_event", DeliveryID: "abcdef0123456789", RawJSON: []byte(`{"action":"created","repository":{"full_name":"acme/repo","html_url":"https://github.com/acme/repo"},"sender":{"login":"octo"}}`)})
	for _, text := range []string{"[future_event:created]", "acme/repo", "@octo"} {
		if !strings.Contains(line, text) {
			t.Fatalf("fallback missing %q in %q", text, line)
		}
	}
	if strings.Contains(line, "delivery=") {
		t.Fatalf("delivery marker shown in %q", line)
	}
	if strings.Contains(line, "action=created") {
		t.Fatalf("action was not folded into event label: %q", line)
	}
}

func TestMakeMessageLinksRepositoryAndShortensGitHubURLs(t *testing.T) {
	message := MakeMessage([]string{
		"[workflow_job:queued] acme/repo | @octo | https://github.com/acme/repo",
		"[workflow_run:completed] acme/repo | workflow CI success | https://github.com/acme/repo/actions/runs/123",
	})
	locale := message.Content.Post["zh_cn"]
	if locale.Title != "" {
		t.Fatalf("unexpected message title %q", locale.Title)
	}
	content := locale.Content
	if len(content) != 2 {
		t.Fatalf("content=%v", content)
	}
	for i, paragraph := range content {
		if len(paragraph) < 2 || paragraph[1].Tag != "a" || paragraph[1].Text != "acme/repo" || paragraph[1].Href != "https://github.com/acme/repo" {
			t.Fatalf("paragraph %d repo link=%+v", i, paragraph)
		}
		for _, segment := range paragraph {
			if strings.Contains(segment.Text, "github.com/") {
				t.Fatalf("paragraph %d exposes github prefix in %+v", i, paragraph)
			}
		}
	}
	for _, segment := range content[0] {
		if segment.Href == "https://github.com/acme/repo" && segment.Text != "acme/repo" {
			t.Fatalf("repository URL shown separately: %+v", content[0])
		}
	}
	last := content[1][len(content[1])-1]
	if last.Tag != "a" || last.Text != "acme/repo/actions/runs/123" || last.Href != "https://github.com/acme/repo/actions/runs/123" {
		t.Fatalf("shortened workflow URL=%+v", last)
	}
}

func TestPushRepositoryLinkTargetsBranch(t *testing.T) {
	message := MakeMessage([]string{"[push] acme/repo/feature/clean | 1 commit"})
	paragraph := message.Content.Post["zh_cn"].Content[0]
	if len(paragraph) < 2 || paragraph[1].Tag != "a" || paragraph[1].Text != "acme/repo/feature/clean" || paragraph[1].Href != "https://github.com/acme/repo/tree/feature/clean" {
		t.Fatalf("push branch link=%+v", paragraph)
	}
}

func TestFormatLabelIncludesUsefulDetails(t *testing.T) {
	line := Format(store.Item{Event: "label", RawJSON: []byte(`{
		"action":"created",
		"repository":{"full_name":"acme/repo","html_url":"https://github.com/acme/repo"},
		"label":{"name":"bug","description":"Blocks production deployments","color":"d73a4a"}
	}`)})
	for _, text := range []string{"[label:created]", "acme/repo", "label bug", "Blocks production deployments", "color #d73a4a"} {
		if !strings.Contains(line, text) {
			t.Fatalf("label detail %q missing from %q", text, line)
		}
	}

	line = Format(store.Item{Event: "label", RawJSON: []byte(`{"label":{"name":"bug"}}`)})
	if strings.Contains(line, "description") || strings.Contains(line, "color #") {
		t.Fatalf("empty label fields shown in %q", line)
	}
}

func TestRichTextLinksOnlyHTTPSURLs(t *testing.T) {
	for _, tc := range []struct {
		url      string
		wantLink bool
	}{
		{url: "https://ci.example.com/build/123", wantLink: true},
		{url: "http://ci.example.com/build/123"},
		{url: "mailto:ci@example.com"},
	} {
		t.Run(tc.url, func(t *testing.T) {
			segments := richText("[check_run:created] acme/repo | " + tc.url)
			for _, segment := range segments {
				if segment.Text != tc.url {
					continue
				}
				if tc.wantLink && (segment.Tag != "a" || segment.Href != tc.url) {
					t.Fatalf("HTTPS URL was not linked: %+v", segment)
				}
				if !tc.wantLink && (segment.Tag != "text" || segment.Href != "") {
					t.Fatalf("non-HTTPS URL was linked: %+v", segment)
				}
				return
			}
			t.Fatalf("URL segment missing for %q", tc.url)
		})
	}
}

func TestFormatCheckEventsIncludeDetailsWithoutSender(t *testing.T) {
	for _, tc := range []struct {
		name        string
		event       string
		raw         string
		contains    []string
		notContains []string
		url         string
	}{
		{
			name:  "check run completed",
			event: "check_run",
			raw: `{
				"action":"completed",
				"repository":{"full_name":"acme/repo"},
				"sender":{"login":"commit-author"},
				"check_run":{"name":"unit tests","status":"completed","conclusion":"success","details_url":"https://ci.example.com/build/123","app":{"slug":"github-actions"}}
			}`,
			contains:    []string{"[check_run:completed]", "check unit tests", "success", "via github-actions"},
			notContains: []string{" | completed |"},
			url:         "https://ci.example.com/build/123",
		},
		{
			name:  "check run created queued",
			event: "check_run",
			raw: `{
				"action":"created",
				"repository":{"full_name":"acme/repo"},
				"sender":{"login":"commit-author"},
				"check_run":{"name":"lint","status":"queued","app":{"slug":"buildkite"}}
			}`,
			contains: []string{"[check_run:created]", "check lint", "queued", "via buildkite"},
		},
		{
			name:  "check run empty fields",
			event: "check_run",
			raw: `{
				"action":"created",
				"repository":{"full_name":"acme/repo"},
				"sender":{"login":"commit-author"},
				"check_run":{}
			}`,
			contains:    []string{"[check_run:created]"},
			notContains: []string{" | ", "check ", "via ", "@commit-author"},
		},
		{
			name:  "check suite completed",
			event: "check_suite",
			raw: `{
				"action":"completed",
				"repository":{"full_name":"acme/repo"},
				"sender":{"login":"commit-author"},
				"check_suite":{"head_branch":"main","status":"completed","conclusion":"failure","app":{"slug":"github-actions"}}
			}`,
			contains:    []string{"[check_suite:completed]", "branch main", "failure", "via github-actions"},
			notContains: []string{" | completed |"},
		},
		{
			name:  "check suite created queued",
			event: "check_suite",
			raw: `{
				"action":"created",
				"repository":{"full_name":"acme/repo"},
				"sender":{"login":"commit-author"},
				"check_suite":{"head_branch":"feature/checks","status":"queued","app":{"slug":"circleci-checks"}}
			}`,
			contains: []string{"[check_suite:created]", "branch feature/checks", "queued", "via circleci-checks"},
		},
		{
			name:  "check suite empty fields",
			event: "check_suite",
			raw: `{
				"action":"created",
				"repository":{"full_name":"acme/repo"},
				"sender":{"login":"commit-author"},
				"check_suite":{}
			}`,
			contains:    []string{"[check_suite:created]"},
			notContains: []string{" | ", "branch ", "via ", "@commit-author"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := Format(store.Item{Event: tc.event, RawJSON: []byte(tc.raw)})
			for _, text := range tc.contains {
				if !strings.Contains(line, text) {
					t.Fatalf("missing %q in %q", text, line)
				}
			}
			for _, text := range tc.notContains {
				if strings.Contains(line, text) {
					t.Fatalf("unexpected %q in %q", text, line)
				}
			}
			if strings.Contains(line, "@commit-author") {
				t.Fatalf("sender shown in %q", line)
			}
			if tc.url != "" {
				found := false
				for _, segment := range MakeMessage([]string{line}).Content.Post["zh_cn"].Content[0] {
					if segment.Tag == "a" && segment.Text == tc.url && segment.Href == tc.url {
						found = true
					}
				}
				if !found {
					t.Fatalf("details URL link missing from %q", line)
				}
			}
		})
	}
}

func TestMakeMessageLinksPullRequestTitle(t *testing.T) {
	line := Format(store.Item{Event: "pull_request", RawJSON: []byte(`{
		"repository":{"full_name":"acme/repo","html_url":"https://github.com/acme/repo"},
		"pull_request":{"number":42,"title":"Improve release flow","html_url":"https://github.com/acme/repo/pull/42"}
	}`)})
	paragraph := MakeMessage([]string{line}).Content.Post["zh_cn"].Content[0]
	found := false
	for _, segment := range paragraph {
		if segment.Tag == "a" && segment.Text == "PR #42 Improve release flow" && segment.Href == "https://github.com/acme/repo/pull/42" {
			found = true
		}
		if strings.Contains(segment.Text, "acme/repo/pull/42") {
			t.Fatalf("pull request URL shown separately in %+v", paragraph)
		}
	}
	if !found {
		t.Fatalf("pull request title link missing from %+v", paragraph)
	}
}

func TestFormatForLimitKeepsMessageWithinConfiguredSize(t *testing.T) {
	raw := []byte(`{"pull_request":{"title":"` + strings.Repeat("\\u0000", 1000) + `"}}`)
	line := FormatForLimit(store.Item{Event: "pull_request", DeliveryID: "delivery-123456789", RawJSON: raw}, 512)
	if strings.Contains(line, "delivery=") {
		t.Fatalf("delivery marker shown in %q", line)
	}
	if got := serializedMessageSize([]string{line}) + 300; got > 512 {
		t.Fatalf("serialized message plus signature reserve is %d bytes", got)
	}
}

func TestFormatPushIncludesSanitizedDetails(t *testing.T) {
	line := Format(store.Item{Event: "push", DeliveryID: "delivery-123456789", RawJSON: []byte(`{
		"repository":{"full_name":"acme/repo","html_url":"https://github.com/acme/repo"},
		"sender":{"login":"octo"},
		"ref":"refs/heads/main",
		"before":"1234567890abcdef",
		"after":"abcdef0123456789",
		"size":2,
		"compare":"https://github.com/acme/repo/compare/123...abc",
		"head_commit":{"id":"abcdef0123456789","message":"first line\nsecond | separator\u0000"}
	}`)})
	for _, text := range []string{"[push] acme/repo/main", "1234567..abcdef0", "2 commits", "first line second / separator", "@octo", "https://github.com/acme/repo/compare/123...abc"} {
		if !strings.Contains(line, text) {
			t.Fatalf("missing %q in %q", text, line)
		}
	}
	if strings.Contains(line, " | branch main") || strings.Contains(line, "delivery=") || strings.Contains(line, "\n") || strings.Contains(line, "\x00") || strings.Contains(line, "second | separator") {
		t.Fatalf("unsafe push formatting %q", line)
	}
}

func TestFormatPushListsPayloadCommitsInOrderWithDerivedLinks(t *testing.T) {
	commits := []struct {
		id      string
		message string
	}{
		{"1111111111111111111111111111111111111111", "first\nline | separator\x00"},
		{"2222222222222222222222222222222222222222", "https://evil.example/commit"},
		{"3333333333333333333333333333333333333333", "third commit"},
	}
	item := store.Item{Event: "push", RawJSON: []byte(`{
		"repository":{"full_name":"acme/repo"},
		"sender":{"login":"octo"},
		"ref":"refs/heads/main",
		"before":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"after":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"size":3,
		"compare":"https://github.com/acme/repo/compare/a...b",
		"head_commit":{"id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","message":"head commit must not be repeated"},
		"commits":[
			{"id":"1111111111111111111111111111111111111111","message":"first line\nsecond | separator\u0000"},
			{"id":"2222222222222222222222222222222222222222","message":"https://evil.example/commit"},
			{"id":"3333333333333333333333333333333333333333","message":"third commit"}
		]
	}`)}
	line := Format(item)
	if strings.Contains(line, "head commit must not be repeated") {
		t.Fatalf("head commit was repeated in %q", line)
	}
	if !strings.Contains(line, "first line second / separator") || strings.Contains(line, "second | separator") || strings.Contains(line, "\x00") {
		t.Fatalf("commit message was not sanitized in %q", line)
	}

	content := MakeMessage([]string{line}).Content.Post["zh_cn"].Content
	if len(content) != len(commits)+1 {
		t.Fatalf("paragraphs=%d want=%d", len(content), len(commits)+1)
	}
	for i, commit := range commits {
		paragraph := content[i+1]
		found := false
		var text strings.Builder
		for _, segment := range paragraph {
			text.WriteString(segment.Text)
			if segment.Tag == "a" && segment.Text == commit.id[:7] && segment.Href == "https://github.com/acme/repo/commit/"+commit.id {
				found = true
			}
			if segment.Href == "https://evil.example/commit" {
				t.Fatalf("commit message URL was linked: %+v", paragraph)
			}
		}
		if !found {
			t.Fatalf("commit %d link missing from %+v", i, paragraph)
		}
		if !strings.Contains(text.String(), strings.ReplaceAll(commit.message, "\n", " ")) && i > 0 {
			t.Fatalf("commit %d text missing from %q", i, text.String())
		}
	}
}

func TestFormatTagPushLinksCommitToRepository(t *testing.T) {
	const sha = "abcdef0123456789abcdef0123456789abcdef01"
	line := Format(store.Item{Event: "push", RawJSON: []byte(`{
		"repository":{"full_name":"acme/repo"},
		"ref":"refs/tags/v1.0",
		"commits":[{"id":"abcdef0123456789abcdef0123456789abcdef01","message":"release v1.0"}]
	}`)})
	content := MakeMessage([]string{line}).Content.Post["zh_cn"].Content
	if len(content) != 2 {
		t.Fatalf("paragraphs=%d", len(content))
	}
	for _, segment := range content[1] {
		if segment.Tag == "a" && segment.Text == sha[:7] && segment.Href == "https://github.com/acme/repo/commit/"+sha {
			return
		}
	}
	t.Fatalf("tag push commit link missing from %+v", content[1])
}

func TestFormatPushDoesNotHardTruncateQuoteHeavyCommitList(t *testing.T) {
	const (
		sha             = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		maxMessageBytes = 15360
	)
	message := strings.Repeat(`\"\\`, 120)
	var raw strings.Builder
	raw.WriteString(`{"repository":{"full_name":"acme/repo"},"commits":[`)
	for i := 0; i < 20; i++ {
		if i > 0 {
			raw.WriteByte(',')
		}
		raw.WriteString(`{"id":"` + sha + `","message":"` + message + `"}`)
	}
	raw.WriteString(`]}`)
	item := store.Item{Event: "push", RawJSON: []byte(raw.String())}
	line := Format(item)
	if len(line) <= 1800 || strings.Count(line, "commit:"+sha) != 20 {
		t.Fatalf("commit list was hard-truncated: bytes=%d commits=%d", len(line), strings.Count(line, "commit:"+sha))
	}
	limited := FormatForLimit(item, maxMessageBytes)
	if got := serializedMessageSize([]string{limited}) + 300; got > maxMessageBytes {
		t.Fatalf("limited push size=%d", got)
	}
	if got, err := lark.PostRequestBodySize("", MakeMessage([]string{limited})); err != nil || got+300 > maxMessageBytes {
		t.Fatalf("actual post request size=%d err=%v", got, err)
	}
}

func TestFormatPushHeadMessageFallbackAndConfiguredSizeAreBounded(t *testing.T) {
	item := store.Item{Event: "push", DeliveryID: "delivery-123456789", RawJSON: []byte(`{"ref":"refs/heads/main","head_commit":{"message":"fallback ` + strings.Repeat("x", 1000) + `"}}`)}
	line := Format(item)
	if !strings.Contains(line, "fallback") {
		t.Fatalf("head commit fallback missing from %q", line)
	}
	if strings.Contains(line, strings.Repeat("x", maxHeadMessageBytes+1)) {
		t.Fatalf("head message was not bounded: %q", line)
	}
	limited := FormatForLimit(item, 512)
	if strings.Contains(limited, "delivery=") || serializedMessageSize([]string{limited})+300 > 512 {
		t.Fatalf("limited push=%q size=%d", limited, serializedMessageSize([]string{limited})+300)
	}
}

func TestFormatPushShowsZeroCommitCount(t *testing.T) {
	line := Format(store.Item{Event: "push", DeliveryID: "delivery", RawJSON: []byte(`{"size":0}`)})
	if !strings.Contains(line, "0 commits") {
		t.Fatalf("zero commit count missing from %q", line)
	}
}

func TestWorkerCountsDeliveredItemsAfterSuccessfulDelete(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	now := time.Now()
	for _, id := range []string{"push-delivery", "workflow-delivery"} {
		event := "push"
		if id == "workflow-delivery" {
			event = "workflow_job"
		}
		if _, err := s.Enqueue(id, event, []byte(`{}`), now, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.Due(now, 10)
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	w := NewWorker(s, func(context.Context, lark.Message) error { return nil }, m, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	if result := w.sendBatch(context.Background(), items); result != continueRound {
		t.Fatalf("result=%v", result)
	}
	response := httptest.NewRecorder()
	m.Handler(func() metrics.Snapshot { return metrics.Snapshot{} }).ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	for _, line := range []string{`lark_git_webhook_lark_requests_success_total 1`, `lark_git_webhook_events_delivered_total{event="push"} 1`, `lark_git_webhook_events_delivered_total{event="workflow_job"} 1`} {
		if !strings.Contains(response.Body.String(), line) {
			t.Fatalf("missing %q in %s", line, response.Body.String())
		}
	}
}

type targetClient struct {
	chatIDs        []string
	failSendOnce   map[string]bool
	failCreateOnce map[string]bool
}

func (c *targetClient) SendToChat(_ context.Context, chatID string, _ lark.Message) error {
	c.chatIDs = append(c.chatIDs, chatID)
	if c.failSendOnce[chatID] {
		delete(c.failSendOnce, chatID)
		return errors.New("temporary send failure")
	}
	return nil
}
func (c *targetClient) CreateInteractive(context.Context, any, string) (string, error) {
	return "", errors.New("unexpected workflow create")
}
func (c *targetClient) CreateInteractiveToChat(_ context.Context, chatID string, _ any, _ string) (string, error) {
	c.chatIDs = append(c.chatIDs, chatID)
	if c.failCreateOnce[chatID] {
		delete(c.failCreateOnce, chatID)
		return "", errors.New("temporary create failure")
	}
	return "om_" + chatID, nil
}
func (c *targetClient) CreateInteractiveToOpenID(context.Context, string, any, string) (string, error) {
	return "", errors.New("unexpected onboarding create")
}
func (c *targetClient) UpdateInteractive(context.Context, string, any) error { return nil }
func (c *targetClient) AddReaction(context.Context, string, string) (string, error) {
	return "", nil
}
func (c *targetClient) DeleteReaction(context.Context, string, string) error { return nil }

func TestWorkerSendsOrdinaryEventsToEachSnapshottedChat(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	now := time.Now()
	if _, err := s.AdmitToChats("delivery", "push", []byte(`{"repository":{"full_name":"acme/repo"}}`), []string{"oc_default", "oc_subscribed"}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive("delivery"); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	client := &targetClient{}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Second, time.Minute, 3)
	w.SetLarkClient(client)
	if result := w.sendOrdinary(context.Background(), items); result != continueRound {
		t.Fatalf("result=%v", result)
	}
	if got := strings.Join(client.chatIDs, ","); got != "oc_default,oc_subscribed" {
		t.Fatalf("chat IDs=%s", got)
	}
}

func TestWorkerRetriesOnlyFailedOrdinaryChatTarget(t *testing.T) {
	s := openStore(t)
	defer s.Close()
	now := time.Now()
	if _, err := s.AdmitToChats("delivery", "push", []byte(`{"repository":{"full_name":"acme/repo"}}`), []string{"oc_default", "oc_subscribed"}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.FinalizeArchive("delivery"); err != nil {
		t.Fatal(err)
	}
	client := &targetClient{failSendOnce: map[string]bool{"oc_subscribed": true}}
	w := NewWorker(s, nil, &metrics.Metrics{}, testLogger(), 0, 10, 15360, time.Millisecond, time.Second, 3)
	w.SetLarkClient(client)
	items, err := s.Due(now, 1)
	if err != nil || len(items) != 1 || w.sendOrdinary(context.Background(), items) != continueRound {
		t.Fatalf("initial items=%+v err=%v", items, err)
	}
	items, err = s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(items) != 1 || strings.Join(items[0].ChatIDs, ",") != "oc_subscribed" {
		t.Fatalf("retry items=%+v err=%v", items, err)
	}
	if w.sendOrdinary(context.Background(), items) != continueRound {
		t.Fatal("retry result")
	}
	if got := strings.Join(client.chatIDs, ","); got != "oc_default,oc_subscribed,oc_subscribed" {
		t.Fatalf("chat IDs=%s", got)
	}
	items, err = s.Due(time.Now().Add(time.Second), 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("remaining items=%+v err=%v", items, err)
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 100, 1<<20, 100, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
