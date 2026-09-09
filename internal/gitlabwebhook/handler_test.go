package gitlabwebhook

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

func openFixture(t *testing.T) (*store.Store, *archive.Archive, string) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	root := filepath.Join(t.TempDir(), "archive")
	a, err := archive.Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	return s, a, root
}

func gitlabRequest(method, path string, body []byte, headers map[string]string) *http.Request {
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	return r
}

func TestEventLabelMapping(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   string
		err    bool
	}{
		{header: "Push Hook", want: "gitlab:push"},
		{header: "Tag Push Hook", want: "gitlab:tag_push"},
		{header: "Pipeline Hook", want: "gitlab:pipeline"},
		{header: "Merge Request Hook", want: "gitlab:merge_request"},
		{header: "push hook", want: "gitlab:push"},
		{header: "  Tag   Push Hook  ", want: "gitlab:tag_push"},
		{header: "Note Hook", want: "gitlab:note_hook"},
		{header: "Wiki Page Hook", want: "gitlab:wiki_page_hook"},
		{header: "Release", want: "gitlab:release"},
		{header: "", err: true},
		{header: "Push/Hook", err: true},
		{header: "Push\tHook", err: true},
		{header: strings.Repeat("a", 65), err: true},
	} {
		got, err := eventLabel(tc.header)
		if tc.err && err == nil {
			t.Fatalf("eventLabel(%q) accepted", tc.header)
		}
		if !tc.err && (err != nil || got != tc.want) {
			t.Fatalf("eventLabel(%q)=%q err=%v want=%q", tc.header, got, err, tc.want)
		}
	}
}

func TestSystemHookEventMapping(t *testing.T) {
	fallback := "gitlab:system_hook"
	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{name: "push object kind", body: []byte(`{"object_kind":"push","event_name":"push"}`), want: "gitlab:push"},
		{name: "tag push object kind", body: []byte(`{"object_kind":"tag_push","event_name":"tag_push"}`), want: "gitlab:tag_push"},
		{name: "pipeline object kind", body: []byte(`{"object_kind":"pipeline"}`), want: "gitlab:pipeline"},
		{name: "merge request object kind", body: []byte(`{"object_kind":"merge_request"}`), want: "gitlab:merge_request"},
		{name: "repository update event name only", body: []byte(`{"event_name":"repository_update"}`), want: fallback},
		{name: "unknown kind", body: []byte(`{"object_kind":"user_create"}`), want: fallback},
		{name: "empty body", body: []byte(``), want: fallback},
		{name: "invalid json", body: []byte(`{`), want: fallback},
		{name: "non object json", body: []byte(`[]`), want: fallback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := systemHookEvent(tc.body, fallback); got != tc.want {
				t.Fatalf("systemHookEvent(%s)=%q want=%q", tc.body, got, tc.want)
			}
		})
	}
}

func TestSystemHookDeliveriesClassifiedByBody(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		wantEvent  string
		wantQueued int
	}{
		{name: "push system hook relayed", body: []byte(`{"object_kind":"push","event_name":"push","project":{"path_with_namespace":"acme/repo"}}`), wantEvent: "gitlab:push", wantQueued: 1},
		{name: "tag push system hook relayed", body: []byte(`{"object_kind":"tag_push","event_name":"tag_push","project":{"path_with_namespace":"acme/repo"}}`), wantEvent: "gitlab:tag_push", wantQueued: 1},
		{name: "merge request system hook relayed", body: []byte(`{"object_kind":"merge_request","object_attributes":{"action":"open"},"project":{"path_with_namespace":"acme/repo"}}`), wantEvent: "gitlab:merge_request", wantQueued: 1},
		{name: "repository update archived only", body: []byte(`{"event_name":"repository_update","project":{"path_with_namespace":"acme/repo"}}`), wantEvent: "gitlab:system_hook", wantQueued: 0},
		{name: "user create archived only", body: []byte(`{"event_name":"user_create"}`), wantEvent: "gitlab:system_hook", wantQueued: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a, archiveRoot := openFixture(t)
			h := New(s, a, "", "oc_default", 1<<20, time.Hour, 8, &metrics.Metrics{})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", tc.body, map[string]string{"X-Gitlab-Event": "System Hook", "X-Gitlab-Event-UUID": "syshook-1111-2222-4333-8444-555555555555"}))
			if w.Code != http.StatusAccepted {
				t.Fatalf("status=%d", w.Code)
			}
			items, err := s.Due(time.Now(), 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != tc.wantQueued {
				t.Fatalf("queued=%d want=%d items=%+v", len(items), tc.wantQueued, items)
			}
			if tc.wantQueued == 1 && items[0].Event != tc.wantEvent {
				t.Fatalf("event=%q want=%q", items[0].Event, tc.wantEvent)
			}
			if archived := archivedPayloadFile(t, archiveRoot, tc.body); archived == "" {
				t.Fatal("delivery was not archived")
			}
		})
	}
}

func TestHandlerValidatesTokenSemantics(t *testing.T) {
	body := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"}}`)
	for _, tc := range []struct {
		name       string
		secret     string
		headers    map[string]string
		wantStatus int
		queueItems int
	}{
		{name: "configured secret matched", secret: "s3cret", headers: map[string]string{"X-Gitlab-Token": "s3cret"}, wantStatus: http.StatusAccepted, queueItems: 1},
		{name: "configured secret mismatched", secret: "s3cret", headers: map[string]string{"X-Gitlab-Token": "wrong"}, wantStatus: http.StatusUnauthorized},
		{name: "configured secret missing", secret: "s3cret", headers: map[string]string{}, wantStatus: http.StatusUnauthorized},
		{name: "no secret accepts without token", secret: "", headers: map[string]string{}, wantStatus: http.StatusAccepted, queueItems: 1},
		{name: "no secret accepts any token", secret: "", headers: map[string]string{"X-Gitlab-Token": "whatever"}, wantStatus: http.StatusAccepted, queueItems: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a, _ := openFixture(t)
			m := &metrics.Metrics{}
			h := New(s, a, tc.secret, "oc_default", 1<<20, time.Hour, 8, m)
			headers := map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Event-UUID": "11111111-2222-4333-8444-555555555555"}
			for name, value := range tc.headers {
				headers[name] = value
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, headers))
			if w.Code != tc.wantStatus {
				t.Fatalf("status=%d want=%d", w.Code, tc.wantStatus)
			}
			items, err := s.Due(time.Now(), 10)
			if err != nil || len(items) != tc.queueItems {
				t.Fatalf("items=%+v err=%v", items, err)
			}
		})
	}
}

func TestHandlerRejectsBadRequests(t *testing.T) {
	s, a, _ := openFixture(t)
	h := New(s, a, "s3cret", "oc_default", 100, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{"object_kind":"push"}`)
	for _, tc := range []struct {
		name       string
		method     string
		body       []byte
		headers    map[string]string
		wantStatus int
	}{
		{name: "get method", method: http.MethodGet, body: body, headers: map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": "s3cret"}, wantStatus: http.StatusMethodNotAllowed},
		{name: "missing event", method: http.MethodPost, body: body, headers: map[string]string{"X-Gitlab-Token": "s3cret"}, wantStatus: http.StatusBadRequest},
		{name: "invalid event", method: http.MethodPost, body: body, headers: map[string]string{"X-Gitlab-Event": "Push/Hook", "X-Gitlab-Token": "s3cret"}, wantStatus: http.StatusBadRequest},
		{name: "invalid JSON", method: http.MethodPost, body: []byte(`{`), headers: map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": "s3cret"}, wantStatus: http.StatusBadRequest},
		{name: "oversized body", method: http.MethodPost, body: bytes.Repeat([]byte("x"), 101), headers: map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": "s3cret"}, wantStatus: http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, gitlabRequest(tc.method, "/gitlab/webhook", tc.body, tc.headers))
			if w.Code != tc.wantStatus {
				t.Fatalf("status=%d want=%d", w.Code, tc.wantStatus)
			}
		})
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("rejected requests queued items=%+v err=%v", items, err)
	}
}

func TestHandlerAdmitsUnknownEventsArchiveOnly(t *testing.T) {
	s, a, archiveRoot := openFixture(t)
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{"object_kind":"note","project":{"path_with_namespace":"acme/repo"}}`)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, map[string]string{"X-Gitlab-Event": "Note Hook", "X-Gitlab-Event-UUID": "note-1111-2222-3333-4444"}))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d", w.Code)
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("archive-only event queued: %+v err=%v", items, err)
	}
	archived := archivedPayloadFile(t, archiveRoot, body)
	if archived == "" {
		t.Fatal("archive-only event was not archived")
	}
}

func TestHandlerSnapshotsDefaultAndSubscribedChats(t *testing.T) {
	s, a, _ := openFixture(t)
	if err := s.AddRepositorySubscription("oc_subscribed", "acme/repo"); err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 1, &metrics.Metrics{})
	body := []byte(`{"object_kind":"push","project":{"id":15,"path_with_namespace":"Acme/Repo","web_url":"https://gitlab.example.com/acme/repo"}}`)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Event-UUID": "push-uuid-1111"}))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d", w.Code)
	}
	items, err := s.Due(time.Now(), 1)
	if err != nil || len(items) != 1 || strings.Join(items[0].ChatIDs, ",") != "oc_default,oc_subscribed" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if items[0].Event != "gitlab:push" {
		t.Fatalf("event=%q", items[0].Event)
	}
}

func TestHandlerPersistsOnceAndDedupesIdenticalUUID(t *testing.T) {
	s, a, _ := openFixture(t)
	m := &metrics.Metrics{}
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 8, m)
	body := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"}}`)
	headers := map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Event-UUID": "13792a34-cac6-4fda-95a8-c58e00a3954e"}
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, headers))
		if w.Code != http.StatusAccepted {
			t.Fatalf("request %d status=%d", i, w.Code)
		}
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	response := httptest.NewRecorder()
	m.Handler(func() metrics.Snapshot { return metrics.Snapshot{} }).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range []string{
		"lark_git_webhook_gitlab_webhooks_received_total 1",
		"lark_git_webhook_gitlab_webhooks_duplicates_total 1",
		`lark_git_webhook_gitlab_events_received_total{event="push"} 1`,
	} {
		if !strings.Contains(response.Body.String(), line) {
			t.Fatalf("missing %q in %s", line, response.Body.String())
		}
	}
}

func TestHandlerDedupesWithoutUUIDByBodyHash(t *testing.T) {
	s, a, archiveRoot := openFixture(t)
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"},"before":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","after":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`)
	headers := map[string]string{"X-Gitlab-Event": "Push Hook"}
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, headers))
		if w.Code != http.StatusAccepted {
			t.Fatalf("request %d status=%d", i, w.Code)
		}
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if len(items[0].DeliveryID) != 64 {
		t.Fatalf("derived delivery id=%q", items[0].DeliveryID)
	}
	archived := archivedPayloadFile(t, archiveRoot, body)
	if archived == "" {
		t.Fatal("delivery was not archived")
	}
}

func TestHandlerRejectsConflictingDeliveryIdentity(t *testing.T) {
	s, a, _ := openFixture(t)
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 8, &metrics.Metrics{})
	headers := map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Event-UUID": "fixed-uuid-1234"}
	first := httptest.NewRecorder()
	h.ServeHTTP(first, gitlabRequest(http.MethodPost, "/gitlab/webhook", []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"}}`), headers))
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d", first.Code)
	}
	conflicting := httptest.NewRecorder()
	h.ServeHTTP(conflicting, gitlabRequest(http.MethodPost, "/gitlab/webhook", []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/other"}}`), headers))
	if conflicting.Code != http.StatusConflict {
		t.Fatalf("conflicting body status=%d", conflicting.Code)
	}
	differentEvent := httptest.NewRecorder()
	h.ServeHTTP(differentEvent, gitlabRequest(http.MethodPost, "/gitlab/webhook", []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"}}`), map[string]string{"X-Gitlab-Event": "Pipeline Hook", "X-Gitlab-Event-UUID": "fixed-uuid-1234"}))
	if differentEvent.Code != http.StatusConflict {
		t.Fatalf("conflicting event status=%d", differentEvent.Code)
	}
}

func TestHandlerArchivesOnlyValidatedPayloads(t *testing.T) {
	s, a, archiveRoot := openFixture(t)
	h := New(s, a, "s3cret", "oc_default", 1<<20, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"}}`)
	invalid := httptest.NewRecorder()
	h.ServeHTTP(invalid, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Event-UUID": "ab-delivery", "X-Gitlab-Token": "wrong"}))
	if invalid.Code != http.StatusUnauthorized {
		t.Fatalf("invalid status=%d", invalid.Code)
	}
	if entries, _ := os.ReadDir(archiveRoot); len(entries) != 0 {
		t.Fatalf("invalid request created archive entries %v", entries)
	}
}

func TestArchivedRedeliveryBypassesReservationOnlyWhenPayloadExists(t *testing.T) {
	s, a, archiveRoot := openFixture(t)
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 1, &metrics.Metrics{})
	body := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"},"id":1}`)
	headers := map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Event-UUID": "delivery-uuid-1"}
	request := func() int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, headers))
		return w.Code
	}
	if status := request(); status != http.StatusAccepted {
		t.Fatalf("initial status=%d", status)
	}
	lowSpaceArchive, err := archive.Open(archiveRoot, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	h = New(s, lowSpaceArchive, "", "oc_default", 1<<20, time.Hour, 1, &metrics.Metrics{})
	if status := request(); status != http.StatusAccepted {
		t.Fatalf("matching redelivery status=%d", status)
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

// blockingReader holds one handler slot until released.
type blockingReader struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r blockingReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.release
	return 0, io.EOF
}

func TestHandlerBlocksWhileSlotHeld(t *testing.T) {
	s, a, _ := openFixture(t)
	h := New(s, a, "", "oc_default", 1<<20, time.Hour, 1, &metrics.Metrics{})
	release := make(chan struct{})
	started := make(chan struct{})
	firstDone := make(chan struct{})
	req := httptest.NewRequest(http.MethodPost, "/gitlab/webhook", blockingReader{started: started, release: release})
	go func() {
		defer close(firstDone)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire its slot")
	}
	full := httptest.NewRecorder()
	h.ServeHTTP(full, gitlabRequest(http.MethodPost, "/gitlab/webhook", []byte(`{}`), map[string]string{"X-Gitlab-Event": "Push Hook"}))
	if full.Code != http.StatusServiceUnavailable {
		t.Fatalf("full status=%d", full.Code)
	}
	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not release its slot")
	}
	body := []byte(`{"object_kind":"push","project":{"path_with_namespace":"acme/repo"}}`)
	recovered := httptest.NewRecorder()
	h.ServeHTTP(recovered, gitlabRequest(http.MethodPost, "/gitlab/webhook", body, map[string]string{"X-Gitlab-Event": "Push Hook"}))
	if recovered.Code != http.StatusAccepted {
		t.Fatalf("released status=%d", recovered.Code)
	}
}

// archivedPayloadFile finds the payload.<event>.json file under the archive
// root whose content matches body.
func archivedPayloadFile(t *testing.T, root string, body []byte) string {
	t.Helper()
	var found string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		if raw, err := os.ReadFile(path); err == nil && bytes.Equal(raw, body) {
			found = path
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}
