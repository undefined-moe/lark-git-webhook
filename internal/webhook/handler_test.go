package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

func TestGitHubHMACVector(t *testing.T) {
	body := []byte("Hello, World!")
	const secret = "It's a Secret to Everybody"
	const signature = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
	if !validSignature(signature, body, []byte(secret)) {
		t.Fatal("fixed HMAC vector did not verify")
	}
	if validSignature(signature, []byte("Hello, world!"), []byte(secret)) {
		t.Fatal("changed body verified")
	}
	if validSignature(signature, body, []byte("other")) {
		t.Fatal("changed secret verified")
	}
	if validSignature("sha256=00", body, []byte(secret)) {
		t.Fatal("short signature verified")
	}
}

func TestHandlerPersistsOnce(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 2, 1024, 10, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 100, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{"repository":{"full_name":"acme/repo"}}`)
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		r.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
		r.Header.Set("X-GitHub-Delivery", "delivery-1")
		r.Header.Set("X-GitHub-Event", "push")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatalf("request %d status=%d", i, w.Code)
		}
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%d err=%v", len(items), err)
	}
	bad := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	bad.Header.Set("X-Hub-Signature-256", "sha256=00")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, bad)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d", w.Code)
	}
}

func TestGitHubHeaderLimits(t *testing.T) {
	if !validDeliveryID("abc-123_456.789") || validDeliveryID(strings.Repeat("a", 129)) || validDeliveryID("bad/value") {
		t.Fatal("delivery validation failed")
	}
	if !validEvent("pull_request-1") || validEvent(strings.Repeat("a", 65)) || validEvent("bad.event") {
		t.Fatal("event validation failed")
	}
}

func TestHandlerArchivesWorkflowJobCheckSuiteAndStatusWithoutQueueing(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 8, &metrics.Metrics{})
	for _, event := range []string{"workflow_job", "check_suite", "status"} {
		body := []byte(`{"event":"` + event + `"}`)
		r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		r.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
		r.Header.Set("X-GitHub-Delivery", "delivery-"+event)
		r.Header.Set("X-GitHub-Event", event)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatalf("%s status=%d", event, w.Code)
		}
		archived, err := a.VerifyExisting(event, "delivery-"+event, body)
		if err != nil || !archived {
			t.Fatalf("%s archived=%v err=%v", event, archived, err)
		}
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestHandlerArchivesOnlyValidatedPayloads(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1024, 10, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	root := filepath.Join(t.TempDir(), "archive")
	a, err := archive.Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{"ok":true}`)
	invalid := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	invalid.Header.Set("X-Hub-Signature-256", "sha256=00")
	invalid.Header.Set("X-GitHub-Delivery", "ab-delivery")
	invalid.Header.Set("X-GitHub-Event", "push")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, invalid)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("invalid status=%d", w.Code)
	}
	if _, err := os.Stat(filepath.Join(root, "push")); !os.IsNotExist(err) {
		t.Fatalf("invalid request created archive: %v", err)
	}
	valid := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	valid.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
	valid.Header.Set("X-GitHub-Delivery", "ab-delivery")
	valid.Header.Set("X-GitHub-Event", "push")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, valid)
	if w.Code != http.StatusAccepted {
		t.Fatalf("valid status=%d", w.Code)
	}
	archived, err := os.ReadFile(filepath.Join(root, "YW", "YWItZGVsaXZlcnk", "payload.push.json"))
	if err != nil || !bytes.Equal(archived, body) {
		t.Fatalf("archive=%q err=%v", archived, err)
	}
}

func TestHandlerReturnsServiceUnavailableWhenArchiveFails(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1024, 10, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	root := filepath.Join(t.TempDir(), "archive")
	a, err := archive.Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 8, &metrics.Metrics{})
	body := []byte(`{}`)
	r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
	r.Header.Set("X-GitHub-Delivery", "delivery")
	r.Header.Set("X-GitHub-Event", "push")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("items=%d err=%v", len(items), err)
	}
}

func TestArchivedRedeliveryBypassesReservationOnlyWhenPayloadExists(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	root := filepath.Join(t.TempDir(), "archive")
	a, err := archive.Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 1, m)
	body := []byte(`{"id":1}`)
	request := func(event string, payload []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		r.Header.Set("X-Hub-Signature-256", signature(payload, "secret"))
		r.Header.Set("X-GitHub-Delivery", "delivery")
		r.Header.Set("X-GitHub-Event", event)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := request("push", body); w.Code != http.StatusAccepted {
		t.Fatalf("initial status=%d", w.Code)
	}
	lowSpaceArchive, err := archive.Open(root, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	h = New(s, lowSpaceArchive, "secret", "oc_default", 1024, time.Hour, 1, m)
	if w := request("push", body); w.Code != http.StatusAccepted {
		t.Fatalf("matching redelivery status=%d", w.Code)
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 1 {
		t.Fatalf("stats after duplicate=%+v err=%v", stats, err)
	}
	metricsResponse := httptest.NewRecorder()
	m.Handler(func() metrics.Snapshot { return metrics.Snapshot{} }).ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metricsResponse.Body.String(), "lark_git_webhook_webhooks_duplicates_total 1") {
		t.Fatalf("duplicate metric=%s", metricsResponse.Body.String())
	}
	if w := request("push", []byte(`{"id":2}`)); w.Code != http.StatusConflict {
		t.Fatalf("different payload status=%d", w.Code)
	}
	if w := request("issues", body); w.Code != http.StatusConflict {
		t.Fatalf("different event status=%d", w.Code)
	}
	if err := os.Remove(filepath.Join(root, "ZG", "ZGVsaXZlcnk", "payload.push.json")); err != nil {
		t.Fatal(err)
	}
	if w := request("push", body); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing archive status=%d", w.Code)
	}
	stats, err = s.Stats()
	if err != nil || stats.Items != 1 {
		t.Fatalf("stats after missing archive=%+v err=%v", stats, err)
	}
}

func TestPendingRedeliveryFinalizesWithoutReservation(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	root := filepath.Join(t.TempDir(), "archive")
	a, err := archive.Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":1}`)
	if duplicate, err := s.Admit("delivery", "push", body, time.Now(), time.Hour); err != nil || duplicate {
		t.Fatalf("admit duplicate=%v err=%v", duplicate, err)
	}
	if _, err := a.Store("push", "delivery", body); err != nil {
		t.Fatal(err)
	}
	lowSpaceArchive, err := archive.Open(root, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	m := &metrics.Metrics{}
	h := New(s, lowSpaceArchive, "secret", "oc_default", 1024, time.Hour, 1, m)
	request := func(event string, payload []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		r.Header.Set("X-Hub-Signature-256", signature(payload, "secret"))
		r.Header.Set("X-GitHub-Delivery", "delivery")
		r.Header.Set("X-GitHub-Event", event)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := request("push", body); w.Code != http.StatusAccepted {
		t.Fatalf("pending recovery status=%d", w.Code)
	}
	items, err := s.Due(time.Now(), 10)
	if err != nil || len(items) != 1 || !items[0].Archived {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if w := request("push", []byte(`{"id":2}`)); w.Code != http.StatusConflict {
		t.Fatalf("different body status=%d", w.Code)
	}
	if w := request("issues", body); w.Code != http.StatusConflict {
		t.Fatalf("different event status=%d", w.Code)
	}
}

func TestHandlerRejectsBeforeAdmissionWhenReservationFails(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 1, &metrics.Metrics{})
	body := []byte(`{}`)
	r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
	r.Header.Set("X-GitHub-Delivery", "delivery")
	r.Header.Set("X-GitHub-Event", "push")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", w.Code)
	}
	stats, err := s.Stats()
	if err != nil || stats.Items != 0 || stats.Bytes != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	pending, err := s.PendingArchives(1)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
}

func TestHandlerSnapshotsDefaultAndSubscribedChats(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.AddRepositorySubscription("oc_subscribed", "acme/repo"); err != nil {
		t.Fatal(err)
	}
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 1, &metrics.Metrics{})
	body := []byte(`{"repository":{"full_name":"Acme/Repo"}}`)
	r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
	r.Header.Set("X-GitHub-Delivery", "delivery")
	r.Header.Set("X-GitHub-Event", "push")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, r)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d", response.Code)
	}
	items, err := s.Due(time.Now(), 1)
	if err != nil || len(items) != 1 || strings.Join(items[0].ChatIDs, ",") != "oc_default,oc_subscribed" {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

func TestHandlerLimitsInFlightRequestsAndReleasesSlots(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "queue.db"), 10, 1024, 10, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := archive.Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	h := New(s, a, "secret", "oc_default", 1024, time.Hour, 1, &metrics.Metrics{})
	release := make(chan struct{})
	started := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/webhook", blockingReader{started: started, release: release}))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire its slot")
	}
	full := httptest.NewRecorder()
	h.ServeHTTP(full, httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte(`{}`))))
	if full.Code != http.StatusServiceUnavailable {
		t.Fatalf("full status=%d", full.Code)
	}
	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first request did not release its slot")
	}
	body := []byte(`{}`)
	recovered := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	r.Header.Set("X-Hub-Signature-256", signature(body, "secret"))
	r.Header.Set("X-GitHub-Delivery", "delivery")
	r.Header.Set("X-GitHub-Event", "push")
	h.ServeHTTP(recovered, r)
	if recovered.Code != http.StatusAccepted {
		t.Fatalf("released status=%d", recovered.Code)
	}
}

type blockingReader struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r blockingReader) Read([]byte) (int, error) {
	close(r.started)
	<-r.release
	return 0, io.EOF
}

func signature(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
