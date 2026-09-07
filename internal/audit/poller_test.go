package audit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

type fakeFetcher struct {
	records []json.RawMessage
	calls   int
	err     error
	fetch   func(context.Context, string, time.Time) ([]json.RawMessage, error)
}

func (f *fakeFetcher) Fetch(ctx context.Context, org string, checkpoint time.Time) ([]json.RawMessage, error) {
	f.calls++
	if f.fetch != nil {
		return f.fetch(ctx, org, checkpoint)
	}
	return f.records, f.err
}

func testPoller(t *testing.T, f *fakeFetcher) (*Poller, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state.db"), 100, 1<<20, 100, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	a, err := archive.Open(filepath.Join(dir, "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	return NewPoller(s, a, f, "example-org", time.Minute, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil))), s
}

func TestPollerInitializesThenArchivesAndDeduplicates(t *testing.T) {
	raw := json.RawMessage(`{"_document_id":"one","action":"team.add_member","created_at":"2026-01-01T00:00:01Z"}`)
	f := &fakeFetcher{records: []json.RawMessage{raw, raw}}
	p, s := testPoller(t, f)
	first := time.Date(2026, 1, 1, 0, 0, 0, 1, time.UTC)
	if err := p.PollOnce(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if f.calls != 0 {
		t.Fatal("initial poll fetched history")
	}
	checkpoint, found, err := s.AuditCheckpoint("example-org")
	if err != nil || !found || !checkpoint.Equal(first) {
		t.Fatalf("checkpoint=%s found=%v err=%v", checkpoint, found, err)
	}
	second := first.Add(time.Minute)
	if err := p.PollOnce(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(second, 10)
	if err != nil || len(items) != 1 || items[0].Event != "audit_log" {
		t.Fatalf("items=%#v err=%v", items, err)
	}
	checkpoint, _, _ = s.AuditCheckpoint("example-org")
	if !checkpoint.Equal(second) {
		t.Fatalf("checkpoint=%s", checkpoint)
	}
}

func TestPollerInitialOverlapDoesNotReplayHistory(t *testing.T) {
	first := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	f := &fakeFetcher{records: []json.RawMessage{
		json.RawMessage(`{"_document_id":"before","created_at":"2026-01-01T00:00:00Z"}`),
		json.RawMessage(`{"_document_id":"at","created_at":"2026-01-01T00:00:00.123456789Z"}`),
		json.RawMessage(`{"_document_id":"after","created_at":"2026-01-01T00:00:01Z"}`),
	}}
	p, s := testPoller(t, f)
	if err := p.PollOnce(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := first.Add(time.Minute)
	if err := p.PollOnce(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(second, 10)
	if err != nil || len(items) != 1 || items[0].DeliveryID != "audit:example-org:after" {
		t.Fatalf("items=%#v err=%v", items, err)
	}
}

func TestPollerUsesRawDigestWhenDocumentIDIsAbsent(t *testing.T) {
	raw := json.RawMessage(`{"action":"repo.destroy","created_at":"2026-01-01T00:00:01Z"}`)
	f := &fakeFetcher{records: []json.RawMessage{raw}}
	p, s := testPoller(t, f)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := p.PollOnce(context.Background(), at); err != nil {
		t.Fatal(err)
	}
	if err := p.PollOnce(context.Background(), at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	items, err := s.Due(at.Add(time.Minute), 1)
	if err != nil || len(items) != 1 || !strings.HasPrefix(items[0].DeliveryID, "audit:example-org:") {
		t.Fatalf("items=%#v err=%v", items, err)
	}
}

func TestPollerDoesNotAdvanceCheckpointAfterFetchFailure(t *testing.T) {
	f := &fakeFetcher{}
	p, s := testPoller(t, f)
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := p.PollOnce(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	f.err = errors.New("unavailable")
	if err := p.PollOnce(context.Background(), first.Add(time.Minute)); err == nil {
		t.Fatal("expected fetch failure")
	}
	checkpoint, _, _ := s.AuditCheckpoint("example-org")
	if !checkpoint.Equal(first) {
		t.Fatalf("checkpoint advanced to %s", checkpoint)
	}
}

func TestPollerRetriesAfterRetryWait(t *testing.T) {
	f := &fakeFetcher{}
	p, _ := testPoller(t, f)
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := p.PollOnce(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	const wait = 20 * time.Millisecond
	retried := make(chan time.Duration, 1)
	var firstAttempt time.Time
	f.fetch = func(context.Context, string, time.Time) ([]json.RawMessage, error) {
		if f.calls == 1 {
			firstAttempt = time.Now()
			return nil, &RetryError{Status: 429, Wait: wait}
		}
		retried <- time.Since(firstAttempt)
		return nil, nil
	}
	done := make(chan struct{})
	go func() {
		p.poll(context.Background())
		close(done)
	}()
	select {
	case elapsed := <-retried:
		if elapsed < wait {
			t.Fatalf("retry happened after %s, want at least %s", elapsed, wait)
		}
	case <-time.After(time.Second):
		t.Fatal("poller did not retry after Retry-After wait")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poller did not finish after retry")
	}
}

func TestPollerRetryWaitIsCancellable(t *testing.T) {
	f := &fakeFetcher{}
	p, _ := testPoller(t, f)
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := p.PollOnce(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	f.fetch = func(context.Context, string, time.Time) ([]json.RawMessage, error) {
		close(started)
		return nil, &RetryError{Status: 429, Wait: time.Hour}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.poll(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("poller did not begin retry wait")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poller did not cancel retry wait")
	}
}

func TestPollerStopsAfterFetchContextCancellation(t *testing.T) {
	raw := json.RawMessage(`{"_document_id":"one","action":"team.add_member"}`)
	f := &fakeFetcher{}
	p, s := testPoller(t, f)
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := p.PollOnce(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.fetch = func(context.Context, string, time.Time) ([]json.RawMessage, error) {
		cancel()
		return []json.RawMessage{raw}, nil
	}
	if err := p.PollOnce(ctx, first.Add(time.Minute)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	checkpoint, _, err := s.AuditCheckpoint("example-org")
	if err != nil || !checkpoint.Equal(first) {
		t.Fatalf("checkpoint=%s err=%v", checkpoint, err)
	}
	items, err := s.Due(first.Add(time.Minute), 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("items=%#v err=%v", items, err)
	}
}

func TestPollerBootstrapTimestampBoundary(t *testing.T) {
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		record    json.RawMessage
		wantError bool
	}{
		{name: "missing", record: json.RawMessage(`{"_document_id":"missing"}`), wantError: true},
		{name: "malformed", record: json.RawMessage(`{"_document_id":"malformed","created_at":"not-a-time"}`), wantError: true},
		{name: "numeric milliseconds", record: json.RawMessage(`{"_document_id":"milliseconds","created_at":1767225601000}`)},
		{name: "numeric seconds", record: json.RawMessage(`{"_document_id":"seconds","created_at":1767225601}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeFetcher{records: []json.RawMessage{tc.record}}
			p, s := testPoller(t, f)
			if err := p.PollOnce(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			err := p.PollOnce(context.Background(), first.Add(time.Minute))
			if tc.wantError {
				if err == nil || err.Error() != errBootstrapTimestamp.Error() {
					t.Fatalf("err=%v", err)
				}
				checkpoint, _, checkpointErr := s.AuditCheckpoint("example-org")
				if checkpointErr != nil || !checkpoint.Equal(first) {
					t.Fatalf("checkpoint=%s err=%v", checkpoint, checkpointErr)
				}
				items, dueErr := s.Due(first.Add(time.Minute), 1)
				if dueErr != nil || len(items) != 0 {
					t.Fatalf("items=%#v err=%v", items, dueErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			items, dueErr := s.Due(first.Add(time.Minute), 1)
			if dueErr != nil || len(items) != 1 {
				t.Fatalf("items=%#v err=%v", items, dueErr)
			}
		})
	}
}
