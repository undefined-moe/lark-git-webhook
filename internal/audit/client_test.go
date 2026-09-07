package audit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

type cancelingReadBody struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (b cancelingReadBody) Read([]byte) (int, error) {
	b.cancel()
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (cancelingReadBody) Close() error { return nil }

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(status int, body, link string, headers map[string]string) *http.Response {
	h := make(http.Header)
	if link != "" {
		h.Set("Link", link)
	}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

func TestClientAuthenticatesAndFollowsValidatedPages(t *testing.T) {
	requests := 0
	client := NewClient("test-token")
	client.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("authorization=%q", got)
		}
		if r.Header.Get("Accept") != "application/vnd.github+json" || r.Header.Get("X-GitHub-Api-Version") != apiVersion {
			t.Fatal("missing GitHub headers")
		}
		if r.URL.Host != "api.github.com" || r.URL.Path != "/orgs/example-org/audit-log" {
			t.Fatalf("URL=%s", r.URL)
		}
		if requests == 1 {
			if r.URL.Query().Get("phrase") != "created:>=2026-01-02T02:59:05Z" {
				t.Fatalf("phrase=%q", r.URL.Query().Get("phrase"))
			}
			return response(http.StatusOK, `[{"_document_id":"one"}]`, `<https://api.github.com/orgs/example-org/audit-log?page=2>; rel="next"`, nil), nil
		}
		return response(http.StatusOK, `[{"_document_id":"two"}]`, "", nil), nil
	})
	records, err := client.Fetch(context.Background(), "example-org", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(records) != 2 || !bytes.Equal(records[0], []byte(`{"_document_id":"one"}`)) {
		t.Fatalf("requests=%d records=%q", requests, records)
	}
}

func TestClientRejectsUnsafeNextLinksAndOversizeBodies(t *testing.T) {
	for _, link := range []string{
		`<https://evil.example/orgs/example-org/audit-log?page=2>; rel="next"`,
		`<https://api.github.com/user?page=2>; rel="next"`,
	} {
		t.Run(link, func(t *testing.T) {
			client := NewClient("test-token")
			client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) { return response(http.StatusOK, `[]`, link, nil), nil })
			if _, err := client.Fetch(context.Background(), "example-org", time.Now()); err == nil {
				t.Fatal("accepted unsafe pagination link")
			}
		})
	}
	client := NewClient("test-token")
	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, strings.Repeat("x", maxBodyBytes+1), "", nil), nil
	})
	if _, err := client.Fetch(context.Background(), "example-org", time.Now()); err == nil {
		t.Fatal("accepted oversized body")
	}
}

func TestClientReturnsCancellationFromResponseRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient("test-token")
	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: cancelingReadBody{ctx: ctx, cancel: cancel}}, nil
	})
	_, err := client.Fetch(ctx, "example-org", time.Now())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestClientQueryRoundsAndOverlapsCheckpoint(t *testing.T) {
	client := NewClient("test-token")
	client.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got, want := r.URL.Query().Get("phrase"), "created:>=2026-01-02T02:59:05Z"; got != want {
			t.Fatalf("phrase=%q want %q", got, want)
		}
		return response(http.StatusOK, `[]`, "", nil), nil
	})
	_, err := client.Fetch(context.Background(), "example-org", time.Date(2026, 1, 2, 3, 4, 5, 987654321, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientQueryClampsAtUnixEpoch(t *testing.T) {
	client := NewClient("test-token")
	client.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got, want := r.URL.Query().Get("phrase"), "created:>=1970-01-01T00:00:00Z"; got != want {
			t.Fatalf("phrase=%q want %q", got, want)
		}
		return response(http.StatusOK, `[]`, "", nil), nil
	})
	_, err := client.Fetch(context.Background(), "example-org", time.Unix(1, 900000000).UTC())
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientReturnsRateLimitHint(t *testing.T) {
	client := NewClient("test-token")
	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusTooManyRequests, "", "", map[string]string{"Retry-After": "7"}), nil
	})
	_, err := client.Fetch(context.Background(), "example-org", time.Now())
	var retry *RetryError
	if !errors.As(err, &retry) || retry.Wait != 7*time.Second {
		t.Fatalf("err=%v", err)
	}
}

func TestClientRetriesOnlyRateLimitedForbidden(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers map[string]string
		retry   bool
	}{
		{name: "retry-after", headers: map[string]string{"Retry-After": "7"}, retry: true},
		{name: "exhausted-rate-limit", headers: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "2000000000"}, retry: true},
		{name: "ordinary-forbidden", headers: map[string]string{"X-RateLimit-Remaining": "42"}},
		{name: "invalid-retry-after", headers: map[string]string{"Retry-After": "invalid"}},
		{name: "stale-rate-limit-reset", headers: map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient("test-token")
			client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				return response(http.StatusForbidden, "sensitive response body test-token", "", tc.headers), nil
			})
			_, err := client.Fetch(context.Background(), "example-org", time.Now())
			var retry *RetryError
			if errors.As(err, &retry) != tc.retry {
				t.Fatalf("err=%v retry=%v", err, retry)
			}
			if !tc.retry && (err == nil || err.Error() != "GitHub audit log access forbidden") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestClientRecognizesHeaderlessSecondaryRateLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  string
		retry bool
	}{
		{name: "rate limit", body: `{"message":"API RATE LIMIT exceeded"}`, retry: true},
		{name: "secondary rate limit", body: `{"message":"You have exceeded a secondary rate limit."}`, retry: true},
		{name: "abuse detection", body: `{"message":"Abuse detection mechanism triggered"}`, retry: true},
		{name: "other forbidden", body: `{"message":"sensitive response body test-token"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient("test-token")
			client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
				return response(http.StatusForbidden, tc.body, "", nil), nil
			})
			_, err := client.Fetch(context.Background(), "example-org", time.Now())
			var retry *RetryError
			if errors.As(err, &retry) != tc.retry {
				t.Fatalf("err=%v retry=%v", err, retry)
			}
			if tc.retry && retry.Wait != time.Minute {
				t.Fatalf("wait=%s", retry.Wait)
			}
			if !tc.retry && (err == nil || err.Error() != "GitHub audit log access forbidden" || strings.Contains(err.Error(), "test-token")) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
