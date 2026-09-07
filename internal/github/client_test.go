package github

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func response(status int, body string, header http.Header) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func TestValidateUserUsesFixedGitHubRequest(t *testing.T) {
	client := New()
	client.http.Transport = roundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.Scheme != "https" || req.URL.Host != "api.github.com" || req.URL.Path != "/users/octocat" || req.URL.RawQuery != "" {
			t.Fatalf("request=%s", req.URL)
		}
		if got := req.Header.Get("Accept"); got != "application/vnd.github+json" {
			t.Fatalf("Accept=%q", got)
		}
		if got := req.Header.Get("User-Agent"); got != userAgent {
			t.Fatalf("User-Agent=%q", got)
		}
		if got := req.Header.Get("X-GitHub-Api-Version"); got != apiVersion {
			t.Fatalf("X-GitHub-Api-Version=%q", got)
		}
		return response(http.StatusOK, `{"login":"OctoCat"}`, nil), nil
	})

	login, err := client.ValidateUser(context.Background(), "octocat")
	if err != nil || login != "OctoCat" {
		t.Fatalf("login=%q err=%v", login, err)
	}
}

func TestValidateUserResponseClasses(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		header    http.Header
		notFound  bool
		temporary bool
	}{
		{name: "not found", status: http.StatusNotFound, notFound: true},
		{name: "forbidden", status: http.StatusForbidden, temporary: true},
		{name: "rate limited", status: http.StatusTooManyRequests, temporary: true},
		{name: "server error", status: http.StatusInternalServerError, temporary: true},
		{name: "other invalid status", status: http.StatusBadRequest, temporary: true},
		{name: "invalid JSON", status: http.StatusOK, body: `{`, temporary: true},
		{name: "missing login", status: http.StatusOK, body: `{}`, temporary: true},
		{name: "redirect", status: http.StatusFound, header: http.Header{"Location": []string{"https://example.invalid/"}}, temporary: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := New()
			client.http.Transport = roundTripper(func(*http.Request) (*http.Response, error) {
				return response(test.status, test.body, test.header), nil
			})

			_, err := client.ValidateUser(context.Background(), "missing")
			var notFound *NotFoundError
			var temporary *TemporaryError
			if errors.As(err, &notFound) != test.notFound || errors.As(err, &temporary) != test.temporary {
				t.Fatalf("err=%v notFound=%v temporary=%v", err, errors.As(err, &notFound), errors.As(err, &temporary))
			}
		})
	}
}

func TestValidateUserReturnsTemporaryErrorsForTimeoutAndOversizedResponse(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		client := New()
		client.http.Transport = roundTripper(func(*http.Request) (*http.Response, error) {
			return nil, &url.Error{Op: "Get", URL: "https://api.github.com/users/octocat", Err: context.DeadlineExceeded}
		})

		_, err := client.ValidateUser(context.Background(), "octocat")
		var temporary *TemporaryError
		if !errors.As(err, &temporary) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("oversized response", func(t *testing.T) {
		client := New()
		client.http.Transport = roundTripper(func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, string(bytes.Repeat([]byte("x"), maxResponseBodyBytes+1)), nil), nil
		})

		_, err := client.ValidateUser(context.Background(), "octocat")
		var temporary *TemporaryError
		if !errors.As(err, &temporary) {
			t.Fatalf("err=%v", err)
		}
	})
}
