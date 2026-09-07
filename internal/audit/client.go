package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	apiURL                       = "https://api.github.com"
	apiVersion                   = "2026-03-10"
	maxBodyBytes                 = 1 << 20
	maxRateLimitMessageBodyBytes = 4 << 10
	auditQueryOverlap            = 5 * time.Minute
)

// RetryError reports an audit-log request that may be retried after Wait.
type RetryError struct {
	Status int
	Wait   time.Duration
}

func (e *RetryError) Error() string   { return fmt.Sprintf("GitHub audit log HTTP status %d", e.Status) }
func (e *RetryError) Temporary() bool { return true }

type Client struct {
	token string
	http  *http.Client
}

func NewClient(token string) *Client {
	return &Client{token: token, http: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
}

// Fetch returns audit records in API order. Every page URL is revalidated before
// it is requested so an untrusted Link header cannot receive the bearer token.
func (c *Client) Fetch(ctx context.Context, org string, checkpoint time.Time) ([]json.RawMessage, error) {
	u, _ := url.Parse(apiURL + "/orgs/" + url.PathEscape(org) + "/audit-log")
	q := u.Query()
	q.Set("include", "all")
	q.Set("order", "asc")
	q.Set("per_page", "100")
	start := checkpoint.UTC().Truncate(time.Second)
	epoch := time.Unix(0, 0).UTC()
	if start.Before(epoch.Add(auditQueryOverlap)) {
		start = epoch
	} else {
		start = start.Add(-auditQueryOverlap)
	}
	q.Set("phrase", "created:>="+start.Format(time.RFC3339))
	u.RawQuery = q.Encode()
	var records []json.RawMessage
	for u != nil {
		if !validAuditURL(u, org) {
			return nil, errors.New("invalid GitHub audit pagination link")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create GitHub audit request: %w", err)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("X-GitHub-Api-Version", apiVersion)
		req.Header.Set("User-Agent", "lark-git-webhook")
		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, &RetryError{Wait: time.Minute}
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		resp.Body.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if readErr != nil {
			return nil, &RetryError{Status: resp.StatusCode, Wait: time.Minute}
		}
		if len(body) > maxBodyBytes {
			return nil, errors.New("GitHub audit response exceeds 1 MiB")
		}
		now := time.Now()
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 || (resp.StatusCode == http.StatusForbidden && rateLimitEvidence(resp.Header, now)) {
			return nil, &RetryError{Status: resp.StatusCode, Wait: retryWait(resp.Header, now)}
		}
		if resp.StatusCode == http.StatusForbidden {
			if !hasRateLimitHeaders(resp.Header) && secondaryRateLimitMessage(body) {
				return nil, &RetryError{Status: resp.StatusCode, Wait: time.Minute}
			}
			return nil, errors.New("GitHub audit log access forbidden")
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GitHub audit log HTTP status %d", resp.StatusCode)
		}
		var page []json.RawMessage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, errors.New("invalid GitHub audit response")
		}
		for _, record := range page {
			if !json.Valid(record) {
				return nil, errors.New("invalid GitHub audit record")
			}
			records = append(records, append(json.RawMessage(nil), record...))
		}
		u = nextLink(resp.Header.Get("Link"), org)
	}
	return records, nil
}

func validAuditURL(u *url.URL, org string) bool {
	return u != nil && u.Scheme == "https" && u.Host == "api.github.com" && u.User == nil && u.Path == "/orgs/"+org+"/audit-log"
}

func nextLink(header, org string) *url.URL {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if !strings.Contains(part, "rel=\"next\"") && !strings.Contains(part, "rel=next") {
			continue
		}
		start, end := strings.Index(part, "<"), strings.Index(part, ">")
		if start < 0 || end <= start {
			return &url.URL{}
		}
		u, err := url.Parse(part[start+1 : end])
		if err != nil || !validAuditURL(u, org) {
			return &url.URL{}
		}
		return u
	}
	return nil
}

func rateLimitEvidence(header http.Header, now time.Time) bool {
	if value := strings.TrimSpace(header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
			return true
		}
		if at, err := http.ParseTime(value); err == nil && at.After(now) {
			return true
		}
	}
	if strings.TrimSpace(header.Get("X-RateLimit-Remaining")) != "0" {
		return false
	}
	reset, err := strconv.ParseInt(header.Get("X-RateLimit-Reset"), 10, 64)
	return err == nil && reset > now.Unix()
}

func hasRateLimitHeaders(header http.Header) bool {
	return header.Get("Retry-After") != "" || header.Get("X-RateLimit-Remaining") != "" || header.Get("X-RateLimit-Reset") != ""
}

func secondaryRateLimitMessage(body []byte) bool {
	if len(body) > maxRateLimitMessageBodyBytes {
		return false
	}
	var response struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &response) != nil {
		return false
	}
	message := strings.ToLower(response.Message)
	return strings.Contains(message, "rate limit") || strings.Contains(message, "secondary rate limit") || strings.Contains(message, "abuse detection")
}

func retryWait(header http.Header, now time.Time) time.Duration {
	if value := strings.TrimSpace(header.Get("Retry-After")); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
		if at, err := http.ParseTime(value); err == nil && at.After(now) {
			return at.Sub(now)
		}
	}
	if reset, err := strconv.ParseInt(header.Get("X-RateLimit-Reset"), 10, 64); err == nil && reset > now.Unix() {
		return time.Until(time.Unix(reset, 0))
	}
	return time.Minute
}
