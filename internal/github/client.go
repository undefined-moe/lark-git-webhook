// Package github validates GitHub users through the public GitHub API.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	apiURL               = "https://api.github.com"
	userAgent            = "lark-git-webhook"
	apiVersion           = "2022-11-28"
	maxResponseBodyBytes = 1 << 20
)

// NotFoundError reports that a GitHub username does not exist.
type NotFoundError struct {
	Username string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("GitHub user %q not found", e.Username)
}

// TemporaryError reports a validation failure that may succeed when retried.
type TemporaryError struct {
	Err error
}

func (e *TemporaryError) Error() string   { return e.Err.Error() }
func (e *TemporaryError) Unwrap() error   { return e.Err }
func (e *TemporaryError) Temporary() bool { return true }

// Client validates GitHub users using the public API.
type Client struct {
	http *http.Client
}

// New creates a GitHub user validation client.
func New() *Client {
	return &Client{http: &http.Client{
		Timeout:       2 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// ValidateUser returns the canonical GitHub login for username.
func (c *Client) ValidateUser(ctx context.Context, username string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/users/"+url.PathEscape(username), nil)
	if err != nil {
		return "", &TemporaryError{Err: errors.New("create GitHub user request")}
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("X-GitHub-Api-Version", apiVersion)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", &TemporaryError{Err: fmt.Errorf("GitHub user request failed: %w", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if err != nil {
		return "", &TemporaryError{Err: errors.New("read GitHub user response")}
	}
	if len(body) > maxResponseBodyBytes {
		return "", &TemporaryError{Err: errors.New("GitHub user response exceeds 1 MiB")}
	}

	switch resp.StatusCode {
	case http.StatusOK:
		var user struct {
			Login string `json:"login"`
		}
		if err := json.Unmarshal(body, &user); err != nil || user.Login == "" {
			return "", &TemporaryError{Err: errors.New("invalid GitHub user response")}
		}
		return user.Login, nil
	case http.StatusNotFound:
		return "", &NotFoundError{Username: username}
	default:
		return "", &TemporaryError{Err: fmt.Errorf("GitHub user HTTP status %d", resp.StatusCode)}
	}
}
