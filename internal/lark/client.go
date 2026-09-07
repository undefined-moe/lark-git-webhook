package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const defaultBaseURL = "https://open.larksuite.com/open-apis"

type Message struct {
	MsgType string  `json:"msg_type"`
	Content Content `json:"content"`
}

type Content struct {
	Post Post `json:"post"`
}
type Post map[string]Locale
type Locale struct {
	Title   string   `json:"title,omitempty"`
	Content [][]Text `json:"content"`
}
type Text struct {
	Tag  string `json:"tag"`
	Text string `json:"text"`
	Href string `json:"href,omitempty"`
}

type RetryError struct {
	Err        error
	RetryAfter time.Duration
	Permanent  bool
	HTTPStatus int
}

func (e *RetryError) Error() string { return e.Err.Error() }
func (e *RetryError) Unwrap() error { return e.Err }

type Client struct {
	appID     string
	appSecret string
	chatID    string
	baseURL   string
	http      *http.Client
	now       func() time.Time
	tokenMu   sync.Mutex
	token     string
	tokenExp  time.Time
}

func New(appID, appSecret, chatID string, timeout time.Duration) *Client {
	return &Client{
		appID:     appID,
		appSecret: appSecret,
		chatID:    chatID,
		baseURL:   defaultBaseURL,
		now:       time.Now,
		http:      &http.Client{Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

// Send creates an ordinary post message in the configured chat.
func (c *Client) Send(ctx context.Context, message Message) error {
	return c.SendToChat(ctx, c.chatID, message)
}

// SendToChat creates an ordinary post message in the requested chat.
func (c *Client) SendToChat(ctx context.Context, chatID string, message Message) error {
	body, err := marshalPostRequest(chatID, message)
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodPost, "/im/v1/messages?receive_id_type=chat_id", json.RawMessage(body), nil)
}

// PostRequestBodySize returns the serialized size of an ordinary post request.
func PostRequestBodySize(receiveID string, message Message) (int, error) {
	body, err := marshalPostRequest(receiveID, message)
	return len(body), err
}

func marshalPostRequest(receiveID string, message Message) ([]byte, error) {
	content, err := json.Marshal(message.Content.Post)
	if err != nil {
		return nil, errors.New("marshal Lark post content")
	}
	body, err := json.Marshal(map[string]string{
		"receive_id": receiveID,
		"msg_type":   message.MsgType,
		"content":    string(content),
	})
	if err != nil {
		return nil, errors.New("marshal Lark post request")
	}
	return body, nil
}

// CreateInteractive creates an idempotent card message and returns its message ID.
func (c *Client) CreateInteractive(ctx context.Context, card any, uuid string) (string, error) {
	return c.CreateInteractiveToChat(ctx, c.chatID, card, uuid)
}

// CreateInteractiveToChat creates an idempotent card message in the requested chat.
func (c *Client) CreateInteractiveToChat(ctx context.Context, chatID string, card any, uuid string) (string, error) {
	return c.createInteractive(ctx, chatID, "chat_id", card, uuid)
}

// CreateInteractiveToOpenID creates an idempotent card message for an Open ID.
func (c *Client) CreateInteractiveToOpenID(ctx context.Context, openID string, card any, uuid string) (string, error) {
	return c.createInteractive(ctx, openID, "open_id", card, uuid)
}

func (c *Client) createInteractive(ctx context.Context, receiveID, receiveIDType string, card any, uuid string) (string, error) {
	content, err := json.Marshal(card)
	if err != nil {
		return "", errors.New("marshal Lark card")
	}
	var response struct {
		MessageID string `json:"message_id"`
	}
	err = c.call(ctx, http.MethodPost, "/im/v1/messages?receive_id_type="+receiveIDType, map[string]any{
		"receive_id": receiveID,
		"msg_type":   "interactive",
		"content":    string(content),
		"uuid":       uuid,
	}, &response)
	if err != nil {
		return "", err
	}
	if response.MessageID == "" {
		return "", &RetryError{Err: errors.New("Lark create response missing message_id")}
	}
	return response.MessageID, nil
}

func (c *Client) UpdateInteractive(ctx context.Context, messageID string, card any) error {
	content, err := json.Marshal(card)
	if err != nil {
		return errors.New("marshal Lark card")
	}
	return c.call(ctx, http.MethodPatch, "/im/v1/messages/"+url.PathEscape(messageID), map[string]any{
		"msg_type": "interactive",
		"content":  string(content),
	}, nil)
}

func (c *Client) AddReaction(ctx context.Context, messageID, emoji string) (string, error) {
	var response struct {
		ReactionID string `json:"reaction_id"`
	}
	err := c.call(ctx, http.MethodPost, "/im/v1/messages/"+url.PathEscape(messageID)+"/reactions", map[string]any{
		"reaction_type": map[string]string{"emoji_type": emoji},
	}, &response)
	if err != nil {
		return "", err
	}
	if response.ReactionID == "" {
		return "", &RetryError{Err: errors.New("Lark reaction response missing reaction_id")}
	}
	return response.ReactionID, nil
}

func (c *Client) DeleteReaction(ctx context.Context, messageID, reactionID string) error {
	err := c.call(ctx, http.MethodDelete, "/im/v1/messages/"+url.PathEscape(messageID)+"/reactions/"+url.PathEscape(reactionID), nil, nil)
	var retry *RetryError
	if errors.As(err, &retry) && retry.HTTPStatus == http.StatusNotFound {
		return nil
	}
	return err
}

func (c *Client) call(ctx context.Context, method, path string, body any, target any) error {
	for attempt := 0; attempt < 2; attempt++ {
		token, err := c.tenantToken(ctx, attempt > 0)
		if err != nil {
			return err
		}
		code, err := c.do(ctx, method, path, token, body, target)
		if err == nil {
			return nil
		}
		if attempt == 0 && (code == http.StatusUnauthorized || code == 99991663) {
			c.invalidateToken(token)
			continue
		}
		return err
	}
	return &RetryError{Err: errors.New("Lark request failed")}
}

func (c *Client) tenantToken(ctx context.Context, force bool) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if !force && c.token != "" && c.now().Add(time.Minute).Before(c.tokenExp) {
		return c.token, nil
	}
	payload, err := json.Marshal(map[string]string{"app_id": c.appID, "app_secret": c.appSecret})
	if err != nil {
		return "", &RetryError{Err: errors.New("marshal Lark token request")}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/auth/v3/tenant_access_token/internal/", bytes.NewReader(payload))
	if err != nil {
		return "", &RetryError{Err: errors.New("create Lark token request")}
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", &RetryError{Err: errors.New("Lark token request failed")}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", &RetryError{Err: errors.New("read Lark token response"), RetryAfter: retryAfter(resp), HTTPStatus: resp.StatusCode}
	}
	var response struct {
		Code              int    `json:"code"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int64  `json:"expire"`
	}
	if err := json.Unmarshal(body, &response); err == nil && response.Code != 0 {
		return "", larkResponseError("token", resp.StatusCode, response.Code, resp)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &RetryError{Err: fmt.Errorf("Lark token HTTP status %d", resp.StatusCode), RetryAfter: retryAfter(resp), Permanent: permanentStatus(resp.StatusCode), HTTPStatus: resp.StatusCode}
	}
	if response.Code != 0 || response.TenantAccessToken == "" || response.Expire <= 0 {
		return "", &RetryError{Err: errors.New("invalid Lark token response"), HTTPStatus: resp.StatusCode}
	}
	c.token, c.tokenExp = response.TenantAccessToken, c.now().Add(time.Duration(response.Expire)*time.Second)
	return c.token, nil
}
func (c *Client) invalidateToken(token string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.token == token {
		c.token, c.tokenExp = "", time.Time{}
	}
}

// do returns a Lark response code so an expired token can be refreshed once.
func (c *Client) do(ctx context.Context, method, path, token string, body any, target any) (int, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, &RetryError{Err: errors.New("marshal Lark request")}
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, &RetryError{Err: errors.New("create Lark request")}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, &RetryError{Err: errors.New("Lark request failed")}
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, &RetryError{Err: errors.New("read Lark response failed"), RetryAfter: retryAfter(resp), HTTPStatus: resp.StatusCode}
	}
	var envelope struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	decoded := json.Unmarshal(responseBody, &envelope) == nil
	// Lark carries its authoritative error code in the JSON envelope even when
	// the HTTP status is non-2xx. Preserve it for retry/permanence handling.
	if decoded && envelope.Code != 0 {
		return envelope.Code, larkResponseError("response", resp.StatusCode, envelope.Code, resp)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, &RetryError{Err: fmt.Errorf("Lark HTTP status %d", resp.StatusCode), RetryAfter: retryAfter(resp), Permanent: permanentStatus(resp.StatusCode), HTTPStatus: resp.StatusCode}
	}
	if !decoded {
		return 0, &RetryError{Err: errors.New("decode Lark response"), HTTPStatus: resp.StatusCode}
	}
	if target != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return 0, &RetryError{Err: errors.New("decode Lark response data"), HTTPStatus: resp.StatusCode}
		}
	}
	return 0, nil
}

func larkResponseError(scope string, status, code int, resp *http.Response) error {
	retry := &RetryError{Err: fmt.Errorf("Lark %s code %d", scope, code), RetryAfter: retryAfter(resp), HTTPStatus: status}
	if code == 99991400 { // transient service busy; GitHub/Lark retries need a small persisted cooldown.
		if retry.RetryAfter < time.Second {
			retry.RetryAfter = time.Second
		}
		return retry
	}
	if code == 10014 {
		retry.Permanent = true
		return retry
	} // invalid app credentials
	if status >= 400 && status < 500 && status != http.StatusNotFound && !permanentStatus(status) {
		return retry
	}
	if permanentStatus(status) {
		retry.Permanent = true
	}
	return retry
}
func permanentStatus(status int) bool {
	return status >= 400 && status < 500 && status != 408 && status != 409 && status != 425 && status != 429
}
func retryAfter(resp *http.Response) time.Duration {
	if resp.StatusCode != http.StatusTooManyRequests {
		return 0
	}
	value := resp.Header.Get("Retry-After")
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(time.Now()) {
		return time.Until(when)
	}
	return 0
}
