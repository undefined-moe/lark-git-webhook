package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientUsesTenantTokenAndCreatesPost(t *testing.T) {
	var tokenCalls atomic.Int32
	var messageCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/v3/tenant_access_token/internal/":
			tokenCalls.Add(1)
			var got map[string]string
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got["app_id"] != "cli_test" || got["app_secret"] != "secret" {
				t.Fatalf("token body=%v", got)
			}
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
		case "/im/v1/messages":
			messageCalls.Add(1)
			if r.URL.Query().Get("receive_id_type") != "chat_id" || r.Header.Get("Authorization") != "Bearer token" {
				t.Fatalf("request=%s authorization=%q", r.URL, r.Header.Get("Authorization"))
			}
			var got map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			var contentText string
			if err := json.Unmarshal(got["content"], &contentText); err != nil {
				t.Fatal(err)
			}
			var content Post
			if err := json.Unmarshal([]byte(contentText), &content); err != nil {
				t.Fatal(err)
			}
			if string(got["receive_id"]) != `"oc_test"` || string(got["msg_type"]) != `"post"` || content["zh_cn"].Title != "test" {
				t.Fatalf("message=%s", got)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"message_id":"om_1"}}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client := New("cli_test", "secret", "oc_test", time.Second)
	client.baseURL = server.URL
	if err := client.Send(context.Background(), MakeTestMessage()); err != nil {
		t.Fatal(err)
	}
	if err := client.Send(context.Background(), MakeTestMessage()); err != nil {
		t.Fatal(err)
	}
	if tokenCalls.Load() != 1 || messageCalls.Load() != 2 {
		t.Fatalf("token/messages=%d/%d", tokenCalls.Load(), messageCalls.Load())
	}
}

func TestPostRequestBodySizeMatchesSentRequest(t *testing.T) {
	message := Message{MsgType: "post", Content: Content{Post: Post{"zh_cn": {Content: [][]Text{{{Tag: "text", Text: `quote " and slash \\`}}}}}}}
	var sent []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/v3/tenant_access_token/internal/":
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
		case "/im/v1/messages":
			var err error
			sent, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := New("id", "secret", "oc_test", time.Second)
	client.baseURL = server.URL
	if err := client.Send(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	want, err := marshalPostRequest("oc_test", message)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sent, want) {
		t.Fatalf("sent=%s want=%s", sent, want)
	}
	size, err := PostRequestBodySize("oc_test", message)
	if err != nil || len(sent) != size {
		t.Fatalf("helper=%d sent=%d err=%v", size, len(sent), err)
	}
}

func TestInteractiveMethodsUseAppIMAPI(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		if r.URL.Path == "/auth/v3/tenant_access_token/internal/" {
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("authorization=%q", r.Header.Get("Authorization"))
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /im/v1/messages":
			var got map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if string(got["msg_type"]) != `"interactive"` {
				t.Fatalf("create=%s", got)
			}
			switch r.URL.Query().Get("receive_id_type") {
			case "chat_id":
				if string(got["receive_id"]) != `"oc_test"` || string(got["uuid"]) != `"suite-uuid"` {
					t.Fatalf("chat create=%s", got)
				}
				_, _ = w.Write([]byte(`{"code":0,"data":{"message_id":"om_1"}}`))
			case "open_id":
				if string(got["receive_id"]) != `"ou_test"` || string(got["uuid"]) != `"open-id-uuid"` {
					t.Fatalf("open ID create=%s", got)
				}
				_, _ = w.Write([]byte(`{"code":0,"data":{"message_id":"om_open"}}`))
			default:
				t.Fatalf("receive_id_type=%q", r.URL.Query().Get("receive_id_type"))
			}
		case "PATCH /im/v1/messages/om_1":
			_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
		case "POST /im/v1/messages/om_1/reactions":
			_, _ = w.Write([]byte(`{"code":0,"data":{"reaction_id":"reaction_1"}}`))
		case "DELETE /im/v1/messages/om_1/reactions/reaction_1":
			_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client := New("id", "secret", "oc_test", time.Second)
	client.baseURL = server.URL
	messageID, err := client.CreateInteractive(context.Background(), map[string]string{"tag": "card"}, "suite-uuid")
	if err != nil || messageID != "om_1" {
		t.Fatalf("create message=%q err=%v", messageID, err)
	}
	openMessageID, err := client.CreateInteractiveToOpenID(context.Background(), "ou_test", map[string]string{"tag": "card"}, "open-id-uuid")
	if err != nil || openMessageID != "om_open" {
		t.Fatalf("create Open ID message=%q err=%v", openMessageID, err)
	}
	if err := client.UpdateInteractive(context.Background(), messageID, map[string]string{"tag": "card"}); err != nil {
		t.Fatal(err)
	}
	reactionID, err := client.AddReaction(context.Background(), messageID, "DONE")
	if err != nil || reactionID != "reaction_1" {
		t.Fatalf("reaction=%q err=%v", reactionID, err)
	}
	if err := client.DeleteReaction(context.Background(), messageID, reactionID); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(paths, ","); !strings.Contains(got, "PATCH /im/v1/messages/om_1") || !strings.Contains(got, "DELETE /im/v1/messages/om_1/reactions/reaction_1") {
		t.Fatalf("paths=%s", got)
	}
}

func TestClientRefreshesTokenAfterUnauthorized(t *testing.T) {
	var tokenCalls atomic.Int32
	var messageCalls atomic.Int32
	var messageBodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v3/tenant_access_token/internal/" {
			call := tokenCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token-` + string(rune('0'+call)) + `","expire":7200}`))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		messageBodies = append(messageBodies, body)
		if messageCalls.Add(1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") != "Bearer token-2" {
			t.Fatalf("authorization=%q", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
	}))
	defer server.Close()
	client := New("id", "secret", "oc", time.Second)
	client.baseURL = server.URL
	if err := client.Send(context.Background(), MakeTestMessage()); err != nil {
		t.Fatal(err)
	}
	if tokenCalls.Load() != 2 || messageCalls.Load() != 2 {
		t.Fatalf("token/messages=%d/%d", tokenCalls.Load(), messageCalls.Load())
	}
	if len(messageBodies) != 2 || !bytes.Equal(messageBodies[0], messageBodies[1]) {
		t.Fatalf("retried message bodies differ: %q", messageBodies)
	}
}

func TestClientReturnsRetryError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v3/tenant_access_token/internal/" {
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
			return
		}
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := New("id", "secret", "oc", time.Second)
	client.baseURL = server.URL
	err := client.Send(context.Background(), MakeTestMessage())
	var retry *RetryError
	if !errors.As(err, &retry) || retry.RetryAfter != 3*time.Second || retry.Permanent {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
}

func MakeTestMessage() Message {
	return Message{MsgType: "post", Content: Content{Post: Post{"zh_cn": {Title: "test", Content: [][]Text{{{Tag: "text", Text: "test"}}}}}}}
}

func TestClientHandlesEnvelopeErrorsOnHTTPErrorAndMissingReaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v3/tenant_access_token/internal/" {
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":7200}`))
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":99991679}`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":99991400}`))
	}))
	defer server.Close()
	client := New("id", "secret", "oc", time.Second)
	client.baseURL = server.URL
	err := client.Send(context.Background(), MakeTestMessage())
	var retry *RetryError
	if !errors.As(err, &retry) || retry.Permanent || retry.RetryAfter < time.Second {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
	if err := client.DeleteReaction(context.Background(), "om", "gone"); err != nil {
		t.Fatalf("missing reaction: %v", err)
	}
}
