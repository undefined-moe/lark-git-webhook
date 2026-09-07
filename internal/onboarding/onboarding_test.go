package onboarding

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseEnteredEvent(t *testing.T) {
	event, err := ParseEnteredEvent([]byte(`{"event":{"operator_id":{"open_id":"ou_123"},"chat_id":"oc_123"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if event.OpenID != "ou_123" || event.ChatID != "oc_123" {
		t.Fatalf("event=%+v", event)
	}
	if _, err := ParseEnteredEvent([]byte(`{`)); err == nil {
		t.Fatal("invalid JSON was accepted")
	}
}

func TestParseCallbackRecognizesOnlyOnboardingAction(t *testing.T) {
	body := []byte(`{"event":{"operator":{"open_id":"ou_123"},"action":{"name":"submit_github_id","value":{"action":"submit_github_id"},"form_value":{"github_id":"octocat"}},"context":{"open_message_id":"om_123"}}}`)
	callback, err := ParseCallback(body)
	if err != nil {
		t.Fatal(err)
	}
	if !callback.IsOnboarding() || callback.OpenID != "ou_123" || callback.GitHubID != "octocat" || callback.OpenMessageID != "om_123" {
		t.Fatalf("callback=%+v", callback)
	}
	callback.ActionValue = "another_action"
	if callback.IsOnboarding() {
		t.Fatal("callback with mismatched value action was accepted")
	}
	if _, err := ParseCallback([]byte(`{`)); err == nil {
		t.Fatal("invalid JSON was accepted")
	}
}

func TestValidateGitHubLogin(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
		ok    bool
	}{
		{" octocat ", "octocat", true},
		{"A1-b", "A1-b", true},
		{strings.Repeat("a", 39), strings.Repeat("a", 39), true},
		{"", "", false},
		{"-octocat", "", false},
		{"octocat-", "", false},
		{"octo--cat", "", false},
		{"octo_cat", "", false},
		{"octo猫", "", false},
		{strings.Repeat("a", 40), "", false},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, ok := ValidateGitHubLogin(test.input)
			if got != test.want || ok != test.ok {
				t.Fatalf("ValidateGitHubLogin(%q) = %q, %v; want %q, %v", test.input, got, ok, test.want, test.ok)
			}
		})
	}
}

func TestBoundCardIsReadOnlyPlainText(t *testing.T) {
	encoded, err := json.Marshal(BoundCard("<script>OctoCat</script>"))
	if err != nil {
		t.Fatal(err)
	}
	var card struct {
		Schema string `json:"schema"`
		Header struct {
			Title struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Body struct {
			Elements []struct {
				Tag  string `json:"tag"`
				Text struct {
					Tag     string `json:"tag"`
					Content string `json:"content"`
				} `json:"text"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(encoded, &card); err != nil {
		t.Fatal(err)
	}
	if card.Schema != "2.0" || card.Header.Title.Tag != "plain_text" || card.Header.Title.Content != "已绑定 GitHub 账号" {
		t.Fatalf("header=%s", encoded)
	}
	if len(card.Body.Elements) != 1 || card.Body.Elements[0].Tag != "div" || card.Body.Elements[0].Text.Tag != "plain_text" || card.Body.Elements[0].Text.Content != "GitHub 用户名：<script>OctoCat</script>" {
		t.Fatalf("body=%s", encoded)
	}
}

func TestFormCardIsCardJSONTwoForm(t *testing.T) {
	encoded, err := json.Marshal(FormCard())
	if err != nil {
		t.Fatal(err)
	}
	var card struct {
		Schema string `json:"schema"`
		Body   struct {
			Elements []struct {
				Tag      string `json:"tag"`
				Name     string `json:"name"`
				Elements []struct {
					Tag            string `json:"tag"`
					Name           string `json:"name"`
					InputType      string `json:"input_type"`
					Required       bool   `json:"required"`
					FormActionType string `json:"form_action_type"`
					Behaviors      []struct {
						Type  string `json:"type"`
						Value struct {
							Action string `json:"action"`
						} `json:"value"`
					} `json:"behaviors"`
				} `json:"elements"`
			} `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal(encoded, &card); err != nil {
		t.Fatal(err)
	}
	if card.Schema != "2.0" || len(card.Body.Elements) != 1 || card.Body.Elements[0].Tag != "form" {
		t.Fatalf("card=%s", encoded)
	}
	elements := card.Body.Elements[0].Elements
	if len(elements) != 2 || elements[0].Tag != "input" || elements[0].Name != GitHubIDField || elements[0].InputType != "text" || !elements[0].Required {
		t.Fatalf("input=%+v", elements)
	}
	if elements[1].Tag != "button" || elements[1].Name != SubmitGitHubIDAction || elements[1].FormActionType != "submit" || len(elements[1].Behaviors) != 1 || elements[1].Behaviors[0].Type != "callback" || elements[1].Behaviors[0].Value.Action != SubmitGitHubIDAction {
		t.Fatalf("button=%+v", elements[1])
	}
}
