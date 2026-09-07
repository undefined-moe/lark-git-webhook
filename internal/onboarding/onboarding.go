// Package onboarding defines the Lark onboarding event, callback, and form protocol.
package onboarding

import (
	"encoding/json"
	"strings"
)

const (
	P2PChatEnteredEventType = "im.chat.access_event.bot_p2p_chat_entered_v1"
	CardActionTriggerEvent  = "card.action.trigger"
	SubmitGitHubIDAction    = "submit_github_id"
	GitHubIDField           = "github_id"
)

// EnteredEvent is the portion of a P2P chat-entered event used for onboarding.
type EnteredEvent struct {
	OpenID string
	ChatID string
}

// Callback is the portion of a Card callback used for onboarding.
type Callback struct {
	OpenID        string
	ActionName    string
	ActionValue   string
	GitHubID      string
	OpenMessageID string
}

// ParseEnteredEvent reads the operator open ID and chat ID from a Lark event payload.
func ParseEnteredEvent(body []byte) (EnteredEvent, error) {
	var payload struct {
		Event struct {
			OperatorID struct {
				OpenID string `json:"open_id"`
			} `json:"operator_id"`
			ChatID string `json:"chat_id"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return EnteredEvent{}, err
	}
	return EnteredEvent{OpenID: payload.Event.OperatorID.OpenID, ChatID: payload.Event.ChatID}, nil
}

// ParseCallback reads the fields needed to validate an onboarding Card callback.
func ParseCallback(body []byte) (Callback, error) {
	var payload struct {
		Event struct {
			Operator struct {
				OpenID string `json:"open_id"`
			} `json:"operator"`
			Action struct {
				Name  string `json:"name"`
				Value struct {
					Action string `json:"action"`
				} `json:"value"`
				FormValue map[string]string `json:"form_value"`
			} `json:"action"`
			Context struct {
				OpenMessageID string `json:"open_message_id"`
			} `json:"context"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Callback{}, err
	}
	return Callback{
		OpenID:        payload.Event.Operator.OpenID,
		ActionName:    payload.Event.Action.Name,
		ActionValue:   payload.Event.Action.Value.Action,
		GitHubID:      payload.Event.Action.FormValue[GitHubIDField],
		OpenMessageID: payload.Event.Context.OpenMessageID,
	}, nil
}

// IsOnboarding reports whether a callback is the GitHub onboarding form action.
func (c Callback) IsOnboarding() bool {
	return c.ActionName == SubmitGitHubIDAction && c.ActionValue == SubmitGitHubIDAction
}

// ValidateGitHubLogin trims and validates GitHub's 1-39 character login syntax.
func ValidateGitHubLogin(login string) (string, bool) {
	login = strings.TrimSpace(login)
	if len(login) == 0 || len(login) > 39 || login[0] == '-' || login[len(login)-1] == '-' {
		return "", false
	}
	previousHyphen := false
	for i := 0; i < len(login); i++ {
		c := login[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
			return "", false
		}
		if c == '-' && previousHyphen {
			return "", false
		}
		previousHyphen = c == '-'
	}
	return login, true
}

// FormCard returns the Card JSON 2.0 GitHub login form.
func FormCard() map[string]any {
	return map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"title": map[string]string{"tag": "plain_text", "content": "绑定 GitHub 账号"},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":  "form",
					"name": "github_onboarding",
					"elements": []any{
						map[string]any{
							"tag":         "input",
							"name":        GitHubIDField,
							"input_type":  "text",
							"required":    true,
							"placeholder": map[string]string{"tag": "plain_text", "content": "GitHub 用户名"},
						},
						map[string]any{
							"tag":              "button",
							"name":             SubmitGitHubIDAction,
							"text":             map[string]string{"tag": "plain_text", "content": "提交"},
							"type":             "primary",
							"form_action_type": "submit",
							"behaviors": []any{
								map[string]any{
									"type":  "callback",
									"value": map[string]string{"action": SubmitGitHubIDAction},
								},
							},
						},
					},
				},
			},
		},
	}
}

// BoundCard returns the read-only Card JSON 2.0 confirmation for a verified login.
func BoundCard(login string) map[string]any {
	return map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"title": map[string]string{"tag": "plain_text", "content": "已绑定 GitHub 账号"},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":  "div",
					"text": map[string]string{"tag": "plain_text", "content": "GitHub 用户名：" + login},
				},
			},
		},
	}
}
