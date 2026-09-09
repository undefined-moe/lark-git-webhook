// Package gitlabwebhook receives GitLab webhook deliveries (project, group,
// and system hooks) and feeds them through the same durable store, permanent
// archive, and delivery queue used for GitHub events. Push, tag push,
// pipeline, and merge request events are queued for Lark delivery; every other
// GitLab event type is authenticated, validated, and permanently archived but
// never forwarded. System hooks label every delivery with the same
// X-Gitlab-Event header ("System Hook"), so their real event type is read from
// the payload body; supported system-hook kinds (push, tag push, merge
// request) are forwarded exactly like the equivalent project-webhook events.
package gitlabwebhook

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/metrics"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

// Event labels queued for Lark delivery. The "gitlab:" prefix lets the worker
// dispatch GitLab events and attribute their metrics independently of GitHub.
const (
	eventPush         = "gitlab:push"
	eventTagPush      = "gitlab:tag_push"
	eventPipeline     = "gitlab:pipeline"
	eventMergeRequest = "gitlab:merge_request"
	eventLabelPrefix  = "gitlab:"
	eventHeaderMax    = 64
	deliveryIDMax     = 128
	// systemHookHeader is the normalized X-Gitlab-Event value GitLab sends for
	// every system-hook delivery regardless of the underlying event type.
	systemHookHeader = "system_hook"
)

// deliveredEvents are the GitLab event types forwarded to Lark; any other
// validated event is admitted archive-only.
var deliveredEvents = map[string]bool{
	eventPush: true, eventTagPush: true, eventPipeline: true, eventMergeRequest: true,
}

type Handler struct {
	store        *store.Store
	archive      *archive.Archive
	secret       []byte
	defaultChat  string
	maxBodyBytes int64
	dedupeTTL    time.Duration
	metrics      *metrics.Metrics
	inFlight     chan struct{}
	wg           sync.WaitGroup
}

func New(s *store.Store, a *archive.Archive, secret, defaultChat string, maxBodyBytes int64, dedupeTTL time.Duration, maxInFlight int, m *metrics.Metrics) *Handler {
	return &Handler{store: s, archive: a, secret: []byte(secret), defaultChat: defaultChat, maxBodyBytes: maxBodyBytes, dedupeTTL: dedupeTTL, metrics: m, inFlight: make(chan struct{}, maxInFlight)}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.wg.Add(1)
	defer h.wg.Done()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	select {
	case h.inFlight <- struct{}{}:
		defer func() { <-h.inFlight }()
	default:
		h.metrics.GitlabRejected()
		http.Error(w, "webhook capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBodyBytes+1))
	if err != nil {
		h.metrics.GitlabRejected()
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > h.maxBodyBytes {
		h.metrics.GitlabRejected()
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if !h.validToken(r.Header.Get("X-Gitlab-Token")) {
		h.metrics.GitlabVerificationFailed()
		http.Error(w, "invalid secret token", http.StatusUnauthorized)
		return
	}
	event, err := eventLabel(r.Header.Get("X-Gitlab-Event"))
	if err != nil {
		h.metrics.GitlabRejected()
		http.Error(w, "missing or invalid X-Gitlab-Event header", http.StatusBadRequest)
		return
	}
	if event == eventLabelPrefix+systemHookHeader {
		event = systemHookEvent(body, event)
	}
	deliveryID := deliveryIDFor(r.Header.Get("X-Gitlab-Event-UUID"), event, body)
	if !json.Valid(body) {
		h.metrics.GitlabRejected()
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}
	state, found, err := h.store.LookupDelivery(deliveryID, event, body)
	if err != nil {
		h.metrics.GitlabRejected()
		if errors.Is(err, store.ErrDeliveryConflict) {
			http.Error(w, "delivery conflicts with permanent state", http.StatusConflict)
			return
		}
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	if found {
		exists, err := h.archive.VerifyExisting(event, deliveryID, body)
		if err != nil {
			h.metrics.GitlabRejected()
			if errors.Is(err, archive.ErrConflict) {
				http.Error(w, "delivery conflicts with permanent archive", http.StatusConflict)
				return
			}
			http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
			return
		}
		if exists {
			if !state.Archived {
				if err := h.store.FinalizeArchive(deliveryID); err != nil {
					h.metrics.GitlabRejected()
					http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
					return
				}
			}
			h.metrics.GitlabDuplicate()
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}
	reservation, err := h.archive.Reserve(archive.AdmissionRequired(uint64(len(body))))
	if err != nil {
		h.metrics.GitlabRejected()
		http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
		return
	}
	defer reservation.Release()
	var duplicate bool
	if deliveredEvents[event] {
		targets, err := h.deliveryTargets(body)
		if err != nil {
			h.metrics.GitlabRejected()
			http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
			return
		}
		duplicate, err = h.store.AdmitToChats(deliveryID, event, body, targets, time.Now(), h.dedupeTTL)
	} else {
		duplicate, err = h.store.AdmitArchiveOnly(deliveryID, event, body)
	}
	if err != nil {
		h.metrics.GitlabRejected()
		if errors.Is(err, store.ErrDeliveryConflict) {
			http.Error(w, "delivery conflicts with permanent state", http.StatusConflict)
			return
		}
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	if _, err := reservation.Store(event, deliveryID, body); err != nil {
		h.metrics.GitlabRejected()
		http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := h.store.FinalizeArchive(deliveryID); err != nil {
		h.metrics.GitlabRejected()
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	if duplicate {
		h.metrics.GitlabDuplicate()
	} else {
		h.metrics.GitlabReceived()
		h.metrics.GitlabEventReceived(event)
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) Wait() { h.wg.Wait() }

// validToken accepts the X-Gitlab-Token secret token. When no secret is
// configured, deliveries are accepted without a token so operators may enable
// GitLab webhooks without a shared secret; when one is configured the
// comparison is constant time.
func (h *Handler) validToken(provided string) bool {
	if len(h.secret) == 0 {
		return true
	}
	if provided == "" || len(h.secret) != len(provided) {
		return false
	}
	return subtle.ConstantTimeCompare(h.secret, []byte(provided)) == 1
}

// eventLabel maps a validated X-Gitlab-Event header to the canonical queue
// label. Event headers are GitLab's "<Event> Hook" names, so normalization is
// case-insensitive and tolerates the spaces GitLab sends.
func eventLabel(header string) (string, error) {
	if header == "" || len(header) > eventHeaderMax {
		return "", errors.New("missing GitLab event header")
	}
	normalized := make([]byte, 0, len(header))
	space := false
	for i := 0; i < len(header); i++ {
		c := header[i]
		switch {
		case c >= 'a' && c <= 'z':
			normalized, space = append(normalized, c), false
		case c >= 'A' && c <= 'Z':
			normalized, space = append(normalized, c+('a'-'A')), false
		case c >= '0' && c <= '9':
			normalized, space = append(normalized, c), false
		case c == ' ' || c == '-' || c == '_':
			if space {
				continue
			}
			normalized, space = append(normalized, '_'), true
		default:
			return "", errors.New("invalid GitLab event header")
		}
	}
	name := strings.Trim(string(normalized), "_")
	if name == "" {
		return "", errors.New("invalid GitLab event header")
	}
	for _, known := range []string{"push", "tag_push", "pipeline", "merge_request"} {
		if name == known || name == known+"_hook" {
			return eventLabelPrefix + known, nil
		}
	}
	return eventLabelPrefix + name, nil
}

// systemHookEvent maps a System Hook payload to the canonical delivered-event
// label. GitLab sends every system-hook delivery with the same X-Gitlab-Event
// header value, so the underlying type must come from the body: object_kind
// for merge requests and pipeline-shaped payloads, event_name for pushes and
// tag pushes (both fields are present on push payloads). Supported kinds
// return the same labels as project webhooks so they are forwarded to Lark;
// all other system events keep the caller-provided fallback (the archive-only
// system_hook label).
func systemHookEvent(body []byte, fallback string) string {
	var payload struct {
		ObjectKind string `json:"object_kind"`
		EventName  string `json:"event_name"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return fallback
	}
	kind := payload.ObjectKind
	if kind == "" {
		kind = payload.EventName
	}
	switch kind {
	case "push":
		return eventPush
	case "tag_push":
		return eventTagPush
	case "pipeline":
		return eventPipeline
	case "merge_request":
		return eventMergeRequest
	}
	return fallback
}

// deliveryIDFor prefers GitLab's X-Gitlab-Event-UUID when it is present and
// safe; otherwise it derives a stable ID from the event label and the body so
// identical redeliveries without a UUID dedupe against the permanent archive.
func deliveryIDFor(uuid, event string, body []byte) string {
	if validDeliveryID(uuid) {
		return uuid
	}
	digest := sha256.Sum256(append(append([]byte(event), 0), body...))
	return hex.EncodeToString(digest[:])
}

func validDeliveryID(value string) bool {
	if value == "" || len(value) > deliveryIDMax {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func (h *Handler) deliveryTargets(body []byte) ([]string, error) {
	targets := []string{h.defaultChat}
	repository := repositoryName(body)
	if repository == "" {
		return targets, nil
	}
	subscribers, err := h.store.RepositorySubscribers(repository)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{h.defaultChat: true}
	for _, chatID := range subscribers {
		if !seen[chatID] {
			targets, seen[chatID] = append(targets, chatID), true
		}
	}
	return targets, nil
}

// repositoryName normalizes the GitLab project path (project.path_with_namespace
// lowercased) so repository subscribers match the shared owner/repo scheme.
func repositoryName(body []byte) string {
	var payload struct {
		Project struct {
			PathWithNamespace string `json:"path_with_namespace"`
		} `json:"project"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return strings.ToLower(payload.Project.PathWithNamespace)
}
