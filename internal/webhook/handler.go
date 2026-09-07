package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
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
		h.metrics.Rejected()
		http.Error(w, "webhook capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBodyBytes+1))
	if err != nil {
		h.metrics.Rejected()
		http.Error(w, "cannot read request body", http.StatusBadRequest)
		return
	}
	if int64(len(body)) > h.maxBodyBytes {
		h.metrics.Rejected()
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if !validSignature(r.Header.Get("X-Hub-Signature-256"), body, h.secret) {
		h.metrics.VerificationFailed()
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	deliveryID, event := r.Header.Get("X-GitHub-Delivery"), r.Header.Get("X-GitHub-Event")
	if !validDeliveryID(deliveryID) || !validEvent(event) || !json.Valid(body) {
		h.metrics.Rejected()
		http.Error(w, "missing GitHub delivery/event header or invalid JSON", http.StatusBadRequest)
		return
	}
	state, found, err := h.store.LookupDelivery(deliveryID, event, body)
	if err != nil {
		h.metrics.Rejected()
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
			h.metrics.Rejected()
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
					h.metrics.Rejected()
					http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
					return
				}
			}
			h.metrics.Duplicate()
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}
	targets, err := h.deliveryTargets(body)
	if err != nil {
		h.metrics.Rejected()
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	reservation, err := h.archive.Reserve(archive.AdmissionRequired(uint64(len(body))))
	if err != nil {
		h.metrics.Rejected()
		http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
		return
	}
	defer reservation.Release()
	var duplicate bool
	if archiveOnlyEvent(event) {
		duplicate, err = h.store.AdmitArchiveOnly(deliveryID, event, body)
	} else {
		duplicate, err = h.store.AdmitToChats(deliveryID, event, body, targets, time.Now(), h.dedupeTTL)
	}
	if err != nil {
		h.metrics.Rejected()
		if errors.Is(err, store.ErrDeliveryConflict) {
			http.Error(w, "delivery conflicts with permanent state", http.StatusConflict)
			return
		}
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	if _, err := reservation.Store(event, deliveryID, body); err != nil {
		h.metrics.Rejected()
		http.Error(w, "archive unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := h.store.FinalizeArchive(deliveryID); err != nil {
		h.metrics.Rejected()
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	if duplicate {
		h.metrics.Duplicate()
	} else {
		h.metrics.Received()
		h.metrics.EventReceived(event)
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) Wait() { h.wg.Wait() }

// workflow_job, check_suite, and status are retained in the permanent archive but are not forwarded.
func archiveOnlyEvent(event string) bool {
	return event == "workflow_job" || event == "check_suite" || event == "status"
}

func validDeliveryID(value string) bool { return validHeader(value, 128, true) }
func validEvent(value string) bool      { return validHeader(value, 64, false) }
func validHeader(value string, max int, allowDot bool) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || allowDot && r == '.' {
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

func repositoryName(body []byte) string {
	var payload struct {
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return strings.ToLower(payload.Repository.FullName)
}

func validSignature(header string, body, secret []byte) bool {
	const prefix = "sha256="
	if len(header) != len(prefix)+sha256.Size*2 || !strings.HasPrefix(header, prefix) {
		return false
	}
	provided, err := hex.DecodeString(header[len(prefix):])
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(mac.Sum(nil), provided)
}
