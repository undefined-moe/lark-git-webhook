package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrQueueFull                    = errors.New("queue capacity exceeded")
	ErrDeadLetterFull               = errors.New("dead-letter capacity exceeded")
	ErrNotOpen                      = errors.New("store is closed")
	ErrNotFound                     = errors.New("queue item not found")
	ErrDuplicateItem                = errors.New("duplicate queue item")
	ErrRetryTooLarge                = errors.New("retry update exceeds queue capacity")
	ErrDeliveryConflict             = errors.New("delivery identity conflicts with existing state")
	ErrOnboardingFormConflict       = errors.New("onboarding form message conflicts with existing state")
	ErrOnboardingMessageMismatch    = errors.New("onboarding callback does not own form message")
	ErrOnboardingValidationCooldown = errors.New("onboarding validation is rate limited")
	ErrStatsUnderflow               = errors.New("queue stats underflow")
	ErrStatsOverflow                = errors.New("queue stats overflow")
)

var (
	metaBucket                     = []byte("meta")
	queueBucket                    = []byte("queue")
	dedupeBucket                   = []byte("dedupe")
	dedupeExpiryBucket             = []byte("dedupe_expiry")
	rateBucket                     = []byte("rate_history")
	deadLetterBucket               = []byte("dead_letter")
	deliveryStateBucket            = []byte("delivery_state")
	workflowStateBucket            = []byte("workflow_state")
	onboardingStateBucket          = []byte("onboarding_state")
	onboardingCallbackResultBucket = []byte("onboarding_callback_result")
	auditCheckpointBucket          = []byte("audit_checkpoint")
	auditBootstrapBucket           = []byte("audit_bootstrap")
	subscriptionBucket             = []byte("repository_subscription")
	keyNext                        = []byte("next")
	keyCount                       = []byte("count")
	keyBytes                       = []byte("bytes")
	keyRate                        = []byte("rate")
	keyDedupeCount                 = []byte("dedupe_count")
	keyDeadCount                   = []byte("dead_count")
	keyDeadBytes                   = []byte("dead_bytes")
	keyCooldown                    = []byte("cooldown")
)

type Item struct {
	Sequence    uint64    `json:"sequence"`
	DeliveryID  string    `json:"delivery_id"`
	Event       string    `json:"event"`
	ReceivedAt  time.Time `json:"received_at"`
	RawJSON     []byte    `json:"raw_json"`
	Attempts    int       `json:"attempts"`
	NextAttempt time.Time `json:"next_attempt"`
	LastError   string    `json:"last_error,omitempty"`
	Archived    bool      `json:"archived"`
	ChatIDs     []string  `json:"chat_ids,omitempty"`
}

type DeliveryState struct {
	DeliveryID               string   `json:"-"`
	Event                    string   `json:"event"`
	Digest                   [32]byte `json:"digest"`
	Sequence                 uint64   `json:"sequence"`
	Status                   string   `json:"status"`
	Archived                 bool     `json:"archived"`
	SubscriptionReplyClaimed bool     `json:"subscription_reply_claimed,omitempty"`
	RawJSON                  []byte   `json:"raw_json,omitempty"`
}

type DeadItem struct {
	Item     Item      `json:"item"`
	FailedAt time.Time `json:"failed_at"`
}

// WorkflowState is the durable rendering state for one GitHub check suite card.
type WorkflowState struct {
	Key              string                   `json:"-"`
	ChatID           string                   `json:"chat_id,omitempty"`
	RepositoryID     string                   `json:"repository_id"`
	Repository       string                   `json:"repository"`
	SuiteID          string                   `json:"suite_id"`
	RunID            int64                    `json:"run_id,omitempty"`
	RunAttempt       int                      `json:"run_attempt,omitempty"`
	AttemptStartedAt time.Time                `json:"attempt_started_at,omitempty"`
	WorkflowName     string                   `json:"workflow_name"`
	URL              string                   `json:"url"`
	HeadBranch       string                   `json:"head_branch,omitempty"`
	HeadSHA          string                   `json:"head_sha,omitempty"`
	Status           string                   `json:"status"`
	Conclusion       string                   `json:"conclusion"`
	UpdatedAt        time.Time                `json:"updated_at"`
	MessageID        string                   `json:"message_id,omitempty"`
	ReactionID       string                   `json:"reaction_id,omitempty"`
	ReactionType     string                   `json:"reaction_type,omitempty"`
	Checks           map[string]WorkflowCheck `json:"checks,omitempty"`
	Revision         uint64                   `json:"revision"`
}

type WorkflowCheck struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	DetailsURL  string    `json:"details_url"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

// OnboardingState is the durable GitHub onboarding state for one Lark user.
type OnboardingState struct {
	OpenID           string    `json:"-"`
	FormMessageID    string    `json:"form_message_id,omitempty"`
	GitHubLogin      string    `json:"github_login,omitempty"`
	LastValidationAt time.Time `json:"last_validation_at,omitempty"`
}

// OnboardingCallbackResult is the durable response for an onboarding callback delivery.
type OnboardingCallbackResult struct {
	ToastType string    `json:"toast_type,omitempty"`
	Content   string    `json:"content,omitempty"`
	PendingAt time.Time `json:"pending_at,omitempty"`
}

type WorkflowUpdate struct {
	ChatID            string
	RepositoryID      string
	Repository        string
	SuiteID           string
	RunID             int64
	RunAttempt        int
	RunStartedAt      time.Time
	WorkflowName      string
	URL               string
	HeadBranch        string
	HeadSHA           string
	Status            string
	Conclusion        string
	UpdatedAt         time.Time
	AuthoritativeTime bool
	Check             *WorkflowCheck
}

type Stats struct {
	Items       uint64
	Bytes       uint64
	DeadLetters uint64
	DeadBytes   uint64
}

type FailureResult struct {
	Retried        int
	DeadLettered   int
	DeadLetterFull bool
}

type Store struct {
	db             *bolt.DB
	maxItems       uint64
	maxBytes       uint64
	dedupeMaxItems uint64
	deadMaxItems   uint64
	deadMaxBytes   uint64
	consumerMu     sync.Mutex
	consumerHeld   bool
}

func Open(path string, maxItems, maxBytes, dedupeMaxItems, deadMaxItems, deadMaxBytes uint64) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt: %w", err)
	}
	s := &Store{db: db, maxItems: maxItems, maxBytes: maxBytes, dedupeMaxItems: dedupeMaxItems, deadMaxItems: deadMaxItems, deadMaxBytes: deadMaxBytes}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{metaBucket, queueBucket, dedupeBucket, dedupeExpiryBucket, rateBucket, deadLetterBucket, deliveryStateBucket, workflowStateBucket, onboardingStateBucket, onboardingCallbackResultBucket, auditCheckpointBucket, auditBootstrapBucket, subscriptionBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		if err := rebuildDedupe(tx, time.Now(), dedupeMaxItems); err != nil {
			return err
		}
		if err := rebuildActiveStats(tx); err != nil {
			return err
		}
		if err := rebuildDeadStats(tx); err != nil {
			return err
		}
		return rebuildDeliveryStates(tx)
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize bbolt: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// AuditCheckpoint returns the durable lower bound for an organization's audit-log poll.
func (s *Store) AuditCheckpoint(org string) (time.Time, bool, error) {
	if s.db == nil {
		return time.Time{}, false, ErrNotOpen
	}
	var checkpoint time.Time
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(auditCheckpointBucket).Get([]byte(org))
		if value == nil {
			return nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, string(value))
		if err != nil {
			return fmt.Errorf("parse audit checkpoint: %w", err)
		}
		checkpoint, found = parsed.UTC(), true
		return nil
	})
	return checkpoint, found, err
}

// SetAuditCheckpoint advances an organization's poll lower bound after all records are durable.
func (s *Store) SetAuditCheckpoint(org string, checkpoint time.Time) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(auditCheckpointBucket).Put([]byte(org), []byte(checkpoint.UTC().Format(time.RFC3339Nano)))
	})
}

// InitializeAuditCheckpoint records an initial checkpoint that must discard prior records from the first overlapping poll.
func (s *Store) InitializeAuditCheckpoint(org string, checkpoint time.Time) error {
	if s.db == nil {
		return ErrNotOpen
	}
	value := []byte(checkpoint.UTC().Format(time.RFC3339Nano))
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(auditCheckpointBucket).Put([]byte(org), value); err != nil {
			return err
		}
		return tx.Bucket(auditBootstrapBucket).Put([]byte(org), value)
	})
}

// AuditBootstrap returns the initial checkpoint that still needs overlap filtering.
func (s *Store) AuditBootstrap(org string) (time.Time, bool, error) {
	if s.db == nil {
		return time.Time{}, false, ErrNotOpen
	}
	var checkpoint time.Time
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(auditBootstrapBucket).Get([]byte(org))
		if value == nil {
			return nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, string(value))
		if err != nil {
			return fmt.Errorf("parse audit bootstrap checkpoint: %w", err)
		}
		checkpoint, found = parsed.UTC(), true
		return nil
	})
	return checkpoint, found, err
}

// AcquireConsumer serializes workers in this process. bbolt's file lock handles other processes.
// AddRepositorySubscription persistently associates a chat with a normalized repository name.
func (s *Store) AddRepositorySubscription(chatID, repository string) error {
	if s.db == nil {
		return ErrNotOpen
	}
	if chatID == "" || repository == "" {
		return errors.New("subscription chat ID and repository are required")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(subscriptionBucket).Put([]byte(repository+"\x00"+chatID), nil)
	})
}

// RepositorySubscribers returns the durable chat subscriptions for a normalized repository name.
func (s *Store) RepositorySubscribers(repository string) ([]string, error) {
	if s.db == nil {
		return nil, ErrNotOpen
	}
	prefix := []byte(repository + "\x00")
	subscribers := make([]string, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(subscriptionBucket).Cursor()
		for key, _ := cursor.Seek(prefix); key != nil && len(key) > len(prefix) && string(key[:len(prefix)]) == string(prefix); key, _ = cursor.Next() {
			subscribers = append(subscribers, string(key[len(prefix):]))
		}
		return nil
	})
	return subscribers, err
}

func (s *Store) AcquireConsumer() (func(), bool) {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()
	if s.consumerHeld {
		return nil, false
	}
	s.consumerHeld = true
	return func() { s.consumerMu.Lock(); s.consumerHeld = false; s.consumerMu.Unlock() }, true
}

// Enqueue atomically deduplicates and persists an item. The bool reports a best-effort duplicate.
func (s *Store) Enqueue(deliveryID, event string, raw []byte, now time.Time, dedupeTTL time.Duration) (bool, error) {
	if s.db == nil {
		return false, ErrNotOpen
	}
	var duplicate bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta, queue, dedupe, expiryIndex := tx.Bucket(metaBucket), tx.Bucket(queueBucket), tx.Bucket(dedupeBucket), tx.Bucket(dedupeExpiryBucket)
		if expiry := dedupe.Get([]byte(deliveryID)); len(expiry) == 8 {
			if decodeTime(expiry).After(now) {
				duplicate = true
				return nil
			}
			if err := dedupe.Delete([]byte(deliveryID)); err != nil {
				return err
			}
			if err := expiryIndex.Delete(dedupeIndexKey(decodeTime(expiry), deliveryID)); err != nil {
				return err
			}
			if err := decrement(meta, keyDedupeCount); err != nil {
				return err
			}
		}
		if err := cleanupDedupe(tx, now, 1000); err != nil {
			return err
		}
		if err := evictDedupe(tx, s.dedupeMaxItems-1); err != nil {
			return err
		}
		count, logicalBytes := getUint(meta.Get(keyCount)), getUint(meta.Get(keyBytes))
		next := getUint(meta.Get(keyNext)) + 1
		item := Item{Sequence: next, DeliveryID: deliveryID, Event: event, ReceivedAt: now.UTC(), RawJSON: append([]byte(nil), raw...), NextAttempt: now.UTC(), Archived: true}
		encoded, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if count >= s.maxItems || logicalBytes > s.maxBytes || activeItemBytes(item) > s.maxBytes-logicalBytes {
			return ErrQueueFull
		}
		expiresAt := now.Add(dedupeTTL).UTC()
		if err := queue.Put(numberKey(next), encoded); err != nil {
			return err
		}
		if err := dedupe.Put([]byte(deliveryID), timeKey(expiresAt)); err != nil {
			return err
		}
		if err := expiryIndex.Put(dedupeIndexKey(expiresAt, deliveryID), []byte(deliveryID)); err != nil {
			return err
		}
		if err := meta.Put(keyNext, numberKey(next)); err != nil {
			return err
		}
		if err := meta.Put(keyCount, numberKey(count+1)); err != nil {
			return err
		}
		if err := meta.Put(keyBytes, numberKey(logicalBytes+activeItemBytes(item))); err != nil {
			return err
		}
		return meta.Put(keyDedupeCount, numberKey(getUint(meta.Get(keyDedupeCount))+1))
	})
	return duplicate, err
}

// Admit persists a delivery and its archive recovery state in one transaction.
func (s *Store) Admit(deliveryID, event string, raw []byte, now time.Time, dedupeTTL time.Duration) (bool, error) {
	return s.admit(deliveryID, event, raw, nil, now, dedupeTTL)
}

// AdmitToChats snapshots delivery targets with the queued GitHub event.
func (s *Store) AdmitToChats(deliveryID, event string, raw []byte, chatIDs []string, now time.Time, dedupeTTL time.Duration) (bool, error) {
	return s.admit(deliveryID, event, raw, chatIDs, now, dedupeTTL)
}

func (s *Store) admit(deliveryID, event string, raw []byte, chatIDs []string, now time.Time, dedupeTTL time.Duration) (bool, error) {
	if s.db == nil {
		return false, ErrNotOpen
	}
	duplicate := false
	digest := sha256.Sum256(raw)
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta, queue, dedupe, expiryIndex, states := tx.Bucket(metaBucket), tx.Bucket(queueBucket), tx.Bucket(dedupeBucket), tx.Bucket(dedupeExpiryBucket), tx.Bucket(deliveryStateBucket)
		if value := states.Get([]byte(deliveryID)); value != nil {
			var state DeliveryState
			if err := json.Unmarshal(value, &state); err != nil {
				return err
			}
			if state.Event != event || state.Digest != digest {
				return ErrDeliveryConflict
			}
			duplicate = true
			return nil
		}
		if err := cleanupDedupe(tx, now, 1000); err != nil {
			return err
		}
		if err := evictDedupe(tx, s.dedupeMaxItems-1); err != nil {
			return err
		}
		count, logicalBytes := getUint(meta.Get(keyCount)), getUint(meta.Get(keyBytes))
		next := getUint(meta.Get(keyNext)) + 1
		item := Item{Sequence: next, DeliveryID: deliveryID, Event: event, ReceivedAt: now.UTC(), RawJSON: append([]byte(nil), raw...), NextAttempt: now.UTC(), Archived: false, ChatIDs: append([]string(nil), chatIDs...)}
		encoded, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if count >= s.maxItems || logicalBytes > s.maxBytes || activeItemBytes(item) > s.maxBytes-logicalBytes {
			return ErrQueueFull
		}
		state, err := json.Marshal(DeliveryState{Event: event, Digest: digest, Sequence: next, Status: "queued", RawJSON: append([]byte(nil), raw...)})
		if err != nil {
			return err
		}
		expiresAt := now.Add(dedupeTTL).UTC()
		if err := queue.Put(numberKey(next), encoded); err != nil {
			return err
		}
		if err := states.Put([]byte(deliveryID), state); err != nil {
			return err
		}
		if err := dedupe.Put([]byte(deliveryID), timeKey(expiresAt)); err != nil {
			return err
		}
		if err := expiryIndex.Put(dedupeIndexKey(expiresAt, deliveryID), []byte(deliveryID)); err != nil {
			return err
		}
		if err := meta.Put(keyNext, numberKey(next)); err != nil {
			return err
		}
		if err := meta.Put(keyCount, numberKey(count+1)); err != nil {
			return err
		}
		if err := meta.Put(keyBytes, numberKey(logicalBytes+activeItemBytes(item))); err != nil {
			return err
		}
		return meta.Put(keyDedupeCount, numberKey(getUint(meta.Get(keyDedupeCount))+1))
	})
	return duplicate, err
}

// FinalizeArchive allows a queued item to be delivered only after its payload is durable.
// AdmitArchiveOnly records a delivery that must be retained but is intentionally not forwarded.
func (s *Store) AdmitArchiveOnly(deliveryID, event string, raw []byte) (bool, error) {
	if s.db == nil {
		return false, ErrNotOpen
	}
	digest := sha256.Sum256(raw)
	duplicate := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		states := tx.Bucket(deliveryStateBucket)
		if value := states.Get([]byte(deliveryID)); value != nil {
			var state DeliveryState
			if err := json.Unmarshal(value, &state); err != nil {
				return err
			}
			if state.Event != event || state.Digest != digest {
				return ErrDeliveryConflict
			}
			duplicate = true
			return nil
		}
		encoded, err := json.Marshal(DeliveryState{Event: event, Digest: digest, Status: "archive_only", RawJSON: append([]byte(nil), raw...)})
		if err != nil {
			return err
		}
		return states.Put([]byte(deliveryID), encoded)
	})
	return duplicate, err
}

func (s *Store) FinalizeArchive(deliveryID string) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, states, queue := tx.Bucket(metaBucket), tx.Bucket(deliveryStateBucket), tx.Bucket(queueBucket)
		value := states.Get([]byte(deliveryID))
		if value == nil {
			return ErrNotFound
		}
		var state DeliveryState
		if err := json.Unmarshal(value, &state); err != nil {
			return err
		}
		if state.Archived {
			return nil
		}
		state.Archived, state.RawJSON = true, nil
		encodedState, err := json.Marshal(state)
		if err != nil {
			return err
		}
		logicalBytes := getUint(meta.Get(keyBytes))
		if itemValue := queue.Get(numberKey(state.Sequence)); itemValue != nil {
			var item Item
			if err := json.Unmarshal(itemValue, &item); err != nil {
				return err
			}
			oldBytes := activeItemBytes(item)
			if logicalBytes < oldBytes {
				return ErrStatsUnderflow
			}
			item.Archived = true
			encodedItem, err := json.Marshal(item)
			if err != nil {
				return err
			}
			newBytes := activeItemBytes(item)
			newLogicalBytes := logicalBytes - oldBytes
			if ^uint64(0)-newLogicalBytes < newBytes {
				return ErrStatsOverflow
			}
			newLogicalBytes += newBytes
			if err := queue.Put(numberKey(state.Sequence), encodedItem); err != nil {
				return err
			}
			if err := meta.Put(keyBytes, numberKey(newLogicalBytes)); err != nil {
				return err
			}
		}
		return states.Put([]byte(deliveryID), encodedState)
	})
}

// LookupDelivery reads the permanent delivery identity without changing queue or archive state.
// ClaimSubscriptionReply records the one allowed confirmation-message attempt for a delivery.
func (s *Store) ClaimSubscriptionReply(deliveryID string) (bool, error) {
	if s.db == nil {
		return false, ErrNotOpen
	}
	claimed := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		states := tx.Bucket(deliveryStateBucket)
		value := states.Get([]byte(deliveryID))
		if value == nil {
			return ErrNotFound
		}
		var state DeliveryState
		if err := json.Unmarshal(value, &state); err != nil {
			return err
		}
		if state.SubscriptionReplyClaimed {
			return nil
		}
		state.SubscriptionReplyClaimed = true
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := states.Put([]byte(deliveryID), encoded); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claimed, err
}

func (s *Store) LookupDelivery(deliveryID, event string, raw []byte) (DeliveryState, bool, error) {
	if s.db == nil {
		return DeliveryState{}, false, ErrNotOpen
	}
	digest := sha256.Sum256(raw)
	var state DeliveryState
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(deliveryStateBucket).Get([]byte(deliveryID))
		if value == nil {
			return nil
		}
		if err := json.Unmarshal(value, &state); err != nil {
			return err
		}
		if state.Event != event || state.Digest != digest {
			return ErrDeliveryConflict
		}
		state.DeliveryID = deliveryID
		found = true
		return nil
	})
	return state, found, err
}

func (s *Store) PendingArchives(limit int) ([]DeliveryState, error) {
	if s.db == nil {
		return nil, ErrNotOpen
	}
	pending := make([]DeliveryState, 0, limit)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(deliveryStateBucket).ForEach(func(key, value []byte) error {
			if len(pending) >= limit {
				return nil
			}
			var state DeliveryState
			if err := json.Unmarshal(value, &state); err != nil {
				return err
			}
			if !state.Archived {
				state.DeliveryID = string(key)
				pending = append(pending, state)
			}
			return nil
		})
	})
	return pending, err
}

func (s *Store) Due(now time.Time, limit int) ([]Item, error) {
	if s.db == nil {
		return nil, ErrNotOpen
	}
	items := make([]Item, 0, limit)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(queueBucket).Cursor()
		for k, v := c.First(); k != nil && len(items) < limit; k, v = c.Next() {
			var item Item
			if err := json.Unmarshal(v, &item); err != nil {
				return fmt.Errorf("decode queue item: %w", err)
			}
			if item.Archived && !item.NextAttempt.After(now) {
				items = append(items, item)
			}
		}
		return nil
	})
	return items, err
}

func (s *Store) Delete(items []Item) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, queue, states := tx.Bucket(metaBucket), tx.Bucket(queueBucket), tx.Bucket(deliveryStateBucket)
		count, logicalBytes := getUint(meta.Get(keyCount)), getUint(meta.Get(keyBytes))
		for _, item := range items {
			value := queue.Get(numberKey(item.Sequence))
			if value == nil {
				continue
			}
			var current Item
			if err := json.Unmarshal(value, &current); err != nil {
				return err
			}
			if err := queue.Delete(numberKey(item.Sequence)); err != nil {
				return err
			}
			if stateValue := states.Get([]byte(current.DeliveryID)); stateValue != nil {
				var state DeliveryState
				if err := json.Unmarshal(stateValue, &state); err != nil {
					return err
				}
				state.Status = "delivered"
				encodedState, err := json.Marshal(state)
				if err != nil {
					return err
				}
				if err := states.Put([]byte(current.DeliveryID), encodedState); err != nil {
					return err
				}
			}
			count--
			logicalBytes -= activeItemBytes(current)
		}
		if err := meta.Put(keyCount, numberKey(count)); err != nil {
			return err
		}
		return meta.Put(keyBytes, numberKey(logicalBytes))
	})
}

// CompleteChatDelivery records a successful send to chatID. An item remains queued
// until every snapshotted target has completed; an empty ChatIDs field is the legacy
// configured-chat target.
func (s *Store) CompleteChatDelivery(items []Item, chatID string) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		queue, states := tx.Bucket(queueBucket), tx.Bucket(deliveryStateBucket)
		for _, item := range items {
			value := queue.Get(numberKey(item.Sequence))
			if value == nil {
				continue
			}
			var current Item
			if err := json.Unmarshal(value, &current); err != nil {
				return err
			}
			remaining := make([]string, 0, len(current.ChatIDs))
			for _, target := range current.ChatIDs {
				if target != chatID {
					remaining = append(remaining, target)
				}
			}
			if len(current.ChatIDs) == 0 && chatID != "" || len(current.ChatIDs) > 0 && len(remaining) == len(current.ChatIDs) {
				continue
			}
			if len(remaining) > 0 {
				current.ChatIDs = remaining
				encoded, err := json.Marshal(current)
				if err != nil {
					return err
				}
				if err := queue.Put(numberKey(current.Sequence), encoded); err != nil {
					return err
				}
				continue
			}
			if err := queue.Delete(numberKey(current.Sequence)); err != nil {
				return err
			}
			if stateValue := states.Get([]byte(current.DeliveryID)); stateValue != nil {
				var state DeliveryState
				if err := json.Unmarshal(stateValue, &state); err != nil {
					return err
				}
				state.Status = "delivered"
				encodedState, err := json.Marshal(state)
				if err != nil {
					return err
				}
				if err := states.Put([]byte(current.DeliveryID), encodedState); err != nil {
					return err
				}
			}
		}
		return rebuildActiveStats(tx)
	})
}

// HandleFailure atomically retries eligible items, dead-letters exhausted/permanent items, and persists a Retry-After cooldown.
func (s *Store) HandleFailure(items []Item, now, retryAt time.Time, reason string, retryAfter time.Duration, maxAttempts int, permanent bool) (FailureResult, error) {
	if s.db == nil {
		return FailureResult{}, ErrNotOpen
	}
	result := FailureResult{}
	reason = trimReason(reason)
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta, queue, dead, states := tx.Bucket(metaBucket), tx.Bucket(queueBucket), tx.Bucket(deadLetterBucket), tx.Bucket(deliveryStateBucket)
		seen := make(map[uint64]struct{}, len(items))
		for _, item := range items {
			if _, ok := seen[item.Sequence]; ok {
				return ErrDuplicateItem
			}
			seen[item.Sequence] = struct{}{}
		}
		current := make([]Item, 0, len(items))
		deadItems, deadBytes := uint64(0), uint64(0)
		for _, item := range items {
			value := queue.Get(numberKey(item.Sequence))
			if value == nil {
				continue
			}
			var stored Item
			if err := json.Unmarshal(value, &stored); err != nil {
				return err
			}
			current = append(current, stored)
			if permanent || stored.Attempts+1 >= maxAttempts {
				prospective := stored
				prospective.Attempts++
				prospective.LastError = reason
				encoded, err := json.Marshal(DeadItem{Item: prospective, FailedAt: now.UTC()})
				if err != nil {
					return err
				}
				deadItems++
				deadBytes += uint64(len(encoded))
			}
		}
		if deadItems > 0 {
			count, bytes := getUint(meta.Get(keyDeadCount)), getUint(meta.Get(keyDeadBytes))
			if count > s.deadMaxItems || bytes > s.deadMaxBytes || deadItems > s.deadMaxItems-count || deadBytes > s.deadMaxBytes-bytes {
				result.DeadLetterFull = true
			}
		}
		// Active-byte accounting is based on immutable item content plus a fixed
		// retry reserve, so persistence of an attempt is never gated by a
		// changing error string or retry timestamp.
		for _, item := range current {
			shouldDead := (permanent || item.Attempts+1 >= maxAttempts) && !result.DeadLetterFull
			item.Attempts++
			item.LastError = reason
			if shouldDead {
				encoded, err := json.Marshal(DeadItem{Item: item, FailedAt: now.UTC()})
				if err != nil {
					return err
				}
				if err := dead.Put(numberKey(item.Sequence), encoded); err != nil {
					return err
				}
				if err := queue.Delete(numberKey(item.Sequence)); err != nil {
					return err
				}
				if stateValue := states.Get([]byte(item.DeliveryID)); stateValue != nil {
					var state DeliveryState
					if err := json.Unmarshal(stateValue, &state); err != nil {
						return err
					}
					state.Status = "dead"
					encodedState, err := json.Marshal(state)
					if err != nil {
						return err
					}
					if err := states.Put([]byte(item.DeliveryID), encodedState); err != nil {
						return err
					}
				}
				result.DeadLettered++
				continue
			}
			item.NextAttempt = retryAt.UTC()
			if result.DeadLetterFull {
				item.LastError = trimReason("dead-letter capacity full: " + reason)
			}
			encoded, err := json.Marshal(item)
			if err != nil {
				return err
			}
			if err := queue.Put(numberKey(item.Sequence), encoded); err != nil {
				return err
			}
			result.Retried++
		}
		if retryAfter > 0 {
			cooldown := now.Add(retryAfter)
			if currentCooldown := decodeTime(meta.Get(keyCooldown)); cooldown.After(currentCooldown) {
				if err := meta.Put(keyCooldown, timeKey(cooldown)); err != nil {
					return err
				}
			}
		}
		if err := rebuildActiveStats(tx); err != nil {
			return err
		}
		return rebuildDeadStats(tx)
	})
	return result, err
}

func (s *Store) RequeueDeadLetter(sequence uint64, now time.Time) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, queue, dead, states := tx.Bucket(metaBucket), tx.Bucket(queueBucket), tx.Bucket(deadLetterBucket), tx.Bucket(deliveryStateBucket)
		value := dead.Get(numberKey(sequence))
		if value == nil {
			return ErrNotFound
		}
		var deadItem DeadItem
		if err := json.Unmarshal(value, &deadItem); err != nil {
			return err
		}
		count, bytes := getUint(meta.Get(keyCount)), getUint(meta.Get(keyBytes))
		deadItem.Item.Attempts, deadItem.Item.NextAttempt, deadItem.Item.LastError = 0, now.UTC(), ""
		encoded, err := json.Marshal(deadItem.Item)
		if err != nil {
			return err
		}
		if count >= s.maxItems || bytes > s.maxBytes || activeItemBytes(deadItem.Item) > s.maxBytes-bytes {
			return ErrQueueFull
		}
		if err := queue.Put(numberKey(sequence), encoded); err != nil {
			return err
		}
		if err := dead.Delete(numberKey(sequence)); err != nil {
			return err
		}
		if stateValue := states.Get([]byte(deadItem.Item.DeliveryID)); stateValue != nil {
			var state DeliveryState
			if err := json.Unmarshal(stateValue, &state); err != nil {
				return err
			}
			state.Sequence, state.Status, state.Archived, state.RawJSON = sequence, "queued", true, nil
			encodedState, err := json.Marshal(state)
			if err != nil {
				return err
			}
			if err := states.Put([]byte(deadItem.Item.DeliveryID), encodedState); err != nil {
				return err
			}
		}
		if err := rebuildActiveStats(tx); err != nil {
			return err
		}
		return rebuildDeadStats(tx)
	})
}

func (s *Store) Stats() (Stats, error) {
	if s.db == nil {
		return Stats{}, ErrNotOpen
	}
	var stats Stats
	err := s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		stats = Stats{Items: getUint(meta.Get(keyCount)), Bytes: getUint(meta.Get(keyBytes)), DeadLetters: getUint(meta.Get(keyDeadCount)), DeadBytes: getUint(meta.Get(keyDeadBytes))}
		return nil
	})
	return stats, err
}
func (s *Store) OldestAge(now time.Time) (time.Duration, error) {
	if s.db == nil {
		return 0, ErrNotOpen
	}
	var received time.Time
	err := s.db.View(func(tx *bolt.Tx) error {
		k, v := tx.Bucket(queueBucket).Cursor().First()
		if k == nil {
			return nil
		}
		var item Item
		if err := json.Unmarshal(v, &item); err != nil {
			return err
		}
		received = item.ReceivedAt
		return nil
	})
	if err != nil || received.IsZero() {
		return 0, err
	}
	return now.Sub(received), nil
}
func (s *Store) CleanupDedupe(now time.Time, limit int) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error { return cleanupDedupe(tx, now, limit) })
}

// ReserveRequest persists an HTTP request slot before the caller sends it. A positive wait means no slot was reserved.
func (s *Store) ReserveRequest(now time.Time) (time.Duration, error) {
	if s.db == nil {
		return 0, ErrNotOpen
	}
	var wait time.Duration
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta, rate := tx.Bucket(metaBucket), tx.Bucket(rateBucket)
		if cooldown := decodeTime(meta.Get(keyCooldown)); cooldown.After(now) {
			wait = cooldown.Sub(now)
			return nil
		}
		oneSecond, oneMinute := now.Add(-time.Second), now.Add(-time.Minute)
		var recentSecond, recentMinute []time.Time
		c := rate.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			t := decodeTime(v)
			if !t.After(oneMinute) {
				if err := c.Delete(); err != nil {
					return err
				}
				continue
			}
			recentMinute = append(recentMinute, t)
			if t.After(oneSecond) {
				recentSecond = append(recentSecond, t)
			}
		}
		if len(recentSecond) >= 5 {
			wait = recentSecond[0].Add(time.Second).Sub(now)
			return nil
		}
		if len(recentMinute) >= 100 {
			wait = recentMinute[0].Add(time.Minute).Sub(now)
			return nil
		}
		n := getUint(meta.Get(keyRate)) + 1
		key := append(timeKey(now), numberKey(n)...)
		if err := rate.Put(key, timeKey(now)); err != nil {
			return err
		}
		return meta.Put(keyRate, numberKey(n))
	})
	return wait, err
}
func (s *Store) Writable() error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(*bolt.Tx) error { return nil })
}
func (s *Store) FileSize() int64 {
	if s.db == nil {
		return 0
	}
	info, err := os.Stat(s.db.Path())
	if err != nil {
		return 0
	}
	return info.Size()
}

// UpsertWorkflow merges a workflow or check-run delivery into its suite state. A
// suite is identified by GitHub's immutable repository ID and check-suite ID, not
// by its mutable full_name. Events without a timestamp never regress a terminal
// state, but can advance an in-progress state.
func (s *Store) UpsertWorkflow(update WorkflowUpdate) (WorkflowState, error) {
	if s.db == nil {
		return WorkflowState{}, ErrNotOpen
	}
	if update.Repository == "" || update.SuiteID == "" {
		return WorkflowState{}, errors.New("workflow repository and suite ID are required")
	}
	if update.RepositoryID == "" {
		update.RepositoryID = update.Repository
	}
	key := update.RepositoryID + "\x00" + update.SuiteID
	if update.ChatID != "" {
		key = update.ChatID + "\x00" + key
	}
	var state WorkflowState
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(workflowStateBucket)
		if value := bucket.Get([]byte(key)); value != nil {
			if err := json.Unmarshal(value, &state); err != nil {
				return err
			}
		} else {
			state = WorkflowState{ChatID: update.ChatID, RepositoryID: update.RepositoryID, Repository: update.Repository, SuiteID: update.SuiteID, Checks: make(map[string]WorkflowCheck)}
		}
		if state.Checks == nil {
			state.Checks = make(map[string]WorkflowCheck)
		}
		accept := true
		at := update.UpdatedAt.UTC()
		hasTime := update.AuthoritativeTime || !at.IsZero()
		if hasTime {
			accept = !at.Before(state.UpdatedAt)
		} else if isWorkflowTerminal(state.Status, state.Conclusion) && !isWorkflowTerminal(update.Status, update.Conclusion) && update.RunAttempt <= state.RunAttempt {
			accept = false
		}
		// A higher GitHub run attempt is a new execution and therefore takes
		// precedence over a timestamp from the prior attempt.
		if update.RunAttempt > state.RunAttempt {
			accept = true
		}
		if accept {
			if update.Repository != "" {
				state.Repository = update.Repository
			}
			if update.WorkflowName != "" {
				state.WorkflowName = update.WorkflowName
			}
			if update.URL != "" {
				state.URL = update.URL
			}
			if update.HeadBranch != "" {
				state.HeadBranch = update.HeadBranch
			}
			if update.HeadSHA != "" {
				state.HeadSHA = update.HeadSHA
			}
			if update.RunID != 0 {
				state.RunID = update.RunID
			}
			if update.RunAttempt > state.RunAttempt {
				previousAttempt := state.RunAttempt
				state.RunAttempt = update.RunAttempt
				state.AttemptStartedAt = update.RunStartedAt.UTC()
				if state.AttemptStartedAt.IsZero() && hasTime {
					state.AttemptStartedAt = at
				}
				// Checks can arrive before the first workflow_run delivery, but a
				// later attempt must start from an empty check set.
				if previousAttempt > 0 {
					state.Checks = make(map[string]WorkflowCheck)
				}
				state.Conclusion = ""
			}
			if update.Status != "" {
				state.Status = update.Status
			}
			if update.Conclusion != "" || update.Status == "completed" {
				state.Conclusion = update.Conclusion
			}
			if hasTime {
				state.UpdatedAt = at
			}
		}
		if update.Check != nil && update.Check.ID != "" {
			check := *update.Check
			checkAt := workflowCheckTime(check)
			// A check event has no run_attempt. Once attempt two or later is
			// active, only a timestamp at/after its run cutoff can identify it as
			// belonging to that attempt. Keep attempt-one check-first delivery.
			validAttempt := !(state.RunAttempt > 1 && checkAt.IsZero())
			if !checkAt.IsZero() && !state.AttemptStartedAt.IsZero() && checkAt.Before(state.AttemptStartedAt) {
				validAttempt = false
			}
			if validAttempt {
				old, found := state.Checks[check.ID]
				oldAt := workflowCheckTime(old)
				if !found || (!checkAt.IsZero() && !checkAt.Before(oldAt)) || (!isWorkflowTerminal(old.Status, old.Conclusion) && isWorkflowTerminal(check.Status, check.Conclusion)) {
					state.Checks[check.ID] = check
				}
			}
		}
		state.Revision++
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), encoded)
	})
	state.Key = key
	return state, err
}

func isWorkflowTerminal(status, conclusion string) bool {
	return status == "completed" || conclusion != ""
}
func workflowCheckTime(check WorkflowCheck) time.Time {
	if !check.CompletedAt.IsZero() {
		return check.CompletedAt.UTC()
	}
	if !check.StartedAt.IsZero() {
		return check.StartedAt.UTC()
	}
	return check.UpdatedAt.UTC()
}

func (s *Store) SetWorkflowMessage(key, messageID string) error {
	return s.updateWorkflow(key, func(state *WorkflowState) {
		state.MessageID = messageID
	})
}

func (s *Store) SetWorkflowReaction(key, reactionID, reactionType string) error {
	return s.updateWorkflow(key, func(state *WorkflowState) {
		state.ReactionID, state.ReactionType = reactionID, reactionType
	})
}

func (s *Store) updateWorkflow(key string, update func(*WorkflowState)) error {
	if s.db == nil {
		return ErrNotOpen
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(workflowStateBucket)
		value := bucket.Get([]byte(key))
		if value == nil {
			return ErrNotFound
		}
		var state WorkflowState
		if err := json.Unmarshal(value, &state); err != nil {
			return err
		}
		update(&state)
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), encoded)
	})
}

func (s *Store) Workflow(key string) (WorkflowState, error) {
	if s.db == nil {
		return WorkflowState{}, ErrNotOpen
	}
	var state WorkflowState
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(workflowStateBucket).Get([]byte(key))
		if value == nil {
			return ErrNotFound
		}
		return json.Unmarshal(value, &state)
	})
	state.Key = key
	return state, err
}

// Onboarding returns the durable onboarding state for openID.
func (s *Store) Onboarding(openID string) (OnboardingState, error) {
	if s.db == nil {
		return OnboardingState{}, ErrNotOpen
	}
	var state OnboardingState
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(onboardingStateBucket).Get([]byte(openID))
		if value == nil {
			return ErrNotFound
		}
		return json.Unmarshal(value, &state)
	})
	state.OpenID = openID
	return state, err
}

// SetOnboardingForm records the first form message sent to openID. Repeating the
// same message ID is idempotent; a different message ID cannot replace it.
func (s *Store) SetOnboardingForm(openID, messageID string) error {
	if s.db == nil {
		return ErrNotOpen
	}
	if openID == "" || messageID == "" {
		return errors.New("onboarding open ID and form message ID are required")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(onboardingStateBucket)
		var state OnboardingState
		if value := bucket.Get([]byte(openID)); value != nil {
			if err := json.Unmarshal(value, &state); err != nil {
				return err
			}
			if state.FormMessageID != "" {
				if state.FormMessageID == messageID {
					return nil
				}
				return ErrOnboardingFormConflict
			}
		}
		state.FormMessageID = messageID
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(openID), encoded)
	})
}

// SetOnboardingGitHubLogin stores a verified login only when the callback owns
// the form message previously sent to openID.
func (s *Store) SetOnboardingGitHubLogin(openID, expectedMessageID, login string) error {
	return s.SetOnboardingGitHubLoginContext(context.Background(), openID, expectedMessageID, login)
}

// SetOnboardingGitHubLoginContext aborts a late callback before mutating its mapping.
func (s *Store) SetOnboardingGitHubLoginContext(ctx context.Context, openID, expectedMessageID, login string) error {
	if s.db == nil {
		return ErrNotOpen
	}
	if openID == "" || expectedMessageID == "" || login == "" {
		return errors.New("onboarding open ID, form message ID, and GitHub login are required")
	}
	if err := contextCommitErr(ctx); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		bucket := tx.Bucket(onboardingStateBucket)
		value := bucket.Get([]byte(openID))
		if value == nil {
			return ErrNotFound
		}
		var state OnboardingState
		if err := json.Unmarshal(value, &state); err != nil {
			return err
		}
		if state.FormMessageID == "" {
			return ErrNotFound
		}
		if state.FormMessageID != expectedMessageID {
			return ErrOnboardingMessageMismatch
		}
		state.GitHubLogin = login
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		return bucket.Put([]byte(openID), encoded)
	})
}

// ReserveOnboardingValidation atomically permits one external GitHub validation
// per user per minute while confirming the submitted form ownership.
func (s *Store) ReserveOnboardingValidation(openID, expectedMessageID string, now time.Time) (bool, error) {
	return s.ReserveOnboardingValidationContext(context.Background(), openID, expectedMessageID, now)
}

// ReserveOnboardingValidationContext never records a late validation reservation.
func (s *Store) ReserveOnboardingValidationContext(ctx context.Context, openID, expectedMessageID string, now time.Time) (bool, error) {
	if s.db == nil {
		return false, ErrNotOpen
	}
	if err := contextCommitErr(ctx); err != nil {
		return false, err
	}
	reserved := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		bucket := tx.Bucket(onboardingStateBucket)
		value := bucket.Get([]byte(openID))
		if value == nil {
			return ErrNotFound
		}
		var state OnboardingState
		if err := json.Unmarshal(value, &state); err != nil {
			return err
		}
		if state.FormMessageID == "" {
			return ErrNotFound
		}
		if state.FormMessageID != expectedMessageID {
			return ErrOnboardingMessageMismatch
		}
		if state.LastValidationAt.Add(time.Minute).After(now) {
			return ErrOnboardingValidationCooldown
		}
		state.LastValidationAt = now.UTC()
		encoded, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		if err := bucket.Put([]byte(openID), encoded); err != nil {
			return err
		}
		reserved = true
		return nil
	})
	return reserved, err
}

// OnboardingCallbackResult returns a cached callback outcome by qualified delivery ID.
func (s *Store) OnboardingCallbackResult(deliveryID string) (OnboardingCallbackResult, bool, error) {
	if s.db == nil {
		return OnboardingCallbackResult{}, false, ErrNotOpen
	}
	var result OnboardingCallbackResult
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(onboardingCallbackResultBucket).Get([]byte(deliveryID))
		if value == nil {
			return nil
		}
		if err := json.Unmarshal(value, &result); err != nil {
			return err
		}
		found = true
		return nil
	})
	return result, found, err
}

// ClaimOnboardingCallbackResult atomically claims one delivery for validation.
// A pending claim may be recovered after the callback response budget expires.
func (s *Store) ClaimOnboardingCallbackResult(deliveryID string, now time.Time, staleAfter time.Duration) (OnboardingCallbackResult, bool, error) {
	return s.ClaimOnboardingCallbackResultContext(context.Background(), deliveryID, now, staleAfter)
}

// ClaimOnboardingCallbackResultContext never creates a late pending claim.
func (s *Store) ClaimOnboardingCallbackResultContext(ctx context.Context, deliveryID string, now time.Time, staleAfter time.Duration) (OnboardingCallbackResult, bool, error) {
	if s.db == nil {
		return OnboardingCallbackResult{}, false, ErrNotOpen
	}
	if err := contextCommitErr(ctx); err != nil {
		return OnboardingCallbackResult{}, false, err
	}
	var result OnboardingCallbackResult
	claimed := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		bucket := tx.Bucket(onboardingCallbackResultBucket)
		if value := bucket.Get([]byte(deliveryID)); value != nil {
			if err := json.Unmarshal(value, &result); err != nil {
				return err
			}
			if result.ToastType != "" && result.Content != "" {
				return nil
			}
			if result.PendingAt.Add(staleAfter).After(now) {
				return nil
			}
		}
		result = OnboardingCallbackResult{PendingAt: now.UTC()}
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		if err := bucket.Put([]byte(deliveryID), encoded); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return result, claimed, err
}

// SetOnboardingCallbackResult durably stores a callback outcome for replay.
func (s *Store) SetOnboardingCallbackResult(deliveryID string, result OnboardingCallbackResult) error {
	return s.SetOnboardingCallbackResultContext(context.Background(), deliveryID, result)
}

// SetOnboardingCallbackResultContext never publishes a callback outcome after its deadline.
func (s *Store) SetOnboardingCallbackResultContext(ctx context.Context, deliveryID string, result OnboardingCallbackResult) error {
	if s.db == nil {
		return ErrNotOpen
	}
	if deliveryID == "" || result.ToastType == "" || result.Content == "" {
		return errors.New("onboarding callback result is required")
	}
	if err := contextCommitErr(ctx); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if err := contextCommitErr(ctx); err != nil {
			return err
		}
		return tx.Bucket(onboardingCallbackResultBucket).Put([]byte(deliveryID), encoded)
	})
}

// callbackCommitGuard leaves time for bbolt's synchronous commit after the last
// pre-Put context check. Callback callers use a 2.5 second response deadline.
const callbackCommitGuard = 500 * time.Millisecond

func contextCommitErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= callbackCommitGuard {
		return context.DeadlineExceeded
	}
	return nil
}

func rebuildDeliveryStates(tx *bolt.Tx) error {
	states, queue, dead := tx.Bucket(deliveryStateBucket), tx.Bucket(queueBucket), tx.Bucket(deadLetterBucket)
	if err := queue.ForEach(func(_, value []byte) error {
		var item Item
		if err := json.Unmarshal(value, &item); err != nil {
			return err
		}
		if states.Get([]byte(item.DeliveryID)) != nil {
			return nil
		}
		state, err := json.Marshal(DeliveryState{Event: item.Event, Digest: sha256.Sum256(item.RawJSON), Sequence: item.Sequence, Status: "queued", Archived: item.Archived, RawJSON: append([]byte(nil), item.RawJSON...)})
		if err != nil {
			return err
		}
		return states.Put([]byte(item.DeliveryID), state)
	}); err != nil {
		return err
	}
	return dead.ForEach(func(_, value []byte) error {
		var item DeadItem
		if err := json.Unmarshal(value, &item); err != nil {
			return err
		}
		if states.Get([]byte(item.Item.DeliveryID)) != nil {
			return nil
		}
		state, err := json.Marshal(DeliveryState{Event: item.Item.Event, Digest: sha256.Sum256(item.Item.RawJSON), Sequence: item.Item.Sequence, Status: "dead", RawJSON: append([]byte(nil), item.Item.RawJSON...)})
		if err != nil {
			return err
		}
		return states.Put([]byte(item.Item.DeliveryID), state)
	})
}

func rebuildDedupe(tx *bolt.Tx, now time.Time, max uint64) error {
	meta, dedupe, index := tx.Bucket(metaBucket), tx.Bucket(dedupeBucket), tx.Bucket(dedupeExpiryBucket)
	keys := make([][]byte, 0)
	if err := index.ForEach(func(k, _ []byte) error { keys = append(keys, append([]byte(nil), k...)); return nil }); err != nil {
		return err
	}
	for _, k := range keys {
		if err := index.Delete(k); err != nil {
			return err
		}
	}
	count := uint64(0)
	c := dedupe.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		expiresAt := decodeTime(v)
		if len(v) != 8 || !expiresAt.After(now) {
			if err := c.Delete(); err != nil {
				return err
			}
			continue
		}
		if err := index.Put(dedupeIndexKey(expiresAt, string(k)), append([]byte(nil), k...)); err != nil {
			return err
		}
		count++
	}
	if err := meta.Put(keyDedupeCount, numberKey(count)); err != nil {
		return err
	}
	return evictDedupe(tx, max)
}
func rebuildActiveStats(tx *bolt.Tx) error {
	meta, queue := tx.Bucket(metaBucket), tx.Bucket(queueBucket)
	count, bytes := uint64(0), uint64(0)
	if err := queue.ForEach(func(_, value []byte) error {
		var item Item
		if err := json.Unmarshal(value, &item); err != nil {
			return err
		}
		count++
		bytes += activeItemBytes(item)
		return nil
	}); err != nil {
		return err
	}
	if err := meta.Put(keyCount, numberKey(count)); err != nil {
		return err
	}
	return meta.Put(keyBytes, numberKey(bytes))
}

func rebuildDeadStats(tx *bolt.Tx) error {
	meta, dead := tx.Bucket(metaBucket), tx.Bucket(deadLetterBucket)
	count, bytes := uint64(0), uint64(0)
	if err := dead.ForEach(func(_, v []byte) error {
		var item DeadItem
		if err := json.Unmarshal(v, &item); err != nil {
			return err
		}
		count++
		bytes += uint64(len(v))
		return nil
	}); err != nil {
		return err
	}
	if err := meta.Put(keyDeadCount, numberKey(count)); err != nil {
		return err
	}
	return meta.Put(keyDeadBytes, numberKey(bytes))
}
func cleanupDedupe(tx *bolt.Tx, now time.Time, limit int) error {
	meta, dedupe, index := tx.Bucket(metaBucket), tx.Bucket(dedupeBucket), tx.Bucket(dedupeExpiryBucket)
	c, removed := index.Cursor(), 0
	for k, v := c.First(); k != nil && removed < limit; k, v = c.Next() {
		expiresAt := decodeTime(k)
		if expiresAt.After(now) {
			break
		}
		deliveryID := string(v)
		if stored := dedupe.Get([]byte(deliveryID)); len(stored) == 8 && decodeTime(stored).Equal(expiresAt) {
			if err := dedupe.Delete([]byte(deliveryID)); err != nil {
				return err
			}
			if err := decrement(meta, keyDedupeCount); err != nil {
				return err
			}
		}
		if err := c.Delete(); err != nil {
			return err
		}
		removed++
	}
	return nil
}
func evictDedupe(tx *bolt.Tx, max uint64) error {
	meta, dedupe, index := tx.Bucket(metaBucket), tx.Bucket(dedupeBucket), tx.Bucket(dedupeExpiryBucket)
	for getUint(meta.Get(keyDedupeCount)) > max {
		k, v := index.Cursor().First()
		if k == nil {
			return meta.Put(keyDedupeCount, numberKey(0))
		}
		deliveryID, expiresAt := string(v), decodeTime(k)
		if stored := dedupe.Get([]byte(deliveryID)); len(stored) == 8 && decodeTime(stored).Equal(expiresAt) {
			if err := dedupe.Delete([]byte(deliveryID)); err != nil {
				return err
			}
			if err := decrement(meta, keyDedupeCount); err != nil {
				return err
			}
		}
		if err := index.Delete(k); err != nil {
			return err
		}
	}
	return nil
}
func decrement(meta *bolt.Bucket, key []byte) error {
	count := getUint(meta.Get(key))
	if count == 0 {
		return nil
	}
	return meta.Put(key, numberKey(count-1))
}
func trimReason(reason string) string {
	if len(reason) > 256 {
		return reason[:256]
	}
	return reason
}
func dedupeIndexKey(expiry time.Time, deliveryID string) []byte {
	return append(timeKey(expiry), []byte(deliveryID)...)
}
func numberKey(n uint64) []byte  { b := make([]byte, 8); binary.BigEndian.PutUint64(b, n); return b }
func timeKey(t time.Time) []byte { return numberKey(uint64(t.UnixNano())) }
func decodeTime(v []byte) time.Time {
	if len(v) < 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(v[:8]))).UTC()
}

const activeMetadataReserve uint64 = 512

// activeItemBytes intentionally excludes mutable delivery fields. The fixed
// reserve covers attempts, timestamps, archive state, and a bounded error, so a
// full active queue can still record retry metadata and eventually dead-letter.
func activeItemBytes(item Item) uint64 {
	base, _ := json.Marshal(struct {
		Sequence   uint64    `json:"sequence"`
		DeliveryID string    `json:"delivery_id"`
		Event      string    `json:"event"`
		ReceivedAt time.Time `json:"received_at"`
		RawJSON    []byte    `json:"raw_json"`
		ChatIDs    []string  `json:"chat_ids,omitempty"`
	}{item.Sequence, item.DeliveryID, item.Event, item.ReceivedAt, item.RawJSON, item.ChatIDs})
	return uint64(len(base)) + activeMetadataReserve
}

func getUint(v []byte) uint64 {
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}
