package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/undefined-moe/lark-git-webhook/internal/archive"
	"github.com/undefined-moe/lark-git-webhook/internal/store"
)

type fetcher interface {
	Fetch(context.Context, string, time.Time) ([]json.RawMessage, error)
}

type Poller struct {
	store     *store.Store
	archive   *archive.Archive
	client    fetcher
	org       string
	interval  time.Duration
	dedupeTTL time.Duration
	logger    *slog.Logger
}

func NewPoller(s *store.Store, a *archive.Archive, client fetcher, org string, interval, dedupeTTL time.Duration, logger *slog.Logger) *Poller {
	return &Poller{store: s, archive: a, client: client, org: org, interval: interval, dedupeTTL: dedupeTTL, logger: logger}
}

func (p *Poller) Run(ctx context.Context) {
	p.poll(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *Poller) poll(ctx context.Context) {
	for {
		err := p.PollOnce(ctx, time.Now())
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		var retry *RetryError
		if !errors.As(err, &retry) {
			p.logger.Error("poll GitHub audit log", "org", p.org, "error", err)
			return
		}
		wait := retry.Wait
		if wait <= 0 {
			wait = time.Second
		}
		p.logger.Warn("GitHub audit log polling delayed", "org", p.org, "wait", wait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// PollOnce initializes a new checkpoint without history, then archives and queues
// all later records before advancing the checkpoint to the poll start time. A bootstrap
// checkpoint also permanently excludes records that predate audit polling.
func (p *Poller) PollOnce(ctx context.Context, now time.Time) error {
	checkpoint, found, err := p.store.AuditCheckpoint(p.org)
	if err != nil {
		return err
	}
	if !found {
		if err := ctx.Err(); err != nil {
			return err
		}
		return p.store.InitializeAuditCheckpoint(p.org, now)
	}
	bootstrap, filteringBootstrap, err := p.store.AuditBootstrap(p.org)
	if err != nil {
		return err
	}
	pollStarted := now.UTC()
	records, err := p.client.Fetch(ctx, p.org, checkpoint)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, raw := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if filteringBootstrap {
			after, err := createdAfter(raw, bootstrap)
			if err != nil {
				return err
			}
			if !after {
				continue
			}
		}
		if err := p.admit(ctx, raw, pollStarted); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.store.SetAuditCheckpoint(p.org, pollStarted)
}

var errBootstrapTimestamp = errors.New("GitHub audit log bootstrap timestamp invalid")

func createdAfter(raw json.RawMessage, checkpoint time.Time) (bool, error) {
	var record map[string]json.RawMessage
	if json.Unmarshal(raw, &record) != nil {
		return false, errBootstrapTimestamp
	}
	for _, key := range []string{"created_at", "@timestamp", "timestamp"} {
		value, ok := record[key]
		if !ok {
			continue
		}
		if created, ok := parseAuditTimestamp(value); ok {
			return created.After(checkpoint), nil
		}
	}
	return false, errBootstrapTimestamp
}

func parseAuditTimestamp(value json.RawMessage) (time.Time, bool) {
	var text string
	if json.Unmarshal(value, &text) == nil {
		created, err := time.Parse(time.RFC3339Nano, text)
		return created, err == nil
	}
	var number json.Number
	if json.Unmarshal(value, &number) != nil {
		return time.Time{}, false
	}
	unix, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	if unix > -100_000_000_000 && unix < 100_000_000_000 {
		return time.Unix(unix, 0), true
	}
	return time.UnixMilli(unix), true
}

func (p *Poller) admit(ctx context.Context, raw json.RawMessage, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var record struct {
		DocumentID string `json:"_document_id"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return fmt.Errorf("decode audit record: %w", err)
	}
	identity := record.DocumentID
	if identity == "" {
		digest := sha256.Sum256(raw)
		identity = hex.EncodeToString(digest[:])
	}
	deliveryID := "audit:" + p.org + ":" + identity
	state, found, err := p.store.LookupDelivery(deliveryID, "audit_log", raw)
	if err != nil {
		return err
	}
	if found {
		exists, err := p.archive.VerifyExisting("audit_log", deliveryID, raw)
		if err != nil {
			return err
		}
		if exists {
			if !state.Archived {
				return p.store.FinalizeArchive(deliveryID)
			}
			return nil
		}
	}
	reservation, err := p.archive.ReserveContext(ctx, archive.AdmissionRequired(uint64(len(raw))))
	if err != nil {
		return err
	}
	defer reservation.Release()
	if _, err := p.store.Admit(deliveryID, "audit_log", raw, now, p.dedupeTTL); err != nil {
		return err
	}
	if _, err := reservation.Store("audit_log", deliveryID, raw); err != nil {
		return err
	}
	return p.store.FinalizeArchive(deliveryID)
}
