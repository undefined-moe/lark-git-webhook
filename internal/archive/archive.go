package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

var (
	ErrConflict          = errors.New("archive payload conflicts with existing delivery")
	ErrInsufficientSpace = errors.New("archive free space reserve not met")
	availableBytes       = diskFreeBytes
)

type Archive struct {
	root         string
	minFreeBytes uint64
	storeMu      chan struct{}
}

type Pending struct {
	DeliveryID string
	Event      string
	RawJSON    []byte
}

// Open creates and verifies the permanent archive root.
func Open(root string, minFreeBytes uint64) (*Archive, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create archive root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("set archive root permissions: %w", err)
	}
	if err := syncDir(root); err != nil {
		return nil, fmt.Errorf("sync archive root: %w", err)
	}
	if err := syncDir(filepath.Dir(root)); err != nil {
		return nil, fmt.Errorf("sync archive root parent: %w", err)
	}
	probe, err := os.CreateTemp(root, ".write-probe-")
	if err != nil {
		return nil, fmt.Errorf("create archive probe: %w", err)
	}
	name := probe.Name()
	defer os.Remove(name)
	if err := probe.Chmod(0o600); err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf("set archive probe permissions: %w", err)
	}
	if _, err := probe.Write([]byte("ok")); err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf("write archive probe: %w", err)
	}
	if err := probe.Sync(); err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf("sync archive probe: %w", err)
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf("close archive probe: %w", err)
	}
	a := &Archive{root: root, minFreeBytes: minFreeBytes, storeMu: make(chan struct{}, 1)}
	a.storeMu <- struct{}{}
	return a, nil
}

func (a *Archive) lock()   { <-a.storeMu }
func (a *Archive) unlock() { a.storeMu <- struct{}{} }

func (a *Archive) lockContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.storeMu:
		return nil
	}
}

func (a *Archive) FreeBytes() (uint64, error) { return availableBytes(a.root) }

// VerifyExisting checks an archived payload without consuming space or creating files.
func (a *Archive) VerifyExisting(event, deliveryID string, payload []byte) (bool, error) {
	a.lock()
	defer a.unlock()
	return a.verifyExisting(event, deliveryID, payload)
}

// VerifyExistingContext is the callback-safe variant of VerifyExisting. It
// returns ctx.Err when the archive lock cannot be acquired before cancellation.
func (a *Archive) VerifyExistingContext(ctx context.Context, event, deliveryID string, payload []byte) (bool, error) {
	if err := a.lockContext(ctx); err != nil {
		return false, err
	}
	defer a.unlock()
	return a.verifyExisting(event, deliveryID, payload)
}

func (a *Archive) verifyExisting(event, deliveryID string, payload []byte) (bool, error) {
	dir, err := a.deliveryDir(deliveryID)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat archive delivery directory: %w", err)
	}
	if !info.IsDir() {
		return false, fmt.Errorf("archive delivery path is not a directory: %w", ErrConflict)
	}
	path := filepath.Join(dir, "payload."+event+".json")
	found, err := existingPayload(dir, path, payload)
	if err != nil || !found {
		return found, err
	}
	if err := syncDir(dir); err != nil {
		return false, fmt.Errorf("sync existing archive payload directory: %w", err)
	}
	return true, nil
}

type Reservation struct {
	archive  *Archive
	released bool
}

// AdmissionRequired returns a conservative reservation for archive and bbolt admission writes.
func AdmissionRequired(payloadBytes uint64) uint64 {
	const overhead = 2 << 20
	if payloadBytes > (^uint64(0)-overhead)/4 {
		return ^uint64(0)
	}
	return 4*payloadBytes + overhead
}

// Reserve serializes admission space checks until the caller publishes and finalizes the delivery.
func (a *Archive) Reserve(required uint64) (*Reservation, error) {
	a.lock()
	if err := a.checkSpace(required); err != nil {
		a.unlock()
		return nil, err
	}
	return &Reservation{archive: a}, nil
}

// ReserveContext is the callback-safe variant of Reserve. It returns ctx.Err
// when archive admission is still locked after cancellation.
func (a *Archive) ReserveContext(ctx context.Context, required uint64) (*Reservation, error) {
	if err := a.lockContext(ctx); err != nil {
		return nil, err
	}
	if err := a.checkSpace(required); err != nil {
		a.unlock()
		return nil, err
	}
	return &Reservation{archive: a}, nil
}

func (r *Reservation) Store(event, deliveryID string, payload []byte) (bool, error) {
	if r == nil || r.released {
		return false, errors.New("archive reservation is released")
	}
	return r.archive.store(event, deliveryID, payload, false)
}

func (r *Reservation) Release() {
	if r != nil && !r.released {
		r.released = true
		r.archive.unlock()
	}
}

func (a *Archive) checkSpace(required uint64) error {
	if required == ^uint64(0) {
		return ErrInsufficientSpace
	}
	free, err := a.FreeBytes()
	if err != nil {
		return fmt.Errorf("check archive free space: %w", err)
	}
	if free < a.minFreeBytes || free-a.minFreeBytes < required {
		return fmt.Errorf("archive has %d free bytes; need reserve %d plus required %d: %w", free, a.minFreeBytes, required, ErrInsufficientSpace)
	}
	return nil
}

// Store permanently records payload before queueing it. It reports whether it created a new payload.
func (a *Archive) Store(event, deliveryID string, payload []byte) (bool, error) {
	a.lock()
	defer a.unlock()
	return a.store(event, deliveryID, payload, true)
}

func (a *Archive) store(event, deliveryID string, payload []byte, checkSpace bool) (bool, error) {
	dir, err := a.deliveryDir(deliveryID)
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("create archive directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(dir), 0o700); err != nil {
		return false, fmt.Errorf("set archive shard permissions: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return false, fmt.Errorf("set archive delivery directory permissions: %w", err)
	}
	if err := syncDir(a.root); err != nil {
		return false, fmt.Errorf("sync archive root: %w", err)
	}
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return false, fmt.Errorf("sync archive shard directory: %w", err)
	}
	path := filepath.Join(dir, "payload."+event+".json")
	if exists, err := existingPayload(dir, path, payload); err != nil {
		return false, err
	} else if exists {
		if err := syncDir(dir); err != nil {
			return false, fmt.Errorf("sync existing archive payload directory: %w", err)
		}
		return false, nil
	}
	if checkSpace {
		if err := a.checkSpace(uint64(len(payload))); err != nil {
			return false, err
		}
	}
	temp, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		return false, fmt.Errorf("create archive temporary file: %w", err)
	}
	tempPath := temp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = temp.Close()
		}
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return false, fmt.Errorf("set archive file permissions: %w", err)
	}
	if _, err := temp.Write(payload); err != nil {
		return false, fmt.Errorf("write archive payload: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return false, fmt.Errorf("sync archive payload: %w", err)
	}
	if err := temp.Close(); err != nil {
		return false, fmt.Errorf("close archive payload: %w", err)
	}
	closed = true
	if err := os.Link(tempPath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return false, fmt.Errorf("publish archive payload: %w", err)
		}
		exists, existingErr := existingPayload(dir, path, payload)
		if existingErr != nil {
			return false, existingErr
		}
		if exists {
			if err := syncDir(dir); err != nil {
				return false, fmt.Errorf("sync existing archive payload directory: %w", err)
			}
			return false, nil
		}
		return false, fmt.Errorf("archive payload appeared during publish: %w", ErrConflict)
	}
	if err := os.Remove(tempPath); err != nil {
		return false, fmt.Errorf("remove archive temporary file: %w", err)
	}
	tempPath = ""
	if err := syncDir(dir); err != nil {
		return false, fmt.Errorf("sync archive delivery directory: %w", err)
	}
	return true, nil
}

// MarkAccepted durably records that the archived delivery has been queued.
func (a *Archive) MarkAccepted(event, deliveryID string) error {
	a.lock()
	defer a.unlock()
	dir, err := a.deliveryDir(deliveryID)
	if err != nil {
		return err
	}
	archivedEvent, _, exists, err := archivedPayload(dir)
	if err != nil {
		return err
	}
	if !exists || archivedEvent != event {
		return ErrConflict
	}
	marker := filepath.Join(dir, "accepted")
	if info, err := os.Lstat(marker); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("archive accepted marker is not a regular file: %w", ErrConflict)
		}
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("sync accepted archive directory: %w", err)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat archive accepted marker: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".accepted-")
	if err != nil {
		return fmt.Errorf("create accepted marker: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set accepted marker permissions: %w", err)
	}
	if _, err := temp.Write([]byte("ok\n")); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write accepted marker: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync accepted marker: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close accepted marker: %w", err)
	}
	if err := os.Link(tempPath, marker); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("publish accepted marker: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync accepted archive directory: %w", err)
	}
	return nil
}

// Pending lists archived deliveries that do not yet have a durable accepted marker.
func (a *Archive) Pending() ([]Pending, error) {
	a.lock()
	defer a.unlock()
	shards, err := os.ReadDir(a.root)
	if err != nil {
		return nil, fmt.Errorf("read archive root: %w", err)
	}
	pending := make([]Pending, 0)
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		deliveries, err := os.ReadDir(filepath.Join(a.root, shard.Name()))
		if err != nil {
			return nil, fmt.Errorf("read archive shard: %w", err)
		}
		for _, delivery := range deliveries {
			if !delivery.IsDir() {
				continue
			}
			deliveryID, err := base64.RawURLEncoding.DecodeString(delivery.Name())
			if err != nil {
				return nil, fmt.Errorf("decode archive delivery ID: %w", err)
			}
			dir := filepath.Join(a.root, shard.Name(), delivery.Name())
			event, raw, exists, err := archivedPayload(dir)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			if _, err := os.Lstat(filepath.Join(dir, "accepted")); err == nil {
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("stat archive accepted marker: %w", err)
			}
			pending = append(pending, Pending{DeliveryID: string(deliveryID), Event: event, RawJSON: raw})
		}
	}
	return pending, nil
}

func (a *Archive) deliveryDir(deliveryID string) (string, error) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(deliveryID))
	if len(encoded) < 2 {
		return "", errors.New("encoded delivery ID is too short")
	}
	return filepath.Join(a.root, encoded[:2], encoded), nil
}

func existingPayload(dir, expectedPath string, payload []byte) (bool, error) {
	event, existing, exists, err := archivedPayload(dir)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if filepath.Base(expectedPath) != "payload."+event+".json" || !bytes.Equal(existing, payload) {
		return false, ErrConflict
	}
	return true, nil
}

func archivedPayload(dir string) (string, []byte, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, false, fmt.Errorf("read archive delivery directory: %w", err)
	}
	var name string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "payload.") && strings.HasSuffix(entry.Name(), ".json") {
			if name != "" {
				return "", nil, false, fmt.Errorf("multiple archive payloads: %w", ErrConflict)
			}
			name = entry.Name()
		}
	}
	if name == "" {
		return "", nil, false, nil
	}
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil {
		return "", nil, false, fmt.Errorf("stat archive payload: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", nil, false, fmt.Errorf("archive payload is not a regular file: %w", ErrConflict)
	}
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "", nil, false, fmt.Errorf("read archive payload: %w", err)
	}
	event := strings.TrimSuffix(strings.TrimPrefix(name, "payload."), ".json")
	if event == "" || strings.Contains(event, "/") {
		return "", nil, false, fmt.Errorf("invalid archive payload event: %w", ErrConflict)
	}
	return event, raw, true, nil
}

func diskFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, errors.New("filesystem reported invalid available space")
	}
	available, ok := nonNegativeUint64(stat.Bavail)
	if !ok {
		return 0, errors.New("filesystem reported negative available space")
	}
	blockSize := uint64(stat.Bsize)
	if available > ^uint64(0)/blockSize {
		return ^uint64(0), nil
	}
	return available * blockSize, nil
}

func nonNegativeUint64[T ~int64 | ~uint64](value T) (uint64, bool) {
	if value < 0 {
		return 0, false
	}
	return uint64(value), true
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
