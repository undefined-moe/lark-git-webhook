package archive

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNonNegativeUint64(t *testing.T) {
	if value, ok := nonNegativeUint64(int64(-1)); ok || value != 0 {
		t.Fatalf("negative signed value=%d ok=%v", value, ok)
	}
	if value, ok := nonNegativeUint64(uint64(123)); !ok || value != 123 {
		t.Fatalf("unsigned value=%d ok=%v", value, ok)
	}
}

func TestStoreExactBytesPermissionsAndShard(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	a, err := Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("{\n  \"message\": \"exact bytes\"\n}\n")
	created, err := a.Store("push", "ab-delivery", payload)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	dir, err := a.deliveryDir("ab-delivery")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "payload.push.json")
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("payload=%q err=%v", got, err)
	}
	for _, name := range []string{root, filepath.Dir(dir), dir} {
		info, err := os.Stat(name)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("directory %s mode=%v err=%v", name, info.Mode(), err)
		}
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode=%v err=%v", info.Mode(), err)
	}
}

func TestStoreDuplicateAndConflict(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := a.Store("push", "delivery", []byte(`{"id":1}`)); err != nil || !created {
		t.Fatalf("first store created=%v err=%v", created, err)
	}
	if created, err := a.Store("push", "delivery", []byte(`{"id":1}`)); err != nil || created {
		t.Fatalf("duplicate store created=%v err=%v", created, err)
	}
	if _, err := a.Store("push", "delivery", []byte(`{"id":2}`)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict err=%v", err)
	}
}

func TestStoreReservesFreeSpaceAndLeavesNoPayload(t *testing.T) {
	oldAvailableBytes := availableBytes
	availableBytes = func(string) (uint64, error) { return 100, nil }
	t.Cleanup(func() { availableBytes = oldAvailableBytes })
	a, err := Open(filepath.Join(t.TempDir(), "archive"), 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Store("push", "delivery", []byte(`{}`)); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatalf("store error=%v", err)
	}
	dir, err := a.deliveryDir("delivery")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestStoreEnforcesDeliveryGlobalEvent(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "archive"), 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"id":1}`)
	if _, err := a.Store("push", "delivery", payload); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Store("issues", "delivery", payload); !errors.Is(err, ErrConflict) {
		t.Fatalf("different event conflict=%v", err)
	}
}

func TestReservationSerializesChecksAndRejectsOverflow(t *testing.T) {
	oldAvailableBytes := availableBytes
	calls := 0
	availableBytes = func(string) (uint64, error) {
		calls++
		if calls == 1 {
			return 100, nil
		}
		return 89, nil
	}
	t.Cleanup(func() { availableBytes = oldAvailableBytes })
	a, err := Open(filepath.Join(t.TempDir(), "archive"), 10)
	if err != nil {
		t.Fatal(err)
	}
	first, err := a.Reserve(80)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := a.Reserve(80)
		result <- err
	}()
	if calls != 1 {
		t.Fatalf("second reservation checked space while first was held: calls=%d", calls)
	}
	first.Release()
	if err := <-result; !errors.Is(err, ErrInsufficientSpace) {
		t.Fatalf("second reservation error=%v", err)
	}
	if got := AdmissionRequired((^uint64(0)-((2<<20)-1))/4 + 1); got != ^uint64(0) {
		t.Fatalf("overflow reservation=%d", got)
	}
}

func TestStoreConcurrentDuplicateCleansTemporaryFiles(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	a, err := Open(root, 0)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := a.Store("push", "ab-delivery", []byte(`{"same":true}`))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	dir, err := a.deliveryDir("ab-delivery")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "payload.push.json" {
		t.Fatalf("archive entries=%v", entries)
	}
}
