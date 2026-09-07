package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestAuditBootstrapSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path, 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	if err := s.InitializeAuditCheckpoint("example-org", want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, found, err := s.AuditBootstrap("example-org")
	if err != nil || !found || !got.Equal(want) {
		t.Fatalf("bootstrap=%s found=%v err=%v", got, found, err)
	}
}

func TestAuditCheckpointSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path, 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	if err := s.SetAuditCheckpoint("example-org", want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, 10, 1<<20, 10, 10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, found, err := s.AuditCheckpoint("example-org")
	if err != nil || !found || !got.Equal(want) {
		t.Fatalf("checkpoint=%s found=%v err=%v", got, found, err)
	}
}
