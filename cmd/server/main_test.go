package main

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

type startupRecordHandler struct {
	records chan slog.Record
}

func (h *startupRecordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *startupRecordHandler) Handle(_ context.Context, record slog.Record) error {
	h.records <- record.Clone()
	return nil
}

func (h *startupRecordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *startupRecordHandler) WithGroup(string) slog.Handler { return h }

func TestStartupProgress_ReportsStartHeartbeatAndCompletion(t *testing.T) {
	records := make(chan slog.Record, 3)
	ticks := make(chan time.Time, 1)
	tickerStopped := make(chan struct{}, 1)
	started := time.Date(2026, time.August, 20, 10, 0, 0, 0, time.UTC)
	logger := slog.New(&startupRecordHandler{records: records})

	progress := newStartupProgress(
		logger,
		"loading storage and ensuring database indexes",
		started,
		ticks,
		func() { tickerStopped <- struct{}{} },
		"data_dir", "/tmp/hearthstore",
	)

	assertStartupRecord(t, nextStartupRecord(t, records), "startup task started", "0s")
	ticks <- started.Add(12 * time.Second)
	assertStartupRecord(t, nextStartupRecord(t, records), "startup task still running", "12s")
	progress.complete(started.Add(13 * time.Second))
	assertStartupRecord(t, nextStartupRecord(t, records), "startup task complete", "13s")

	select {
	case <-tickerStopped:
	default:
		t.Fatal("ticker was not stopped on completion")
	}
}

func nextStartupRecord(t *testing.T, records <-chan slog.Record) slog.Record {
	t.Helper()
	select {
	case record := <-records:
		return record
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for startup log record")
		return slog.Record{}
	}
}

func assertStartupRecord(t *testing.T, record slog.Record, message, elapsed string) {
	t.Helper()
	if record.Message != message {
		t.Fatalf("message = %q, want %q", record.Message, message)
	}
	attrs := map[string]string{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	if attrs["task"] != "loading storage and ensuring database indexes" {
		t.Errorf("task = %q", attrs["task"])
	}
	if attrs["elapsed"] != elapsed {
		t.Errorf("elapsed = %q, want %q", attrs["elapsed"], elapsed)
	}
	if attrs["data_dir"] != "/tmp/hearthstore" {
		t.Errorf("data_dir = %q", attrs["data_dir"])
	}
}
