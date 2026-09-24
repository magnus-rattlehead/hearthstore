package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func TestH2CDrainWaitsForRequestsAndRejectsNewStreams(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	gate := newRequestDrain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		io.WriteString(w, "completed")
	}))
	srv := httptest.NewServer(h2c.NewHandler(gate, &http2.Server{}))
	defer srv.Close()
	transport := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	result := make(chan error, 1)
	go func() {
		resp, err := client.Get(srv.URL)
		if err == nil {
			defer resp.Body.Close()
			body, readErr := io.ReadAll(resp.Body)
			err = readErr
			if string(body) != "completed" {
				err = fmt.Errorf("response = %q", body)
			}
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("request did not start")
	}
	gate.beginDrain()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.wait(canceled); err == nil {
		close(release)
		t.Fatal("drain ignored active HTTP/2 stream")
	}
	resp, err := client.Get(srv.URL)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("new stream status = %d", resp.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gate.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

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

func TestOpenStorageWithRecoveryRequiresConfirmation(t *testing.T) {
	for _, test := range []struct {
		name      string
		answer    string
		canPrompt bool
		wantOpen  bool
	}{
		{name: "confirmed", answer: "yes\n", canPrompt: true, wantOpen: true},
		{name: "short confirmation", answer: " Y \n", canPrompt: true, wantOpen: true},
		{name: "declined", answer: "no\n", canPrompt: true},
		{name: "default", answer: "\n", canPrompt: true},
		{name: "invalid", answer: "maybe\n", canPrompt: true},
		{name: "EOF", canPrompt: true},
		{name: "incomplete confirmation", answer: "yes", canPrompt: true},
		{name: "non-interactive", answer: "yes\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "badger"), 0o700); err != nil {
				t.Fatal(err)
			}
			files := []string{"hearthstore.db", "hearthstore.db-wal", "hearthstore.db-shm", "storage-format", "badger/old-data"}
			for _, name := range append(files, "index.yaml") {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			var output bytes.Buffer
			store, err := openStorageWithRecovery(dir, strings.NewReader(test.answer), &output, test.canPrompt)
			if test.wantOpen {
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if err := storage.CheckCompatibility(dir); err != nil {
					t.Fatalf("replacement storage is incompatible: %v", err)
				}
			} else if !errors.Is(err, storage.ErrIncompatibleData) {
				t.Fatalf("expected incompatible storage error, got %v", err)
			}
			for _, name := range files {
				data, readErr := os.ReadFile(filepath.Join(dir, name))
				if test.wantOpen && name != "storage-format" {
					if !os.IsNotExist(readErr) {
						t.Fatalf("incompatible file %s remains: %v", name, readErr)
					}
				} else if !test.wantOpen && (readErr != nil || string(data) != "old") {
					t.Fatalf("unconfirmed recovery changed %s: %q, %v", name, data, readErr)
				}
			}
			if data, err := os.ReadFile(filepath.Join(dir, "index.yaml")); err != nil || string(data) != "old" {
				t.Fatalf("unrelated file changed: %q, %v", data, err)
			}
			if !strings.Contains(output.String(), dir) {
				t.Fatalf("prompt %q does not identify the data directory", output.String())
			}
			if strings.Contains(output.String(), "[y/N]") != test.canPrompt {
				t.Fatalf("unexpected prompt behavior: %q", output.String())
			}
		})
	}
}

func TestValidateTransferFlagsRequiresProjectID(t *testing.T) {
	if err := validateTransferFlags("", "", ""); err != nil {
		t.Fatalf("no transfer flags: %v", err)
	}
	for _, test := range []struct {
		name, importData, exportOnExit string
	}{
		{name: "import", importData: "/tmp/import"},
		{name: "export", exportOnExit: "/tmp/export"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateTransferFlags("", test.importData, test.exportOnExit); err == nil {
				t.Fatal("missing project ID was accepted")
			}
			if err := validateTransferFlags("project", test.importData, test.exportOnExit); err != nil {
				t.Fatalf("project ID was rejected: %v", err)
			}
		})
	}
}
