package tests

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain silences the hearthstore REST request logs so benchmark output is readable.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	os.Exit(m.Run())
}
