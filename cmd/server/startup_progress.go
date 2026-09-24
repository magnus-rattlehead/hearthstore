package main

import (
	"log/slog"
	"sync"
	"time"
)

const startupHeartbeatInterval = 10 * time.Second

type startupProgress struct {
	logger   *slog.Logger
	task     string
	attrs    []any
	started  time.Time
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

func beginStartupProgress(task string, attrs ...any) *startupProgress {
	ticker := time.NewTicker(startupHeartbeatInterval)
	return newStartupProgress(slog.Default(), task, time.Now(), ticker.C, ticker.Stop, attrs...)
}

func newStartupProgress(
	logger *slog.Logger,
	task string,
	started time.Time,
	ticks <-chan time.Time,
	stopTicker func(),
	attrs ...any,
) *startupProgress {
	progress := &startupProgress{
		logger:  logger,
		task:    task,
		attrs:   append([]any(nil), attrs...),
		started: started,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	progress.log("startup task started", started)
	go func() {
		defer close(progress.stopped)
		defer stopTicker()
		for {
			select {
			case now := <-ticks:
				progress.log("startup task still running", now)
			case <-progress.stop:
				return
			}
		}
	}()
	return progress
}

func (p *startupProgress) log(message string, now time.Time) {
	attrs := make([]any, 0, 4+len(p.attrs))
	attrs = append(attrs, "task", p.task, "elapsed", now.Sub(p.started).Round(time.Second))
	attrs = append(attrs, p.attrs...)
	p.logger.Info(message, attrs...)
}

func (p *startupProgress) halt() {
	p.stopOnce.Do(func() {
		close(p.stop)
		<-p.stopped
	})
}

func (p *startupProgress) complete(now time.Time) {
	p.halt()
	p.log("startup task complete", now)
}
