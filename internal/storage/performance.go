package storage

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// PerformanceStats holds the latest and session-maximum storage timings.
type PerformanceStats struct {
	LastBatchQueueWaitMs     float64 `json:"last_batch_queue_wait_ms"`
	MaxBatchQueueWaitMs      float64 `json:"max_batch_queue_wait_ms"`
	LastBatchExecutionMs     float64 `json:"last_batch_execution_ms"`
	MaxBatchExecutionMs      float64 `json:"max_batch_execution_ms"`
	LastBatchJobs            int64   `json:"last_batch_jobs"`
	CheckpointCount          int64   `json:"checkpoint_count"`
	CheckpointErrors         int64   `json:"checkpoint_errors"`
	CheckpointLastDurationMs float64 `json:"checkpoint_last_duration_ms"`
	CheckpointLogFrames      int64   `json:"checkpoint_log_frames"`
	CheckpointedFrames       int64   `json:"checkpointed_frames"`
	CheckpointBusy           bool    `json:"checkpoint_busy"`
}

// PerformanceSnapshot returns storage timings without blocking write processing.
func (s *Store) PerformanceSnapshot() PerformanceStats {
	return PerformanceStats{
		LastBatchQueueWaitMs:     nanosecondsToMilliseconds(s.batchLastQueueWaitNs.Load()),
		MaxBatchQueueWaitMs:      nanosecondsToMilliseconds(s.batchMaxQueueWaitNs.Load()),
		LastBatchExecutionMs:     nanosecondsToMilliseconds(s.batchLastExecutionNs.Load()),
		MaxBatchExecutionMs:      nanosecondsToMilliseconds(s.batchMaxExecutionNs.Load()),
		LastBatchJobs:            s.batchLastJobs.Load(),
		CheckpointCount:          s.checkpointCount.Load(),
		CheckpointErrors:         s.checkpointErrors.Load(),
		CheckpointLastDurationMs: nanosecondsToMilliseconds(s.checkpointLastDuration.Load()),
		CheckpointLogFrames:      s.checkpointLogFrames.Load(),
		CheckpointedFrames:       s.checkpointedFrames.Load(),
		CheckpointBusy:           s.checkpointBusy.Load(),
	}
}

func (s *Store) runPeriodicCheckpoint() (bool, error) {
	start := time.Now()
	var busy, logFrames, checkpointedFrames int
	err := s.cpdb.QueryRow("PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &logFrames, &checkpointedFrames)
	duration := time.Since(start)
	s.checkpointCount.Add(1)
	s.checkpointLastDuration.Store(duration.Nanoseconds())
	s.checkpointLogFrames.Store(int64(logFrames))
	s.checkpointedFrames.Store(int64(checkpointedFrames))
	s.checkpointBusy.Store(busy != 0)

	attrs := []any{
		"duration", duration.Round(time.Microsecond),
		"busy", busy != 0,
		"log_frames", logFrames,
		"checkpointed_frames", checkpointedFrames,
		"pending_frames", max(0, logFrames-checkpointedFrames),
	}
	if err != nil {
		s.checkpointErrors.Add(1)
		slog.Warn("storage checkpoint failed", append(attrs, "err", err)...)
	} else if busy != 0 || checkpointedFrames < logFrames {
		slog.Warn("storage checkpoint incomplete", attrs...)
	} else {
		slog.Debug("storage checkpoint", attrs...)
	}
	return busy != 0, err
}

func (s *Store) recordBatchPerformance(queueWait, execution time.Duration, jobs int) {
	queueWaitNs := queueWait.Nanoseconds()
	executionNs := execution.Nanoseconds()
	s.batchLastQueueWaitNs.Store(queueWaitNs)
	s.batchLastExecutionNs.Store(executionNs)
	s.batchLastJobs.Store(int64(jobs))
	updateMax(&s.batchMaxQueueWaitNs, queueWaitNs)
	updateMax(&s.batchMaxExecutionNs, executionNs)

	attrs := []any{
		"jobs", jobs,
		"queue_wait", queueWait.Round(time.Microsecond),
		"execution_duration", execution.Round(time.Microsecond),
	}
	if queueWait > batchTimeBudget || execution > batchTimeBudget {
		slog.Warn("slow storage batch", attrs...)
	} else {
		slog.Debug("storage batch", attrs...)
	}
}

func nanosecondsToMilliseconds(value int64) float64 {
	return float64(value) / float64(time.Millisecond)
}

func updateMax(target *atomic.Int64, value int64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}
