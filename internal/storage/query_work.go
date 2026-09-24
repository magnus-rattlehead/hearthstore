package storage

import (
	"context"
	"io"
	"math"
	"runtime"
	"sync/atomic"
)

// WorkKind identifies cumulative execution measurements, never rejection limits.
type WorkKind int

const (
	WorkAttempts WorkKind = iota
	WorkComparisons
	WorkIndexEntries
	WorkDecodedBytes
	WorkProjectionTuples
	WorkScratchReadBytes
	WorkScratchWriteBytes
	WorkYields
	workKinds
)

// QueryWork is shared by an RPC's internal pages, not by unrelated requests.
// Atomics allow safe observation while execution proceeds. A shared index build
// owns a separate collector rather than inheriting a waiting query's lifetime.
type QueryWork struct {
	counts  [workKinds]atomic.Uint64
	quantum uint64
}

type queryWorkKey struct{}

// WithQueryWork starts accounting or preserves the enclosing RPC's collector.
// Zero uses the calibrated internal batch size, not an acceptance limit.
// Tests can force a yield at every primitive checkpoint.
func WithQueryWork(ctx context.Context, quantum uint64) (context.Context, *QueryWork) {
	if work := QueryWorkFromContext(ctx); work != nil {
		return ctx, work
	}
	if quantum == 0 {
		quantum = 1024
	}
	work := &QueryWork{quantum: quantum}
	return context.WithValue(ctx, queryWorkKey{}, work), work
}

// QueryWorkFromContext returns the enclosing collector, or nil outside queries.
// Hot loops retain this pointer rather than repeatedly searching the context.
func QueryWorkFromContext(ctx context.Context) *QueryWork {
	work, _ := ctx.Value(queryWorkKey{}).(*QueryWork)
	return work
}

func addWork(counter *atomic.Uint64, amount uint64) uint64 {
	for {
		old := counter.Load()
		next := old + min(amount, math.MaxUint64-old)
		if counter.CompareAndSwap(old, next) {
			return next
		}
	}
}

// Charge records work even when it produces no rows. A nil collector is inert.
func (w *QueryWork) Charge(kind WorkKind, amount uint64) {
	if w != nil {
		addWork(&w.counts[kind], amount)
	}
}

// Checkpoint cooperatively yields without unwinding the iterator/evaluator.
// Go retains the call stack, tuple positions, bindings and partial accumulators;
// resumption replays no completed step and never reports a partial success.
func (w *QueryWork) Checkpoint(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w != nil && addWork(&w.counts[WorkAttempts], 1)%w.quantum == 0 {
		w.Charge(WorkYields, 1)
		runtime.Gosched()
		return ctx.Err()
	}
	return nil
}

// Snapshot returns saturating cumulative counters. During execution different
// counters may advance between loads; after completion it is an exact snapshot.
func (w *QueryWork) Snapshot() [workKinds]uint64 {
	var result [workKinds]uint64
	if w != nil {
		for i := range result {
			result[i] = w.counts[i].Load()
		}
	}
	return result
}

// Count file traffic at the buffered reader/writer boundary, including prefetched
// or partially written bytes. These are scratch file bytes, not device I/O.
type queryWorkReader struct {
	io.Reader
	work *QueryWork
}

func (r queryWorkReader) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	r.work.Charge(WorkScratchReadBytes, uint64(n))
	return n, err
}

type queryWorkWriter struct {
	io.Writer
	work *QueryWork
}

func (w queryWorkWriter) Write(buffer []byte) (int, error) {
	n, err := w.Writer.Write(buffer)
	w.work.Charge(WorkScratchWriteBytes, uint64(n))
	return n, err
}
