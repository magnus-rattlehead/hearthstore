package server

import (
	"sync"
	"time"
)

// RecentEntry is one recorded RPC/HTTP call.
type RecentEntry struct {
	T         time.Time
	Method    string
	Path      string
	LatencyMs int64
	Status    int
	Err       string
	Details   map[string]any
}

// RecentRing is a fixed-size circular buffer of recent RPC entries.
type RecentRing struct {
	mu   sync.Mutex
	buf  []RecentEntry
	cap  int
	head int // next write position
	full bool
}

func NewRecentRing(capacity int) *RecentRing {
	return &RecentRing{buf: make([]RecentEntry, capacity), cap: capacity}
}

// Add appends an entry, evicting the oldest when full.
func (r *RecentRing) Add(e RecentEntry) {
	r.mu.Lock()
	r.buf[r.head] = e
	r.head = (r.head + 1) % r.cap
	if r.head == 0 {
		r.full = true
	}
	r.mu.Unlock()
}

// Snapshot returns a copy of entries in insertion order, newest last.
func (r *RecentRing) Snapshot() []RecentEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		out := make([]RecentEntry, r.head)
		copy(out, r.buf[:r.head])
		return out
	}
	out := make([]RecentEntry, r.cap)
	copy(out, r.buf[r.head:])
	copy(out[r.cap-r.head:], r.buf[:r.head])
	return out
}
