package server

import (
	_ "embed"
	"database/sql"
	"encoding/json"
	"net/http"
	"runtime"
	"time"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

//go:embed dashboard.html
var dashboardHTML []byte

// NewDashboard returns an http.Handler serving /_/metrics (JSON) and /_/dashboard (HTML).
func NewDashboard(store *storage.Store, ring *RecentRing, startTime time.Time) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(buildSnapshot(store, ring, startTime))
	})
	mux.HandleFunc("/_/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	})
	mux.HandleFunc("/_/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/_/dashboard", http.StatusFound)
	})
	return mux
}

type dbPoolJSON struct {
	Open           int     `json:"open"`
	InUse          int     `json:"in_use"`
	Idle           int     `json:"idle"`
	WaitCount      int64   `json:"wait_count"`
	WaitDurationMs float64 `json:"wait_duration_ms"`
}

func dbStats(s sql.DBStats) dbPoolJSON {
	return dbPoolJSON{
		Open:           s.OpenConnections,
		InUse:          s.InUse,
		Idle:           s.Idle,
		WaitCount:      s.WaitCount,
		WaitDurationMs: float64(s.WaitDuration.Milliseconds()),
	}
}

type storageSnapshot struct {
	BatchCh    map[string]int `json:"batch_ch"`
	PriorityCh map[string]int `json:"priority_ch"`
	SubEntries int            `json:"sub_entries"`
	ByScope    int            `json:"by_scope"`
	ByCollG    int            `json:"by_coll_g"`
	NextID     uint64         `json:"next_id"`
	WDB        dbPoolJSON     `json:"wdb"`
	RDB        dbPoolJSON     `json:"rdb"`
	CPDB       dbPoolJSON     `json:"cpdb"`
}

type runtimeSnapshot struct {
	Goroutines   int     `json:"goroutines"`
	AllocMB      float64 `json:"alloc_mb"`
	SysMB        float64 `json:"sys_mb"`
	HeapInuseMB  float64 `json:"heap_inuse_mb"`
	RSSMB        float64 `json:"rss_mb"`
	HeapObjects  uint64  `json:"heap_objects"`
	NumGC        uint32  `json:"num_gc"`
	PauseTotalMs float64 `json:"pause_total_ms"`
	GOMAXPROCS   int     `json:"gomaxprocs"`
}

type recentJSON struct {
	T         string         `json:"t"`
	Method    string         `json:"method"`
	Path      string         `json:"path"`
	LatencyMs int64          `json:"latency_ms"`
	Status    int            `json:"status,omitempty"`
	Err       string         `json:"err,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

type metricsSnapshot struct {
	UptimeSec float64                 `json:"uptime_sec"`
	Counters  storage.CounterSnapshot `json:"counters"`
	Storage   storageSnapshot         `json:"storage"`
	DB        storage.DBContentStats  `json:"db"`
	Runtime   runtimeSnapshot         `json:"runtime"`
	Recent    []recentJSON            `json:"recent"`
}

func buildSnapshot(store *storage.Store, ring *RecentRing, startTime time.Time) metricsSnapshot {
	batchLen, batchCap, prioLen, prioCap := store.BatchStats()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	entries := ring.Snapshot()
	recent := make([]recentJSON, len(entries))
	for i, e := range entries {
		recent[i] = recentJSON{
			T:         e.T.Format("15:04:05.000"),
			Method:    e.Method,
			Path:      e.Path,
			LatencyMs: e.LatencyMs,
			Status:    e.Status,
			Err:       e.Err,
			Details:   e.Details,
		}
	}

	return metricsSnapshot{
		UptimeSec: time.Since(startTime).Seconds(),
		Counters:  store.CounterSnapshot(),
		Storage: storageSnapshot{
			BatchCh:    map[string]int{"len": batchLen, "cap": batchCap},
			PriorityCh: map[string]int{"len": prioLen, "cap": prioCap},
			SubEntries: store.SubCount(),
			ByScope:    store.ScopeCount(),
			ByCollG:    store.CollGroupCount(),
			NextID:     store.NextID(),
			WDB:        dbStats(store.DB().Stats()),
			RDB:        dbStats(store.RDB().Stats()),
			CPDB:       dbStats(store.CPDB().Stats()),
		},
		DB: store.DBContentStats(),
		Runtime: runtimeSnapshot{
			Goroutines:   runtime.NumGoroutine(),
			AllocMB:      float64(mem.Alloc) / (1024 * 1024),
			SysMB:        float64(mem.Sys) / (1024 * 1024),
			HeapInuseMB:  float64(mem.HeapInuse) / (1024 * 1024),
			RSSMB:        float64(processRSSBytes()) / (1024 * 1024),
			HeapObjects:  mem.HeapObjects,
			NumGC:        mem.NumGC,
			PauseTotalMs: float64(mem.PauseTotalNs) / 1e6,
			GOMAXPROCS:   runtime.GOMAXPROCS(0),
		},
		Recent: recent,
	}
}
