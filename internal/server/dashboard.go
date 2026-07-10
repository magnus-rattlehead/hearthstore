package server

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

//go:embed dashboard.html
var dashboardHTML []byte

// NewDashboard returns an http.Handler serving /_/metrics (JSON) and /_/dashboard (HTML).
func NewDashboard(store *storage.Store, ops *OperationLog, startTime time.Time) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(buildSnapshot(store, ops, startTime))
	})
	mux.HandleFunc("/_/operations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(ops.Query(operationQueryFromValues(r.URL.Query())))
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
	ID        int64          `json:"id"`
	SessionID string         `json:"session_id"`
	T         string         `json:"t"`
	Source    string         `json:"source,omitempty"`
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

func buildSnapshot(store *storage.Store, ops *OperationLog, startTime time.Time) metricsSnapshot {
	batchLen, batchCap, prioLen, prioCap := store.BatchStats()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	entries := ops.Recent(60)
	recent := make([]recentJSON, len(entries))
	for i, e := range entries {
		recent[i] = recentJSON{
			ID:        e.ID,
			SessionID: e.SessionID,
			T:         e.T.Format("15:04:05.000"),
			Source:    e.Source,
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

func operationQueryFromValues(v url.Values) OperationQuery {
	q := OperationQuery{
		Method:     strings.TrimSpace(v.Get("method")),
		Source:     strings.TrimSpace(v.Get("source")),
		Text:       strings.TrimSpace(v.Get("q")),
		Path:       strings.TrimSpace(v.Get("path")),
		Collection: strings.TrimSpace(v.Get("collection")),
		Detail:     strings.TrimSpace(v.Get("detail")),
		Limit:      intParam(v, "limit", 100),
		Offset:     intParam(v, "offset", 0),
		OrderDesc:  strings.ToLower(v.Get("order")) != "asc",
	}
	if cls := strings.TrimSpace(v.Get("status_class")); cls != "" {
		if n, err := strconv.Atoi(strings.TrimSuffix(cls, "xx")); err == nil {
			if n < 10 {
				n *= 100
			}
			q.StatusClass = n
		}
	}
	switch strings.ToLower(strings.TrimSpace(v.Get("error"))) {
	case "1", "true", "yes", "only":
		q.ErrorOnly = true
	}
	q.MinLatencyMs = int64(intParam(v, "min_latency_ms", 0))
	q.From = timeParam(v.Get("from"))
	q.To = timeParam(v.Get("to"))
	return q
}

func intParam(v url.Values, key string, def int) int {
	raw := strings.TrimSpace(v.Get(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func timeParam(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02T15:04", raw); err == nil {
		return t
	}
	return time.Time{}
}
