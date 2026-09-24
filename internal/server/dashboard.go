package server

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

//go:embed dashboard.html
var dashboardHTML []byte

// NewDashboard returns an http.Handler serving the operational dashboard and its data feeds.
func NewDashboard(store *storage.Store, ops *OperationLog) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_/indexes", func(w http.ResponseWriter, r *http.Request) {
		indexes, err := store.ListDsCompositeIndexes("")
		if err != nil {
			writeDashboardError(w, http.StatusInternalServerError, "index_list_failed", "failed to list indexes")
			return
		}
		if indexes == nil {
			indexes = []storage.DsCompositeIndex{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(indexes)
	})
	mux.HandleFunc("GET /_/operations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ops.Query(operationQueryFromValues(r.URL.Query())))
	})
	mux.HandleFunc("GET /_/dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	})
	mux.HandleFunc("GET /_/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/_/dashboard", http.StatusFound)
	})
	return mux
}

func writeDashboardError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

func operationQueryFromValues(v url.Values) OperationQuery {
	q := OperationQuery{
		Method:    strings.TrimSpace(v.Get("method")),
		Text:      strings.TrimSpace(v.Get("q")),
		Path:      strings.TrimSpace(v.Get("path")),
		Limit:     intParam(v, "limit", 100),
		Offset:    intParam(v, "offset", 0),
		OrderDesc: strings.ToLower(v.Get("order")) != "asc",
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
