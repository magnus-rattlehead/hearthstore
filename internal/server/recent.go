package server

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

const operationHistoryLimit = 10_000

// OperationEntry is one dashboard-visible logical operation.
type OperationEntry struct {
	ID         int64          `json:"id"`
	SessionID  string         `json:"session_id"`
	T          time.Time      `json:"-"`
	Time       string         `json:"t"`
	Source     string         `json:"source,omitempty"`
	Method     string         `json:"method"`
	Path       string         `json:"path,omitempty"`
	LatencyMs  int64          `json:"latency_ms"`
	Status     int            `json:"status,omitempty"`
	Err        string         `json:"err,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	detailText string
}

// OperationLog keeps every operation for the current server process.
type OperationLog struct {
	mu        sync.RWMutex
	sessionID string
	nextID    int64
	entries   []OperationEntry
	start     int
}

func NewOperationLog(sessionID string) *OperationLog {
	if sessionID == "" {
		sessionID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return &OperationLog{sessionID: sessionID}
}

func (l *OperationLog) SessionID() string {
	if l == nil {
		return ""
	}
	return l.sessionID
}

// Add appends one operation and returns the stored row.
func (l *OperationLog) Add(e OperationEntry) OperationEntry {
	if l == nil {
		return e
	}
	if e.T.IsZero() {
		e.T = time.Now()
	}
	e.Time = e.T.Format("15:04:05.000")
	e.SessionID = l.sessionID
	stored := e
	if e.Details != nil {
		stored.Details = cloneDetails(e.Details)
		stored.detailText = detailsSearchText(stored.Details)
	}
	l.mu.Lock()
	l.nextID++
	e.ID = l.nextID
	stored.ID = e.ID
	if len(l.entries) < operationHistoryLimit {
		l.entries = append(l.entries, stored)
	} else {
		l.entries[l.start] = stored
		l.start = (l.start + 1) % operationHistoryLimit
	}
	l.mu.Unlock()
	return e
}

// Recent returns up to n entries in insertion order, newest last.
func (l *OperationLog) Recent(n int) []OperationEntry {
	if l == nil || n <= 0 {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	start := len(l.entries) - n
	if start < 0 {
		start = 0
	}
	out := make([]OperationEntry, len(l.entries)-start)
	for i := range out {
		out[i] = l.entryAtLocked(start + i)
	}
	return out
}

type OperationQuery struct {
	Method       string
	StatusClass  int
	ErrorOnly    bool
	Text         string
	Path         string
	From         time.Time
	To           time.Time
	MinLatencyMs int64
	Limit        int
	Offset       int
	OrderDesc    bool
}

type OperationQueryResult struct {
	SessionID string           `json:"session_id"`
	Total     int              `json:"total"`
	Matched   int              `json:"matched"`
	Limit     int              `json:"limit"`
	Offset    int              `json:"offset"`
	Order     string           `json:"order"`
	Entries   []OperationEntry `json:"entries"`
}

func (l *OperationLog) Query(q OperationQuery) OperationQueryResult {
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Limit > 1000 {
		q.Limit = 1000
	}
	if q.Offset < 0 {
		q.Offset = 0
	}
	if l == nil {
		return OperationQueryResult{Limit: q.Limit, Offset: q.Offset, Order: queryOrder(q)}
	}

	method := strings.ToLower(q.Method)
	text := strings.ToLower(q.Text)
	path := strings.ToLower(q.Path)

	matches := func(e OperationEntry) bool {
		if method != "" && !strings.Contains(strings.ToLower(e.Method), method) {
			return false
		}
		if q.StatusClass > 0 && e.Status/100 != q.StatusClass/100 {
			return false
		}
		if q.ErrorOnly && e.Err == "" && e.Status < 400 {
			return false
		}
		if !q.From.IsZero() && e.T.Before(q.From) {
			return false
		}
		if !q.To.IsZero() && e.T.After(q.To) {
			return false
		}
		if q.MinLatencyMs > 0 && e.LatencyMs < q.MinLatencyMs {
			return false
		}
		if path != "" && !strings.Contains(strings.ToLower(e.Path), path) {
			return false
		}
		if text != "" && !operationContains(e, text) {
			return false
		}
		return true
	}

	l.mu.RLock()
	total := len(l.entries)
	filtered := make([]OperationEntry, 0, min(q.Limit, total))
	matched := 0
	for i := 0; i < total; i++ {
		logicalIndex := i
		if q.OrderDesc {
			logicalIndex = total - 1 - i
		}
		e := l.entryAtLocked(logicalIndex)
		if !matches(e) {
			continue
		}
		matched++
		if matched <= q.Offset || len(filtered) >= q.Limit {
			continue
		}
		filtered = append(filtered, e)
	}
	sessionID := l.sessionID
	l.mu.RUnlock()

	return OperationQueryResult{
		SessionID: sessionID,
		Total:     total,
		Matched:   matched,
		Limit:     q.Limit,
		Offset:    q.Offset,
		Order:     queryOrder(q),
		Entries:   filtered,
	}
}

func (l *OperationLog) entryAtLocked(logicalIndex int) OperationEntry {
	physicalIndex := l.start + logicalIndex
	if physicalIndex >= len(l.entries) {
		physicalIndex -= len(l.entries)
	}
	return l.entries[physicalIndex]
}

func queryOrder(q OperationQuery) string {
	if q.OrderDesc {
		return "desc"
	}
	return "asc"
}

func cloneDetails(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func operationContains(e OperationEntry, needle string) bool {
	hay := strings.ToLower(e.Method + " " + e.Source + " " + e.Path + " " + e.Err + " " + e.detailText)
	return strings.Contains(hay, needle)
}

func detailsSearchText(details map[string]any) string {
	if len(details) == 0 {
		return ""
	}
	b, err := json.Marshal(details)
	if err != nil {
		return ""
	}
	return string(b)
}

func detailsAnyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return ""
		}
		return string(b)
	}
}
