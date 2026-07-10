package server

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	mu        sync.Mutex
	sessionID string
	nextID    int64
	entries   []OperationEntry
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
	if e.Details != nil {
		e.Details = cloneDetails(e.Details)
		e.detailText = detailsSearchText(e.Details)
	}
	l.mu.Lock()
	l.nextID++
	e.ID = l.nextID
	l.entries = append(l.entries, e)
	l.mu.Unlock()
	return e
}

// Recent returns up to n entries in insertion order, newest last.
func (l *OperationLog) Recent(n int) []OperationEntry {
	if l == nil || n <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	start := len(l.entries) - n
	if start < 0 {
		start = 0
	}
	return cloneEntries(l.entries[start:])
}

type OperationQuery struct {
	Method       string
	Source       string
	StatusClass  int
	ErrorOnly    bool
	Text         string
	Path         string
	Collection   string
	Detail       string
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
	source := strings.ToLower(q.Source)
	text := strings.ToLower(q.Text)
	path := strings.ToLower(q.Path)
	collection := strings.ToLower(q.Collection)
	detail := strings.ToLower(q.Detail)

	l.mu.Lock()
	all := cloneEntries(l.entries)
	sessionID := l.sessionID
	l.mu.Unlock()

	filtered := make([]OperationEntry, 0, len(all))
	for _, e := range all {
		if method != "" && !strings.Contains(strings.ToLower(e.Method), method) {
			continue
		}
		if source != "" && !strings.Contains(strings.ToLower(e.Source), source) {
			continue
		}
		if q.StatusClass > 0 && e.Status/100 != q.StatusClass/100 {
			continue
		}
		if q.ErrorOnly && e.Err == "" && e.Status < 400 {
			continue
		}
		if !q.From.IsZero() && e.T.Before(q.From) {
			continue
		}
		if !q.To.IsZero() && e.T.After(q.To) {
			continue
		}
		if q.MinLatencyMs > 0 && e.LatencyMs < q.MinLatencyMs {
			continue
		}
		if path != "" && !strings.Contains(strings.ToLower(e.Path), path) {
			continue
		}
		if collection != "" && !strings.Contains(strings.ToLower(detailsValue(e.Details, "collection")), collection) {
			continue
		}
		if detail != "" && !strings.Contains(strings.ToLower(e.detailText), detail) {
			continue
		}
		if text != "" && !operationContains(e, text) {
			continue
		}
		filtered = append(filtered, e)
	}

	if q.OrderDesc {
		for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
			filtered[i], filtered[j] = filtered[j], filtered[i]
		}
	}
	matched := len(filtered)
	if q.Offset > len(filtered) {
		filtered = nil
	} else {
		filtered = filtered[q.Offset:]
	}
	if len(filtered) > q.Limit {
		filtered = filtered[:q.Limit]
	}

	return OperationQueryResult{
		SessionID: sessionID,
		Total:     len(all),
		Matched:   matched,
		Limit:     q.Limit,
		Offset:    q.Offset,
		Order:     queryOrder(q),
		Entries:   filtered,
	}
}

func queryOrder(q OperationQuery) string {
	if q.OrderDesc {
		return "desc"
	}
	return "asc"
}

func cloneEntries(in []OperationEntry) []OperationEntry {
	out := make([]OperationEntry, len(in))
	for i, e := range in {
		e.Details = cloneDetails(e.Details)
		out[i] = e
	}
	return out
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

func detailsValue(details map[string]any, key string) string {
	if details == nil {
		return ""
	}
	v, ok := details[key]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []string:
		return strings.Join(x, " ")
	case []any:
		parts := make([]string, 0, len(x))
		for _, item := range x {
			parts = append(parts, detailsAnyString(item))
		}
		return strings.Join(parts, " ")
	default:
		return detailsAnyString(v)
	}
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
