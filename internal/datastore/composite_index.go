package datastore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// IndexManager connects index.yaml definitions, query discovery, and physical storage.
type IndexManager struct {
	store            *storage.Store
	mu               sync.Mutex
	templates        map[string]storage.DsCompositeIndex
	templatesApplied map[string]bool
}

func NewIndexManager(store *storage.Store) *IndexManager {
	return &IndexManager{store: store, templates: make(map[string]storage.DsCompositeIndex), templatesApplied: make(map[string]bool)}
}

// LoadIndexFiles loads configured definitions and builds them for existing projects.
func (m *IndexManager) LoadIndexFiles(ctx context.Context, configuredPath string) error {
	if configuredPath != "" {
		data, err := os.ReadFile(configuredPath)
		if err != nil {
			return fmt.Errorf("read configured index config: %w", err)
		}
		indexes, err := parseIndexDefinitions(data)
		if err != nil {
			return fmt.Errorf("parse configured index config: %w", err)
		}
		templates := make(map[string]storage.DsCompositeIndex, len(indexes))
		for _, idx := range indexes {
			idx.Source = "configured"
			templates[idx.ID] = idx
		}
		m.mu.Lock()
		m.templates = templates
		clear(m.templatesApplied)
		m.mu.Unlock()
	}
	projects, err := m.store.ListDsProjects()
	if err != nil {
		return err
	}
	for _, project := range projects {
		if err := m.ensureTemplates(ctx, project, true); err != nil {
			return err
		}
	}
	indexes, err := m.store.ListDsCompositeIndexes("")
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if idx.State != storage.DsIndexCreating {
			continue
		}
		if _, _, err := m.store.EnsureDsCompositeIndex(ctx, idx, true); err != nil {
			return fmt.Errorf("resume index %s (%s): %w", idx.ID, idx.Kind, err)
		}
	}
	return nil
}

func (m *IndexManager) ensureTemplates(ctx context.Context, project string, wait bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.templates) == 0 || m.templatesApplied[project] {
		return nil
	}
	ready := true
	for _, template := range m.templates {
		idx := template
		idx.Project = project
		current, _, err := m.store.EnsureDsCompositeIndex(ctx, idx, wait)
		if err != nil {
			return err
		}
		if wait && current.State == storage.DsIndexError {
			return fmt.Errorf("index %s (%s): %s", current.ID, current.Kind, current.Error)
		}
		ready = ready && current.State == storage.DsIndexReady
	}
	if ready {
		m.templatesApplied[project] = true
	}
	return nil
}

func (m *IndexManager) invalidateTemplates(project string) {
	m.mu.Lock()
	delete(m.templatesApplied, project)
	m.mu.Unlock()
}

// PrepareQuery returns a ready index, building a missing one before returning.
func (m *IndexManager) PrepareQuery(ctx context.Context, project string, q *datastorepb.Query, ancestor bool, pinnedIndexID string) (*storage.DsCompositeIndex, error) {
	if err := m.ensureTemplates(ctx, project, false); err != nil {
		return nil, err
	}
	selected, err := m.selectQueryIndex(ctx, project, q, ancestor, pinnedIndexID)
	if err != nil || selected == nil {
		return nil, err
	}
	current, _, err := m.store.EnsureDsCompositeIndex(ctx, *selected, true)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, err
	}
	if current.State == storage.DsIndexError {
		return nil, status.Errorf(codes.FailedPrecondition, "composite index %s for kind %s failed: %s", current.ID, current.Kind, current.Error)
	}
	if current.State != storage.DsIndexReady {
		return nil, status.Errorf(codes.FailedPrecondition, "composite index %s for kind %s is building; retry the query when it is ready", current.ID, current.Kind)
	}
	return &current, nil
}

func (m *IndexManager) selectQueryIndex(ctx context.Context, project string, q *datastorepb.Query, ancestor bool, pinnedIndexID string) (*storage.DsCompositeIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, needed := queryIndexDefinition(q, ancestor)
	if !needed {
		return nil, nil
	}
	idx.Project = project
	if pinnedIndexID != "" {
		candidate, err := m.store.GetDsCompositeIndex(project, pinnedIndexID)
		if errors.Is(err, storage.ErrIndexNotFound) {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		if err != nil {
			return nil, err
		}
		if !compositeIndexSatisfiesQuery(candidate, idx, q.Filter) || candidate.State != storage.DsIndexReady && candidate.State != storage.DsIndexCreating {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		return &candidate, nil
	}
	allowance := queryOptimizerAllowance(ctx)
	if allowance <= 0 {
		return nil, nil
	}
	// The exact ready definition is the best-ranked candidate. Resolve it
	// directly without loading or traversing the project's index catalog.
	selected, err := m.store.GetDsCompositeIndex(project, idx.ID)
	if err != nil && !errors.Is(err, storage.ErrIndexNotFound) {
		return nil, err
	}
	found := err == nil && (selected.State == storage.DsIndexReady || selected.State == storage.DsIndexCreating) && compositeIndexSatisfiesQuery(selected, idx, q.Filter)
	if found && selected.State == storage.DsIndexReady {
		return &selected, nil
	}
	examined := 0
	exhausted, err := m.store.VisitDsCompositeIndexes(ctx, project, func(candidate storage.DsCompositeIndex) bool {
		examined++
		if compositeIndexSatisfiesQuery(candidate, idx, q.Filter) && (candidate.State == storage.DsIndexReady || candidate.State == storage.DsIndexCreating) {
			if !found || preferCompositeIndex(candidate, selected, idx.ID) {
				selected, found = candidate, true
			}
		}
		return examined < allowance
	})
	if err != nil {
		return nil, err
	}
	if found {
		return &selected, nil
	}
	if !exhausted {
		return nil, nil
	} // Exact scan; don't create an index after an incomplete search.
	return &idx, nil
}

func compositeIndexSatisfiesQuery(candidate, required storage.DsCompositeIndex, filter *datastorepb.Filter) bool {
	if candidate.Kind != required.Kind || candidate.Ancestor != required.Ancestor || len(candidate.Properties) != len(required.Properties) {
		return false
	}
	equality := map[string]struct{}{}
	collectFilterProperties(filter, equality, map[string]struct{}{})
	delete(equality, "__key__")
	if len(equality) > len(candidate.Properties) {
		return false
	}
	seen := make(map[string]struct{}, len(equality))
	for i := 0; i < len(equality); i++ {
		name := candidate.Properties[i].Name
		if _, ok := equality[name]; !ok {
			return false
		}
		if _, duplicate := seen[name]; duplicate {
			return false
		}
		seen[name] = struct{}{}
	}
	for i := len(equality); i < len(required.Properties); i++ {
		if candidate.Properties[i] != required.Properties[i] {
			return false
		}
	}
	return true
}

func preferCompositeIndex(candidate, current storage.DsCompositeIndex, exactID string) bool {
	rank := func(index storage.DsCompositeIndex) [3]int {
		state := 1
		if index.State == storage.DsIndexReady {
			state = 0
		}
		exact := 1
		if index.ID == exactID {
			exact = 0
		}
		source := 2
		switch index.Source {
		case "configured":
			source = 0
		case "generated":
			source = 1
		}
		return [3]int{state, exact, source}
	}
	candidateRank, currentRank := rank(candidate), rank(current)
	for i := range candidateRank {
		if candidateRank[i] != currentRank[i] {
			return candidateRank[i] < currentRank[i]
		}
	}
	return candidate.ID < current.ID
}

func queryIndexDefinition(q *datastorepb.Query, ancestor bool) (storage.DsCompositeIndex, bool) {
	if q == nil || len(q.Kind) == 0 {
		return storage.DsCompositeIndex{}, false
	}
	equality := map[string]struct{}{}
	other := map[string]struct{}{}
	collectFilterProperties(q.Filter, equality, other)
	var equalityNames []string
	for name := range equality {
		if name != "__key__" {
			equalityNames = append(equalityNames, name)
		}
	}
	sort.Strings(equalityNames)
	properties := make([]storage.DsIndexProperty, 0, len(equalityNames)+len(q.Order)+len(other))
	seen := map[string]struct{}{}
	for _, name := range equalityNames {
		properties = append(properties, storage.DsIndexProperty{Name: name})
		seen[name] = struct{}{}
	}
	for i, order := range q.Order {
		name := order.Property.GetName()
		// The storage identity suffix already supplies an ascending final
		// key. Descending ties need a typed key component in the index schema.
		if name == "__key__" && i == len(q.Order)-1 && order.Direction != datastorepb.PropertyOrder_DESCENDING {
			continue
		}
		if _, ok := seen[name]; ok {
			for i := range properties {
				if properties[i].Name == name {
					properties[i].Desc = order.Direction == datastorepb.PropertyOrder_DESCENDING
				}
			}
			continue
		}
		properties = append(properties, storage.DsIndexProperty{Name: name, Desc: order.Direction == datastorepb.PropertyOrder_DESCENDING})
		seen[name] = struct{}{}
	}
	var otherNames []string
	for name := range other {
		if name != "__key__" {
			if _, ok := seen[name]; !ok {
				otherNames = append(otherNames, name)
			}
		}
	}
	sort.Strings(otherNames)
	for _, name := range otherNames {
		properties = append(properties, storage.DsIndexProperty{Name: name})
		seen[name] = struct{}{}
	}
	for _, projection := range q.Projection {
		name := projection.Property.GetName()
		if name != "__key__" {
			if _, ok := seen[name]; !ok {
				properties = append(properties, storage.DsIndexProperty{Name: name})
				seen[name] = struct{}{}
			}
		}
	}
	for _, distinct := range q.DistinctOn {
		name := distinct.GetName()
		if name != "__key__" {
			if _, ok := seen[name]; !ok {
				properties = append(properties, storage.DsIndexProperty{Name: name})
				seen[name] = struct{}{}
			}
		}
	}
	needed := len(properties) > 1 || (ancestor && len(properties) > 0) || len(q.Order) > 1 || len(q.Projection) > 0 || len(q.DistinctOn) > 0
	if !needed {
		return storage.DsCompositeIndex{}, false
	}
	idx := storage.DsCompositeIndex{Kind: q.Kind[0].Name, Ancestor: ancestor, Properties: properties, Source: "generated"}
	idx.ID = storage.DsCompositeIndexID(idx.Kind, idx.Ancestor, idx.Properties)
	return idx, true
}

func collectFilterProperties(f *datastorepb.Filter, equality, other map[string]struct{}) {
	if f == nil {
		return
	}
	switch x := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		name := x.PropertyFilter.Property.GetName()
		if x.PropertyFilter.Op == datastorepb.PropertyFilter_EQUAL {
			equality[name] = struct{}{}
		} else {
			other[name] = struct{}{}
		}
	case *datastorepb.Filter_CompositeFilter:
		for _, child := range x.CompositeFilter.Filters {
			collectFilterProperties(child, equality, other)
		}
	}
}

func equalityFilterValues(f *datastorepb.Filter) (map[string]*datastorepb.Value, bool) {
	values := map[string]*datastorepb.Value{}
	var walk func(*datastorepb.Filter) bool
	walk = func(filter *datastorepb.Filter) bool {
		if filter == nil {
			return true
		}
		switch x := filter.FilterType.(type) {
		case *datastorepb.Filter_PropertyFilter:
			if x.PropertyFilter.Op != datastorepb.PropertyFilter_EQUAL {
				return false
			}
			values[x.PropertyFilter.Property.GetName()] = x.PropertyFilter.Value
			return true
		case *datastorepb.Filter_CompositeFilter:
			if x.CompositeFilter.Op != datastorepb.CompositeFilter_AND {
				return false
			}
			for _, child := range x.CompositeFilter.Filters {
				if !walk(child) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	return values, walk(f)
}

func parseIndexDefinitions(data []byte) ([]storage.DsCompositeIndex, error) {
	var document struct {
		Indexes []struct {
			Kind       string `yaml:"kind"`
			Ancestor   string `yaml:"ancestor"`
			Properties []struct {
				Name      string `yaml:"name"`
				Direction string `yaml:"direction"`
			} `yaml:"properties"`
		} `yaml:"indexes"`
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	var out []storage.DsCompositeIndex
	for _, entry := range document.Indexes {
		ancestor := strings.EqualFold(entry.Ancestor, "yes") || strings.EqualFold(entry.Ancestor, "true") || entry.Ancestor == "1" || strings.EqualFold(entry.Ancestor, "all")
		idx := storage.DsCompositeIndex{Kind: entry.Kind, Ancestor: ancestor}
		for _, p := range entry.Properties {
			idx.Properties = append(idx.Properties, storage.DsIndexProperty{Name: p.Name, Desc: strings.EqualFold(p.Direction, "desc") || strings.EqualFold(p.Direction, "descending")})
		}
		idx.ID = storage.DsCompositeIndexID(idx.Kind, idx.Ancestor, idx.Properties)
		out = append(out, idx)
	}
	return out, nil
}
