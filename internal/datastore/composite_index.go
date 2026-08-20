package datastore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"gopkg.in/yaml.v3"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// IndexManager connects index.yaml definitions, query discovery, and physical storage.
type IndexManager struct {
	store         *storage.Store
	generatedPath string
	mu            sync.Mutex
	templates     map[string]storage.DsCompositeIndex
}

func NewIndexManager(store *storage.Store, generatedPath string) *IndexManager {
	return &IndexManager{store: store, generatedPath: generatedPath, templates: make(map[string]storage.DsCompositeIndex)}
}

// LoadIndexFiles loads user and generated definitions and builds them for existing projects.
func (m *IndexManager) LoadIndexFiles(ctx context.Context, configuredPath string) error {
	for _, source := range []struct{ path, name string }{{configuredPath, "configured"}, {m.generatedPath, "generated"}} {
		if source.path == "" {
			continue
		}
		data, err := os.ReadFile(source.path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read %s index config: %w", source.name, err)
		}
		indexes, err := parseIndexDefinitions(data)
		if err != nil {
			return fmt.Errorf("parse %s index config: %w", source.name, err)
		}
		for _, idx := range indexes {
			idx.Source = source.name
			m.templates[idx.ID] = idx
		}
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
	return nil
}

func (m *IndexManager) ensureTemplates(ctx context.Context, project string, wait bool) error {
	m.mu.Lock()
	templates := make([]storage.DsCompositeIndex, 0, len(m.templates))
	for _, template := range m.templates {
		templates = append(templates, template)
	}
	m.mu.Unlock()
	for _, template := range templates {
		idx := template
		idx.Project = project
		current, _, err := m.store.EnsureDsCompositeIndex(ctx, idx, wait)
		if err != nil {
			return err
		}
		if wait && current.State == storage.DsIndexError {
			return fmt.Errorf("index %s (%s): %s", current.ID, current.Kind, current.Error)
		}
	}
	return nil
}

// PrepareQuery returns a ready index when available and schedules a missing one.
func (m *IndexManager) PrepareQuery(ctx context.Context, project string, q *datastorepb.Query, ancestor bool) (*storage.DsCompositeIndex, error) {
	if err := m.ensureTemplates(ctx, project, false); err != nil {
		return nil, err
	}
	idx, needed := queryIndexDefinition(q, ancestor)
	if !needed {
		return nil, nil
	}
	idx.Project = project
	current, created, err := m.store.EnsureDsCompositeIndex(ctx, idx, false)
	if err != nil {
		return nil, err
	}
	if created {
		_ = m.recordGenerated(idx)
	}
	if current.State != storage.DsIndexReady {
		return nil, nil
	}
	return &current, nil
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
	for _, order := range q.Order {
		name := order.Property.GetName()
		if name == "__key__" {
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

func (m *IndexManager) recordGenerated(idx storage.DsCompositeIndex) error {
	if m.generatedPath == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.templates[idx.ID] = idx
	return m.writeGeneratedLocked()
}

func (m *IndexManager) removeGenerated(id string) error {
	if m.generatedPath == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.templates, id)
	return m.writeGeneratedLocked()
}

func (m *IndexManager) writeGeneratedLocked() error {
	var indexes []storage.DsCompositeIndex
	for _, candidate := range m.templates {
		if candidate.Source == "generated" {
			indexes = append(indexes, candidate)
		}
	}
	storage.SortDsCompositeIndexes(indexes)
	type yamlProperty struct {
		Name      string `yaml:"name"`
		Direction string `yaml:"direction,omitempty"`
	}
	type yamlIndex struct {
		Kind       string         `yaml:"kind"`
		Ancestor   string         `yaml:"ancestor,omitempty"`
		Properties []yamlProperty `yaml:"properties"`
	}
	var document struct {
		Indexes []yamlIndex `yaml:"indexes"`
	}
	for _, candidate := range indexes {
		y := yamlIndex{Kind: candidate.Kind}
		if candidate.Ancestor {
			y.Ancestor = "yes"
		}
		for _, p := range candidate.Properties {
			direction := "asc"
			if p.Desc {
				direction = "desc"
			}
			y.Properties = append(y.Properties, yamlProperty{Name: p.Name, Direction: direction})
		}
		document.Indexes = append(document.Indexes, y)
	}
	data, err := yaml.Marshal(document)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.generatedPath), 0755); err != nil {
		return err
	}
	tmp := m.generatedPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, m.generatedPath)
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
