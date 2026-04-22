// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"fmt"
	"path"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// SelectorResolver expands YAML selectors against a catalog listing.
type SelectorResolver interface {
	Resolve(ctx context.Context, cfg *config.Config, catalog []socrata.CatalogEntry, only []string) ([]DatasetTarget, error)
}

// DefaultSelectorResolver is the Phase 1 implementation.
type DefaultSelectorResolver struct{}

func (r *DefaultSelectorResolver) Resolve(
	ctx context.Context, cfg *config.Config, catalog []socrata.CatalogEntry, only []string,
) ([]DatasetTarget, error) {
	byID := make(map[string]socrata.CatalogEntry, len(catalog))
	for _, e := range catalog {
		byID[e.ID] = e
	}

	included := make(map[string]struct{})
	for _, sel := range cfg.Include {
		for _, e := range catalog {
			if matchSelector(sel, e) {
				included[e.ID] = struct{}{}
			}
		}
	}
	for _, sel := range cfg.Exclude {
		for id := range included {
			if matchSelector(sel, byID[id]) {
				delete(included, id)
			}
		}
	}

	if len(only) > 0 {
		onlySet := make(map[string]struct{}, len(only))
		for _, id := range only {
			onlySet[id] = struct{}{}
			if _, ok := included[id]; !ok {
				return nil, fmt.Errorf("--only %s: not in resolved selector set", id)
			}
		}
		for id := range included {
			if _, ok := onlySet[id]; !ok {
				delete(included, id)
			}
		}
	}

	if len(included) == 0 {
		return nil, fmt.Errorf("no datasets matched the include selectors")
	}

	out := make([]DatasetTarget, 0, len(included))
	for id := range included {
		out = append(out, DatasetTarget{
			ID:        id,
			Name:      byID[id].Name,
			Effective: cfg.EffectiveFor(id),
		})
	}
	return out, nil
}

func matchSelector(sel config.Selector, e socrata.CatalogEntry) bool {
	switch {
	case sel.ID != "":
		return sel.ID == e.ID
	case sel.Name != "":
		return globMatch(sel.Name, e.Name)
	case sel.Category != "":
		return globMatch(sel.Category, e.Category)
	case sel.Tag != "":
		for _, t := range e.Tags {
			if globMatch(sel.Tag, t) {
				return true
			}
		}
		return false
	}
	return false
}

func globMatch(pattern, s string) bool {
	ok, err := path.Match(pattern, s)
	if err != nil {
		return false // malformed pattern — treat as non-match
	}
	return ok
}
