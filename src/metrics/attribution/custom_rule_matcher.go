package attribution

import (
	"sync"

	"github.com/m3db/m3/src/metrics/attribution/rules"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/tagfiltertree"
	"github.com/m3db/m3/src/x/instrument"
)

type customRuleMatcher struct {
	ruleTreeLock    sync.RWMutex
	tagMatchOptions filters.TagMatchOptions
	ruleTree        *tagfiltertree.Tree[*rules.Rule]
	instrumentOpts  instrument.Options
}

// NewCustomRuleMatcher creates a new custom rule matcher.
func NewCustomRuleMatcher(
	opts filters.TagMatchOptions,
	instrumentOpts instrument.Options,
) (RuleMatcher, error) {
	return &customRuleMatcher{
		tagMatchOptions: opts,
		instrumentOpts:  instrumentOpts,
	}, nil
}

func (m *customRuleMatcher) Match(
	metricID []byte,
) ([]rules.Result, error) {
	if m.ruleTree == nil {
		return nil, nil
	}

	tags := make(map[string]string, 0)
	iter := m.tagMatchOptions.SortedTagIteratorFn(metricID)
	for iter.Next() {
		tagName, tagValue := iter.Current()
		tags[string(tagName)] = string(tagValue)
	}

	m.ruleTreeLock.RLock()
	defer m.ruleTreeLock.RUnlock()

	matchedRules, err := m.ruleTree.Match(tags)
	if err != nil {
		return nil, err
	}

	results := make([]rules.Result, 0, len(matchedRules))
	for _, rule := range matchedRules {
		result, err := rule.Resolve(tags)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

func (m *customRuleMatcher) Update(ruleSet rules.RuleSet) error {
	ruleTree := tagfiltertree.New[*rules.Rule]()
	for _, rule := range ruleSet.Rules() {
		// add the namespace to the ruleTree.
		for _, tagFilter := range rule.TagFilters() {
			err := ruleTree.AddTagFilter(tagFilter, rule)
			if err != nil {
				return err
			}
		}
	}

	m.ruleTreeLock.Lock()
	defer m.ruleTreeLock.Unlock()

	m.ruleTree = ruleTree
	return nil
}
