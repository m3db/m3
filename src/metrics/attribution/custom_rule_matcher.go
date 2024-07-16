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
	ruleTree        *tagfiltertree.Tree[*rules.ResolvedRule]
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
) []*rules.ResolvedRule {
	if m.ruleTree == nil {
		return nil
	}

	tags := make(map[string]string, 0)
	iter := m.tagMatchOptions.SortedTagIteratorFn(metricID)
	for iter.Next() {
		tagName, tagValue := iter.Current()
		tags[string(tagName)] = string(tagValue)
	}

	m.ruleTreeLock.RLock()
	defer m.ruleTreeLock.RUnlock()

	return m.ruleTree.Match(tags)
}

func (m *customRuleMatcher) Update(ruleSet rules.RuleSet) error {
	ruleTree := tagfiltertree.New[*rules.ResolvedRule]()
	for _, rule := range ruleSet.Rules() {
		// add the namespace to the ruleTree.
		for _, tagFilter := range rule.TagFilters() {
			tags, err := tagfiltertree.TagsFromTagFilter(tagFilter)
			if err != nil {
				return err
			}
			ruleTree.AddTagFilter(tags, &rules.ResolvedRule{
				Rule: rule,
			})
		}
	}

	m.ruleTreeLock.Lock()
	defer m.ruleTreeLock.Unlock()

	m.ruleTree = ruleTree
	return nil
}
