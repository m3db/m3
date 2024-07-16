package attribution

import (
	"sync"

	"github.com/m3db/m3/src/metrics/attribution/rules"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/x/watch"
)

// Attributor is responsible for attributing metrics to namespaces.
type Attributor interface {
	// Match return a list of namespaces that the metricID should
	// be attributed to.
	Match(metricID []byte) ([]*rules.ResolvedRule, error)

	// Update updates the attributor with new namespaces and their tag filters.
	Update(ruleSet rules.RuleSet) error

	// Init initializes the Attributor.
	Init() error

	// Close closes the Attributor.
	Close()
}

type attributor struct {
	matcherLock    sync.RWMutex
	w              watch.Value
	ruleService    rules.Service
	defaultMatcher RuleMatcher
	customMatcher  RuleMatcher
	opts           Options
	processFn      watch.ProcessFn
}

// NewAttributor creates a new Attributor.
func NewAttributor(opts Options) Attributor {
	a := &attributor{
		opts:           opts,
		ruleService:    opts.RulesService(),
		defaultMatcher: opts.DefaultRuleMatcher(),
	}
	a.processFn = a.process
	return a
}

func (a *attributor) Match(metricID []byte) ([]*rules.ResolvedRule, error) {
	var (
		ruleMap map[string]*rules.ResolvedRule
	)

	var customMatcher RuleMatcher
	{
		a.matcherLock.RLock()
		customMatcher = a.customMatcher
		a.matcherLock.RUnlock()
	}

	ruleMap = make(map[string]*rules.ResolvedRule)
	for _, rule := range customMatcher.Match(metricID) {
		ruleMap[rule.Name()] = rule
	}

	for _, rule := range a.defaultMatcher.Match(metricID) {
		ruleMap[rule.Name()] = rule
	}

	rules := make([]*rules.ResolvedRule, 0, len(ruleMap))
	for _, rule := range ruleMap {
		rules = append(rules, rule)
	}

	return rules, nil
}

func (a *attributor) Update(ruleSet rules.RuleSet) error {
	return a.customMatcher.Update(ruleSet)
}

func (a *attributor) Init() error {
	newUpdatableFn := func() (watch.Updatable, error) {
		return a.ruleService.Watch(a.opts.CustomRulesKVKey())
	}
	getUpdateFn := func(value watch.Updatable) (interface{}, error) {
		rs, err := value.(rules.Watch).Get()
		if err != nil {
			return nil, err
		}
		return rs, nil
	}
	wOpts := watch.NewOptions().
		SetNewUpdatableFn(newUpdatableFn).
		SetGetUpdateFn(getUpdateFn).
		SetProcessFn(a.processFn).
		SetInitWatchTimeout(a.opts.KVWatchInitTimeout()).
		SetInstrumentOptions(a.opts.InstrumentOptions()).
		SetKey(a.opts.CustomRulesKVKey())

	w := watch.NewValue(wOpts)
	if err := w.Watch(); err != nil {
		return err
	}
	a.w = w
	return nil
}

func (a *attributor) Close() {
	a.w.Unwatch()
}

func (a *attributor) process(value interface{}) error {
	rs, ok := value.(rules.RuleSet)
	if !ok {
		return nil
	}

	ruleSet := rs
	customMatcher, err := NewCustomRuleMatcher(
		filters.TagMatchOptions{
			NameAndTagsFn:       a.opts.TagFilterOptions().NameAndTagsFn,
			SortedTagIteratorFn: a.opts.TagFilterOptions().SortedTagIteratorFn,
		},
		a.opts.InstrumentOptions(),
	)
	if err != nil {
		return err
	}

	{
		a.matcherLock.Lock()
		a.customMatcher = customMatcher
		a.matcherLock.Unlock()
	}

	if err := a.Update(ruleSet); err != nil {
		return err
	}

	return nil
}
