package attribution

import "github.com/m3db/m3/src/metrics/attribution/rules"

// RuleMatcher matches metrics to namespace attribution rules.
type RuleMatcher interface {
	// Match returns the rules that match the metric.
	// The returned slice could contain duplicates.
	Match(metricID []byte) []*rules.ResolvedRule

	// Update updates the attributor with new namespaces and their tag filters.
	Update(ruleSet rules.RuleSet) error
}
