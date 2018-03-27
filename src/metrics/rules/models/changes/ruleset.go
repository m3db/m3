package changes

import (
	"sort"
)

// RuleSetChanges is a ruleset diff.
type RuleSetChanges struct {
	Namespace          string              `json:"namespace"`
	MappingRuleChanges []MappingRuleChange `json:"mappingRuleChanges"`
	RollupRuleChanges  []RollupRuleChange  `json:"rollupRuleChanges"`
}

// Sort sorts the ruleset diff by op and rule names.
func (d *RuleSetChanges) Sort() {
	sort.Sort(mappingRuleChangesByOpAscNameAscIDAsc(d.MappingRuleChanges))
	sort.Sort(rollupRuleChangesByOpAscNameAscIDAsc(d.RollupRuleChanges))
}
