package changes

import "github.com/m3db/m3metrics/rules/models"

// RollupRuleChange is a rollup rule diff.
type RollupRuleChange struct {
	Op       Op                 `json:"op"`
	RuleID   *string            `json:"ruleID,omitempty"`
	RuleData *models.RollupRule `json:"to,omitempty"`
}

type rollupRuleChangesByOpAscNameAscIDAsc []RollupRuleChange

func (a rollupRuleChangesByOpAscNameAscIDAsc) Len() int      { return len(a) }
func (a rollupRuleChangesByOpAscNameAscIDAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a rollupRuleChangesByOpAscNameAscIDAsc) Less(i, j int) bool {
	if a[i].Op < a[j].Op {
		return true
	}
	if a[i].Op > a[j].Op {
		return false
	}
	// For adds and changes.
	if a[i].RuleData != nil && a[j].RuleData != nil {
		return a[i].RuleData.Name < a[j].RuleData.Name
	}
	// For deletes.
	if a[i].RuleID != nil && a[j].RuleID != nil {
		return *a[i].RuleID < *a[j].RuleID
	}
	// This should not happen.
	return false
}
