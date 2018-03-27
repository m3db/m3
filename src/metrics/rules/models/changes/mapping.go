package changes

import "github.com/m3db/m3metrics/rules/models"

// MappingRuleChange is a mapping rule diff.
type MappingRuleChange struct {
	Op       Op                  `json:"op"`
	RuleID   *string             `json:"ruleID,omitempty"`
	RuleData *models.MappingRule `json:"ruleData,omitempty"`
}

type mappingRuleChangesByOpAscNameAscIDAsc []MappingRuleChange

func (a mappingRuleChangesByOpAscNameAscIDAsc) Len() int      { return len(a) }
func (a mappingRuleChangesByOpAscNameAscIDAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a mappingRuleChangesByOpAscNameAscIDAsc) Less(i, j int) bool {
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
	// This should not happen
	return false
}
