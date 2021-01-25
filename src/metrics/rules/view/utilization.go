package view

type UtilizationRule struct {
	ID                  string        `json:"id,omitempty`
	Name                string        `json:"name" validate:"required"`
	Tombstoned          bool          `json:"tombstoned"`
	CutoverMillis       int64         `json:"cutoverMillis,omitempty"`
	Value               string        `json:"value,omitempty"`
	Filter              string        `json:"filter" validate:"required"`
	Targets             rollupTargets `json:"targets" validate:"required,dive,required"`
	LastUpdatedBy       string        `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64         `json:"lastUpdatedAtMillis"`
}

// Equal determines whether two utilization rules are equal.
func (u *UtilizationRule) Equal(other *UtilizationRule) bool {
	if u == nil && other == nil {
		return true
	}
	if u == nil || other == nil {
		return false
	}
	return u.ID == other.ID &&
		u.Name == other.Name &&
		u.Filter == other.Filter &&
		u.Value == other.Value &&
		u.Targets.Equal(other.Targets)
}

// UtilizationRules belong to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type UtilizationRules map[string][]UtilizationRule

// UtilizationRulesByNameAsc sorts utilization rules by name in ascending order.
type UtilizationRulesByNameAsc []UtilizationRule

func (a UtilizationRulesByNameAsc) Len() int           { return len(a) }
func (a UtilizationRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a UtilizationRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }

// UtilizationRuleSnapshots contains a list of utilization rule snapshots.
type UtilizationRuleSnapshots struct {
	UtilizationRules []UtilizationRule `json:"utilizationRules"`
}
