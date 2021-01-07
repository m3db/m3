package view

// UtilizationRule is a utilization rule.
type UtilizationRule struct {
	ID                  string         `json:"id,omitempty`
	Name                string         `json:"name" validate:"required"`
	Tombstoned          bool           `json:"tombstoned"`
	CutoverMillis       int64          `json:"cutoverMillis,omitempty"`
	Value               string         `json:"value,omitempty"`
	Filter              string         `json:"filter" validate:"required"`
	Targets             []RollupTarget `json:"targets" validate:"required,dive,required"`
	LastUpdatedBy       string         `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64          `json:"lastUpdatedAtMillis"`
}
