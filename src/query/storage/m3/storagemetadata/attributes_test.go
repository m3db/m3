package storagemetadata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAttributes_CombinedWith(t *testing.T) {
	tests := []struct {
		name  string
		given Attributes
		with  Attributes
		want  Attributes
	}{
		{
			name: "aggregated with unaggregated",
			given: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  5 * time.Minute},
			with: Attributes{
				MetricsType: UnaggregatedMetricsType,
				Retention:   3 * time.Hour,
				Resolution:  time.Minute,
			},
			want: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  5 * time.Minute,
			},
		},

		{
			name: "unaggregated with aggregated",
			given: Attributes{
				MetricsType: UnaggregatedMetricsType,
				Retention:   3 * time.Hour,
				Resolution:  time.Minute,
			},
			with: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  5 * time.Minute,
			},
			want: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  5 * time.Minute,
			},
		},

		{
			name: "aggregated with aggregated",
			given: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  5 * time.Minute},
			with: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   3 * time.Hour,
				Resolution:  10 * time.Minute,
			},
			want: Attributes{
				MetricsType: AggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  10 * time.Minute,
			},
		},

		{
			name: "unaggregated with unaggregated",
			given: Attributes{
				MetricsType: UnaggregatedMetricsType,
				Retention:   3 * time.Hour,
				Resolution:  10 * time.Minute,
			},
			with: Attributes{
				MetricsType: UnaggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  5 * time.Minute,
			},
			want: Attributes{
				MetricsType: UnaggregatedMetricsType,
				Retention:   12 * time.Hour,
				Resolution:  10 * time.Minute,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.given.CombinedWith(tt.with), "CombinedWith(%v)")
		})
	}
}
