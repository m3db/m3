package aggregator

import (
	"testing"
)

func TestExtractSourceTag(t *testing.T) {
	sourceTag := "service"
	tests := []struct {
		name   string
		id     string
		want   string
		wantOk bool
	}{
		{
			name:   "valid source tag with more tags",
			id:     "metricname,service=myservice,env=test",
			want:   "myservice",
			wantOk: true,
		},
		{
			name:   "valid source tag at end",
			id:     "metricname,service=myservice",
			want:   "myservice",
			wantOk: true,
		},
		{
			name:   "service tag not found",
			id:     "metricname,env=test",
			want:   "",
			wantOk: false,
		},
		{
			name:   "empty service tag value",
			id:     "metricname,service=",
			want:   "",
			wantOk: false,
		},
		{
			name:   "empty ID",
			id:     "",
			want:   "",
			wantOk: false,
		},
		{
			name:   "ID shorter than minimum length",
			id:     "s",
			want:   "",
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ExtractSourceTag(tt.id, sourceTag)
			if ok != tt.wantOk {
				t.Errorf("ExtractSourceTag() ok = %v, want %v", ok, tt.wantOk)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractSourceTag() = %v, want %v", got, tt.want)
			}
		})
	}
}
