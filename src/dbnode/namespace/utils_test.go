package namespace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMustBuildMetadatas(t *testing.T) {
	tests := []struct {
		name         string
		indexEnabled bool
		ids          []string
	}{
		{
			name:         "index enabled with multiple IDs",
			indexEnabled: true,
			ids:          []string{"ns1", "ns2"},
		},
		{
			name:         "index disabled with single ID",
			indexEnabled: false,
			ids:          []string{"ns3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadatas := MustBuildMetadatas(tt.indexEnabled, tt.ids...)
			assert.Equal(t, len(tt.ids), len(metadatas))

			for i, md := range metadatas {
				assert.Equal(t, tt.ids[i], md.ID().String())
				assert.Equal(t, tt.indexEnabled, md.Options().IndexOptions().Enabled())
			}
		})
	}
}
