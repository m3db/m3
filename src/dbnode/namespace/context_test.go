package namespace

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/x/ident"
)

func TestNewContextFrom(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSchemaHistory := NewMockSchemaHistory(ctrl)
	mocckSchemaDesc := NewMockSchemaDescr(ctrl)
	mockSchemaHistory.EXPECT().GetLatest().Return(mocckSchemaDesc, true)
	mockns := NewMockMetadata(ctrl)
	mockOpts := NewMockOptions(ctrl)
	mockns.EXPECT().ID().Return(ident.StringID("test-ns"))
	mockns.EXPECT().Options().Return(mockOpts)
	mockOpts.EXPECT().SchemaHistory().Return(mockSchemaHistory)
	ctx := NewContextFrom(mockns)
	assert.NotNil(t, ctx)
}

func TestContextFor(t *testing.T) {
	testID := ident.StringID("test-ns")
	tests := []struct {
		name      string
		setup     func(registry *MockSchemaRegistry, schemsDesc *MockSchemaDescr)
		schemaSet bool
	}{
		{
			name: "schema is not set",
			setup: func(registry *MockSchemaRegistry, schemsDescr *MockSchemaDescr) {
				registry.EXPECT().GetLatestSchema(testID).Return(nil, fmt.Errorf("error getting schema"))
			},
		},
		{
			name: "schema is set",
			setup: func(registry *MockSchemaRegistry, schemsDesc *MockSchemaDescr) {
				registry.EXPECT().GetLatestSchema(testID).Return(schemsDesc, nil)
			},
			schemaSet: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockSchemaRegistry := NewMockSchemaRegistry(ctrl)
			mockSchemaDesc := NewMockSchemaDescr(ctrl)
			test.setup(mockSchemaRegistry, mockSchemaDesc)
			ctx := NewContextFor(testID, mockSchemaRegistry)
			if test.schemaSet {
				assert.NotNil(t, ctx.Schema)
				return
			}
			assert.Nil(t, ctx.Schema)
		})
	}

}
