package namespace

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/ident"
)

func TestUpdateSchemaRegistry(t *testing.T) {
	tests := []struct {
		name              string
		setupMocks        func(ctrl *gomock.Controller) (Map, SchemaRegistry)
		expectError       bool
		errorContainsText string
	}{
		{
			name: "schema not found, updates with latest schema",
			setupMocks: func(ctrl *gomock.Controller) (Map, SchemaRegistry) {
				id := ident.StringID("ns1")
				schemaReg := NewMockSchemaRegistry(ctrl)
				schema := NewMockSchemaDescr(ctrl)
				history := NewMockSchemaHistory(ctrl)
				opts := NewMockOptions(ctrl)
				md := NewMockMetadata(ctrl)
				mdMap := NewMockMap(ctrl)

				schemaReg.EXPECT().GetLatestSchema(id).
					Return(nil, &schemaHistoryNotFoundError{})
				md.EXPECT().ID().Return(id).AnyTimes()
				md.EXPECT().Options().Return(opts).AnyTimes()
				opts.EXPECT().SchemaHistory().Return(history).Times(2)
				history.EXPECT().GetLatest().Return(schema, true)
				schema.EXPECT().DeployId().Return("v1")
				schemaReg.EXPECT().SetSchemaHistory(id, history).Return(nil)
				mdMap.EXPECT().Metadatas().Return([]Metadata{md})
				return mdMap, schemaReg
			},
			expectError: false,
		},
		{
			name: "existing schema has empty deploy ID",
			setupMocks: func(ctrl *gomock.Controller) (Map, SchemaRegistry) {
				id := ident.StringID("ns2")
				schemaReg := NewMockSchemaRegistry(ctrl)
				schema := NewMockSchemaDescr(ctrl)
				opts := NewMockOptions(ctrl)
				md := NewMockMetadata(ctrl)
				mdMap := NewMockMap(ctrl)

				schemaReg.EXPECT().GetLatestSchema(id).Return(schema, nil)
				schema.EXPECT().DeployId().Return("")
				md.EXPECT().ID().Return(id).AnyTimes()
				md.EXPECT().Options().Return(opts).AnyTimes()
				mdMap.EXPECT().Metadatas().Return([]Metadata{md})
				return mdMap, schemaReg
			},
			expectError:       true,
			errorContainsText: "empty deploy ID",
		},
		{
			name: "schema history not found but schema exists",
			setupMocks: func(ctrl *gomock.Controller) (Map, SchemaRegistry) {
				id := ident.StringID("ns3")
				schemaReg := NewMockSchemaRegistry(ctrl)
				schema := NewMockSchemaDescr(ctrl)
				history := NewMockSchemaHistory(ctrl)
				opts := NewMockOptions(ctrl)
				md := NewMockMetadata(ctrl)
				mdMap := NewMockMap(ctrl)

				schemaReg.EXPECT().GetLatestSchema(id).Return(schema, nil)
				schema.EXPECT().DeployId().Return("v2")
				md.EXPECT().ID().Return(id).AnyTimes()
				md.EXPECT().Options().Return(opts).AnyTimes()
				opts.EXPECT().SchemaHistory().Return(history)
				history.EXPECT().GetLatest().Return(nil, false)
				mdMap.EXPECT().Metadatas().Return([]Metadata{md})
				return mdMap, schemaReg
			},
			expectError:       true,
			errorContainsText: "schema not found",
		},
		{
			name: "GetLatestSchema returns unexpected error",
			setupMocks: func(ctrl *gomock.Controller) (Map, SchemaRegistry) {
				id := ident.StringID("ns4")
				schemaReg := NewMockSchemaRegistry(ctrl)
				md := NewMockMetadata(ctrl)
				mdMap := NewMockMap(ctrl)

				schemaReg.EXPECT().GetLatestSchema(id).
					Return(nil, errors.New("unexpected error"))
				md.EXPECT().ID().Return(id).AnyTimes()
				mdMap.EXPECT().Metadatas().Return([]Metadata{md})
				return mdMap, schemaReg
			},
			expectError:       true,
			errorContainsText: "cannot get latest namespace schema",
		},
		{
			name: "SetSchemaHistory fails",
			setupMocks: func(ctrl *gomock.Controller) (Map, SchemaRegistry) {
				id := ident.StringID("ns5")
				schemaReg := NewMockSchemaRegistry(ctrl)
				schema := NewMockSchemaDescr(ctrl)
				history := NewMockSchemaHistory(ctrl)
				opts := NewMockOptions(ctrl)
				md := NewMockMetadata(ctrl)
				mdMap := NewMockMap(ctrl)

				schemaReg.EXPECT().GetLatestSchema(id).Return(nil, &schemaHistoryNotFoundError{})
				md.EXPECT().ID().Return(id).AnyTimes()
				md.EXPECT().Options().Return(opts).AnyTimes()
				opts.EXPECT().SchemaHistory().Return(history).Times(2)
				history.EXPECT().GetLatest().Return(schema, true)
				schema.EXPECT().DeployId().Return("v3")
				schemaReg.EXPECT().SetSchemaHistory(id, history).Return(errors.New("failed to set schema"))
				mdMap.EXPECT().Metadatas().Return([]Metadata{md})
				return mdMap, schemaReg
			},
			expectError:       true,
			errorContainsText: "failed to update to latest schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mdMap, schemaReg := tt.setupMocks(ctrl)
			err := UpdateSchemaRegistry(mdMap, schemaReg, zap.NewNop())

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContainsText != "" {
					assert.Contains(t, err.Error(), tt.errorContainsText)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
